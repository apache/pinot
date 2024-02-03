/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.service.dispatch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.grpc.Deadline;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.calcite.util.Pair;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.reduce.ExecutionStatsAggregator;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.core.util.trace.TracedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.StageNodeSerDeUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.OpChainStats;
import org.apache.pinot.query.runtime.operator.OperatorStats;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code QueryDispatcher} dispatch a query to different workers.
 */
public class QueryDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryDispatcher.class);
  private static final String PINOT_BROKER_QUERY_DISPATCHER_FORMAT = "multistage-query-dispatch-%d";

  private final MailboxService _mailboxService;
  private final ExecutorService _executorService;
  private final Map<String, DispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();

  public QueryDispatcher(MailboxService mailboxService) {
    _mailboxService = mailboxService;
    _executorService = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
        new TracedThreadFactory(Thread.NORM_PRIORITY, false, PINOT_BROKER_QUERY_DISPATCHER_FORMAT));
  }

  public ResultTable submitAndReduce(RequestContext context, DispatchableSubPlan dispatchableSubPlan, long timeoutMs,
      Map<String, String> queryOptions, Map<Integer, ExecutionStatsAggregator> executionStatsAggregator)
      throws Exception {
    long requestId = context.getRequestId();
    try {
      submit(requestId, dispatchableSubPlan, timeoutMs, queryOptions);
      long reduceStartTimeNs = System.nanoTime();
      ResultTable resultTable =
          runReducer(requestId, dispatchableSubPlan, timeoutMs, queryOptions, executionStatsAggregator,
              _mailboxService);
      context.setReduceTimeNanos(System.nanoTime() - reduceStartTimeNs);
      return resultTable;
    } catch (Throwable e) {
      // TODO: Consider always cancel when it returns (early terminate)
      cancel(requestId, dispatchableSubPlan);
      throw e;
    }
  }

  @VisibleForTesting
  void submit(long requestId, DispatchableSubPlan dispatchableSubPlan, long timeoutMs, Map<String, String> queryOptions)
      throws Exception {
    Deadline deadline = Deadline.after(timeoutMs, TimeUnit.MILLISECONDS);
    List<DispatchablePlanFragment> stagePlans = dispatchableSubPlan.getQueryStageList();
    int numStages = stagePlans.size();
    Set<QueryServerInstance> serverInstances = new HashSet<>();
    // TODO: If serialization is slow, consider serializing each stage in parallel
    StageInfo[] stageInfoMap = new StageInfo[numStages];
    // Ignore the reduce stage (stage 0)
    for (int stageId = 1; stageId < numStages; stageId++) {
      DispatchablePlanFragment stagePlan = stagePlans.get(stageId);
      serverInstances.addAll(stagePlan.getServerInstanceToWorkerIdMap().keySet());
      Plan.StageNode rootNode =
          StageNodeSerDeUtils.serializeStageNode((AbstractPlanNode) stagePlan.getPlanFragment().getFragmentRoot());
      List<Worker.WorkerMetadata> workerMetadataList = QueryPlanSerDeUtils.toProtoWorkerMetadataList(stagePlan);
      stageInfoMap[stageId] = new StageInfo(rootNode, workerMetadataList, stagePlan.getCustomProperties());
    }
    Map<String, String> requestMetadata = new HashMap<>();
    requestMetadata.put(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, Long.toString(requestId));
    requestMetadata.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS, Long.toString(timeoutMs));
    requestMetadata.putAll(queryOptions);

    // Submit the query plan to all servers in parallel
    int numServers = serverInstances.size();
    BlockingQueue<AsyncQueryDispatchResponse> dispatchCallbacks = new ArrayBlockingQueue<>(numServers);
    for (QueryServerInstance serverInstance : serverInstances) {
      _executorService.submit(() -> {
        try {
          Worker.QueryRequest.Builder requestBuilder = Worker.QueryRequest.newBuilder();
          for (int stageId = 1; stageId < numStages; stageId++) {
            List<Integer> workerIds = stagePlans.get(stageId).getServerInstanceToWorkerIdMap().get(serverInstance);
            if (workerIds != null) {
              StageInfo stageInfo = stageInfoMap[stageId];
              Worker.StageMetadata stageMetadata =
                  QueryPlanSerDeUtils.toProtoStageMetadata(stageInfo._workerMetadataList, stageInfo._customProperties,
                      serverInstance, workerIds);
              Worker.StagePlan stagePlan =
                  Worker.StagePlan.newBuilder().setStageId(stageId).setStageRoot(stageInfo._rootNode)
                      .setStageMetadata(stageMetadata).build();
              requestBuilder.addStagePlan(stagePlan);
            }
          }
          requestBuilder.putAllMetadata(requestMetadata);
          getOrCreateDispatchClient(serverInstance).submit(requestBuilder.build(), serverInstance, deadline,
              dispatchCallbacks::offer);
        } catch (Throwable t) {
          LOGGER.warn("Caught exception while dispatching query: {} to server: {}", requestId, serverInstance, t);
          dispatchCallbacks.offer(new AsyncQueryDispatchResponse(serverInstance, null, t));
        }
      });
    }

    int numSuccessCalls = 0;
    // TODO: Cancel all dispatched requests if one of the dispatch errors out or deadline is breached.
    while (!deadline.isExpired() && numSuccessCalls < numServers) {
      AsyncQueryDispatchResponse resp =
          dispatchCallbacks.poll(deadline.timeRemaining(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
      if (resp != null) {
        if (resp.getThrowable() != null) {
          throw new RuntimeException(
              String.format("Error dispatching query: %d to server: %s", requestId, resp.getServerInstance()),
              resp.getThrowable());
        } else {
          Worker.QueryResponse response = resp.getQueryResponse();
          assert response != null;
          if (response.containsMetadata(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR)) {
            throw new RuntimeException(
                String.format("Unable to execute query plan for request: %d on server: %s, ERROR: %s", requestId,
                    resp.getServerInstance(),
                    response.getMetadataOrDefault(CommonConstants.Query.Response.ServerResponseStatus.STATUS_ERROR,
                        "null")));
          }
          numSuccessCalls++;
        }
      }
    }
    if (deadline.isExpired()) {
      throw new TimeoutException("Timed out waiting for response of async query-dispatch");
    }
  }

  private static class StageInfo {
    final Plan.StageNode _rootNode;
    final List<Worker.WorkerMetadata> _workerMetadataList;
    final Map<String, String> _customProperties;

    StageInfo(Plan.StageNode rootNode, List<Worker.WorkerMetadata> workerMetadataList,
        Map<String, String> customProperties) {
      _rootNode = rootNode;
      _workerMetadataList = workerMetadataList;
      _customProperties = customProperties;
    }
  }

  private void cancel(long requestId, DispatchableSubPlan dispatchableSubPlan) {
    List<DispatchablePlanFragment> stagePlans = dispatchableSubPlan.getQueryStageList();
    int numStages = stagePlans.size();
    // Skip the reduce stage (stage 0)
    Set<QueryServerInstance> serversToCancel = new HashSet<>();
    for (int stageId = 1; stageId < numStages; stageId++) {
      serversToCancel.addAll(stagePlans.get(stageId).getServerInstanceToWorkerIdMap().keySet());
    }
    for (QueryServerInstance queryServerInstance : serversToCancel) {
      try {
        getOrCreateDispatchClient(queryServerInstance).cancel(requestId);
      } catch (Throwable t) {
        LOGGER.warn("Caught exception while cancelling query: {} on server: {}", requestId, queryServerInstance, t);
      }
    }
  }

  private DispatchClient getOrCreateDispatchClient(QueryServerInstance queryServerInstance) {
    String hostname = queryServerInstance.getHostname();
    int port = queryServerInstance.getQueryServicePort();
    String key = String.format("%s_%d", hostname, port);
    return _dispatchClientMap.computeIfAbsent(key, k -> new DispatchClient(hostname, port));
  }

  @VisibleForTesting
  public static ResultTable runReducer(long requestId, DispatchableSubPlan dispatchableSubPlan, long timeoutMs,
      Map<String, String> queryOptions, @Nullable Map<Integer, ExecutionStatsAggregator> statsAggregatorMap,
      MailboxService mailboxService) {
    // NOTE: Reduce stage is always stage 0
    DispatchablePlanFragment dispatchablePlanFragment = dispatchableSubPlan.getQueryStageList().get(0);
    PlanFragment planFragment = dispatchablePlanFragment.getPlanFragment();
    PlanNode rootNode = planFragment.getFragmentRoot();
    Preconditions.checkState(rootNode instanceof MailboxReceiveNode,
        "Expecting mailbox receive node as root of reduce stage, got: %s", rootNode.getClass().getSimpleName());
    MailboxReceiveNode receiveNode = (MailboxReceiveNode) rootNode;
    List<WorkerMetadata> workerMetadataList = dispatchablePlanFragment.getWorkerMetadataList();
    Preconditions.checkState(workerMetadataList.size() == 1, "Expecting single worker for reduce stage, got: %s",
        workerMetadataList.size());
    StageMetadata stageMetadata = new StageMetadata.Builder().setWorkerMetadataList(workerMetadataList)
        .addCustomProperties(dispatchablePlanFragment.getCustomProperties()).build();
    OpChainExecutionContext opChainExecutionContext =
        new OpChainExecutionContext(mailboxService, requestId, planFragment.getFragmentId(),
            workerMetadataList.get(0).getVirtualServerAddress(), System.currentTimeMillis() + timeoutMs, queryOptions,
            stageMetadata, null);
    MailboxReceiveOperator receiveOperator =
        new MailboxReceiveOperator(opChainExecutionContext, receiveNode.getDistributionType(),
            receiveNode.getSenderStageId());
    ResultTable resultTable =
        getResultTable(receiveOperator, receiveNode.getDataSchema(), dispatchableSubPlan.getQueryResultFields());
    collectStats(dispatchableSubPlan, opChainExecutionContext.getStats(), statsAggregatorMap);
    return resultTable;
  }

  private static void collectStats(DispatchableSubPlan dispatchableSubPlan, OpChainStats opChainStats,
      @Nullable Map<Integer, ExecutionStatsAggregator> executionStatsAggregatorMap) {
    if (executionStatsAggregatorMap != null) {
      LOGGER.info("Extracting broker query execution stats, Runtime: {}ms", opChainStats.getExecutionTime());
      for (Map.Entry<String, OperatorStats> entry : opChainStats.getOperatorStatsMap().entrySet()) {
        OperatorStats operatorStats = entry.getValue();
        ExecutionStatsAggregator rootStatsAggregator = executionStatsAggregatorMap.get(0);
        ExecutionStatsAggregator stageStatsAggregator = executionStatsAggregatorMap.get(operatorStats.getStageId());
        rootStatsAggregator.aggregate(null, entry.getValue().getExecutionStats(), new HashMap<>());
        if (stageStatsAggregator != null) {
          if (dispatchableSubPlan != null) {
            OperatorUtils.recordTableName(operatorStats,
                dispatchableSubPlan.getQueryStageList().get(operatorStats.getStageId()));
          }
          stageStatsAggregator.aggregate(null, entry.getValue().getExecutionStats(), new HashMap<>());
        }
      }
    }
  }

  private static ResultTable getResultTable(MailboxReceiveOperator receiveOperator, DataSchema sourceDataSchema,
      List<Pair<Integer, String>> resultFields) {
    int numColumns = resultFields.size();
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnTypes = new ColumnDataType[numColumns];
    for (int i = 0; i < numColumns; i++) {
      Pair<Integer, String> field = resultFields.get(i);
      columnNames[i] = field.right;
      columnTypes[i] = sourceDataSchema.getColumnDataType(field.left);
    }
    DataSchema resultDataSchema = new DataSchema(columnNames, columnTypes);

    ArrayList<Object[]> resultRows = new ArrayList<>();
    TransferableBlock block = receiveOperator.nextBlock();
    while (!TransferableBlockUtils.isEndOfStream(block)) {
      DataBlock dataBlock = block.getDataBlock();
      int numRows = dataBlock.getNumberOfRows();
      if (numRows > 0) {
        resultRows.ensureCapacity(resultRows.size() + numRows);
        List<Object[]> rawRows = DataBlockExtractUtils.extractRows(dataBlock);
        for (Object[] rawRow : rawRows) {
          Object[] row = new Object[numColumns];
          for (int i = 0; i < numColumns; i++) {
            Object rawValue = rawRow[resultFields.get(i).left];
            if (rawValue != null) {
              ColumnDataType dataType = columnTypes[i];
              row[i] = dataType.format(dataType.toExternal(rawValue));
            }
          }
          resultRows.add(row);
        }
      }
      block = receiveOperator.nextBlock();
    }
    if (block.isErrorBlock()) {
      throw new RuntimeException("Received error query execution result block: " + block.getExceptions());
    }

    return new ResultTable(resultDataSchema, resultRows);
  }

  public void shutdown() {
    for (DispatchClient dispatchClient : _dispatchClientMap.values()) {
      dispatchClient.getChannel().shutdown();
    }
    _dispatchClientMap.clear();
  }
}
