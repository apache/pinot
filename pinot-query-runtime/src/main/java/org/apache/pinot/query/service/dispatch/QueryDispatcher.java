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
import io.grpc.Deadline;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.calcite.util.Pair;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.reduce.ExecutionStatsAggregator;
import org.apache.pinot.core.util.trace.TracedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.DispatchablePlanFragment;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.planner.PhysicalExplainPlanVisitor;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.OpChainStats;
import org.apache.pinot.query.runtime.operator.OperatorStats;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerExecutor;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code QueryDispatcher} dispatch a query to different workers.
 */
public class QueryDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryDispatcher.class);
  private static final long DEFAULT_DISPATCHER_CALLBACK_POLL_TIMEOUT_MS = 100;
  private static final String PINOT_BROKER_QUERY_DISPATCHER_FORMAT = "multistage-query-dispatch-%d";

  private final Map<String, DispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();
  private final ExecutorService _executorService;

  public QueryDispatcher() {
    _executorService = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
        new TracedThreadFactory(Thread.NORM_PRIORITY, false, PINOT_BROKER_QUERY_DISPATCHER_FORMAT));
  }

  public ResultTable submitAndReduce(RequestContext context, DispatchableSubPlan dispatchableSubPlan,
      MailboxService mailboxService, OpChainSchedulerService scheduler, long timeoutMs,
      Map<String, String> queryOptions, Map<Integer, ExecutionStatsAggregator> executionStatsAggregator,
      boolean traceEnabled)
      throws Exception {
    final long requestId = context.getRequestId();
    try {
      // submit all the distributed stages.
      int reduceStageId = submit(requestId, dispatchableSubPlan, timeoutMs, queryOptions);
      // run reduce stage and return result.
      long reduceStartTimeInNanos = System.nanoTime();
      ResultTable resultTable = runReducer(requestId, dispatchableSubPlan, reduceStageId, timeoutMs, mailboxService,
          scheduler, executionStatsAggregator, traceEnabled);
      context.setReduceTimeNanos(System.nanoTime() - reduceStartTimeInNanos);
      return resultTable;
    } catch (Exception e) {
      cancel(requestId, dispatchableSubPlan);
      throw new RuntimeException("Error executing query: "
          + PhysicalExplainPlanVisitor.explain(dispatchableSubPlan), e);
    }
  }

  private void cancel(long requestId, DispatchableSubPlan dispatchableSubPlan) {
    Set<DispatchClient> dispatchClientSet = new HashSet<>();

    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      // stage rooting at a mailbox receive node means reduce stage.
      if (!(dispatchableSubPlan.getQueryStageList().get(stageId).getPlanFragment()
          .getFragmentRoot() instanceof MailboxReceiveNode)) {
        Set<QueryServerInstance> serverInstances =
            dispatchableSubPlan.getQueryStageList().get(stageId).getServerInstanceToWorkerIdMap().keySet();
        for (QueryServerInstance serverInstance : serverInstances) {
          String host = serverInstance.getHostname();
          int servicePort = serverInstance.getQueryServicePort();
          dispatchClientSet.add(getOrCreateDispatchClient(host, servicePort));
        }
      }
    }
    for (DispatchClient dispatchClient : dispatchClientSet) {
      dispatchClient.cancel(requestId);
    }
  }

  @VisibleForTesting
  int submit(long requestId, DispatchableSubPlan dispatchableSubPlan, long timeoutMs,
      Map<String, String> queryOptions)
      throws Exception {
    int reduceStageId = -1;
    Deadline deadline = Deadline.after(timeoutMs, TimeUnit.MILLISECONDS);
    BlockingQueue<AsyncQueryDispatchResponse> dispatchCallbacks = new LinkedBlockingQueue<>();
    int dispatchCalls = 0;

    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      // stage rooting at a mailbox receive node means reduce stage.
      if (dispatchableSubPlan.getQueryStageList().get(stageId).getPlanFragment()
          .getFragmentRoot() instanceof MailboxReceiveNode) {
        reduceStageId = stageId;
      } else {
        for (Map.Entry<QueryServerInstance, List<Integer>> queryServerEntry
            : dispatchableSubPlan.getQueryStageList().get(stageId).getServerInstanceToWorkerIdMap().entrySet()) {
          QueryServerInstance queryServerInstance = queryServerEntry.getKey();
          Worker.QueryRequest.Builder queryRequestBuilder = Worker.QueryRequest.newBuilder();
          String host = queryServerInstance.getHostname();
          int servicePort = queryServerInstance.getQueryServicePort();
          queryRequestBuilder.addStagePlan(
              QueryPlanSerDeUtils.serialize(dispatchableSubPlan, stageId, queryServerInstance,
                  queryServerEntry.getValue()));
          dispatchCalls++;
          Worker.QueryRequest queryRequest =
              queryRequestBuilder.putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_ID, String.valueOf(requestId))
                  .putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS, String.valueOf(timeoutMs))
                  .putAllMetadata(queryOptions).build();
          DispatchClient client = getOrCreateDispatchClient(host, servicePort);
          int finalStageId = stageId;
          _executorService.submit(() -> client.submit(queryRequest, finalStageId, queryServerInstance, deadline,
              dispatchCallbacks::offer));
        }
      }
    }
    int successfulDispatchCalls = 0;
    // TODO: Cancel all dispatched requests if one of the dispatch errors out or deadline is breached.
    while (!deadline.isExpired() && successfulDispatchCalls < dispatchCalls) {
      AsyncQueryDispatchResponse resp =
          dispatchCallbacks.poll(DEFAULT_DISPATCHER_CALLBACK_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (resp != null) {
        if (resp.getThrowable() != null) {
          throw new RuntimeException(
              String.format("Error dispatching query to server=%s stage=%s", resp.getVirtualServer(),
                  resp.getStageId()), resp.getThrowable());
        } else {
          Worker.QueryResponse response = resp.getQueryResponse();
          if (response.containsMetadata(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_ERROR)) {
            throw new RuntimeException(
                String.format("Unable to execute query plan at stage %s on server %s: ERROR: %s", resp.getStageId(),
                    resp.getVirtualServer(),
                    response.getMetadataOrDefault(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_ERROR, "null")));
          }
          successfulDispatchCalls++;
        }
      }
    }
    if (deadline.isExpired()) {
      throw new RuntimeException("Timed out waiting for response of async query-dispatch");
    }
    return reduceStageId;
  }

  @VisibleForTesting
  public static ResultTable runReducer(long requestId, DispatchableSubPlan dispatchableSubPlan, int reduceStageId,
      long timeoutMs, MailboxService mailboxService, OpChainSchedulerService scheduler,
      Map<Integer, ExecutionStatsAggregator> statsAggregatorMap, boolean traceEnabled) {
    DispatchablePlanFragment reduceStagePlanFragment = dispatchableSubPlan.getQueryStageList().get(reduceStageId);
    MailboxReceiveNode reduceNode = (MailboxReceiveNode) reduceStagePlanFragment.getPlanFragment().getFragmentRoot();
    reduceNode.setExchangeType(PinotRelExchangeType.PIPELINE_BREAKER);
    VirtualServerAddress server = new VirtualServerAddress(mailboxService.getHostname(), mailboxService.getPort(), 0);
    StageMetadata brokerStageMetadata = new StageMetadata.Builder()
        .setWorkerMetadataList(reduceStagePlanFragment.getWorkerMetadataList())
        .addCustomProperties(reduceStagePlanFragment.getCustomProperties())
        .build();
    DistributedStagePlan reducerStagePlan = new DistributedStagePlan(0, server, reduceNode, brokerStageMetadata);
    PipelineBreakerResult pipelineBreakerResult =
        PipelineBreakerExecutor.executePipelineBreakers(scheduler, mailboxService, reducerStagePlan,
            System.currentTimeMillis() + timeoutMs, requestId, traceEnabled);
    if (pipelineBreakerResult == null) {
      throw new RuntimeException("Broker reducer error during query execution!");
    }
    collectStats(dispatchableSubPlan, pipelineBreakerResult.getOpChainStats(), statsAggregatorMap);
    List<TransferableBlock> resultDataBlocks = pipelineBreakerResult.getResultMap().get(0);
    return toResultTable(resultDataBlocks, dispatchableSubPlan.getQueryResultFields(),
        dispatchableSubPlan.getQueryStageList().get(0).getPlanFragment().getFragmentRoot().getDataSchema());
  }

  private static void collectStats(DispatchableSubPlan dispatchableSubPlan, @Nullable OpChainStats opChainStats,
      @Nullable Map<Integer, ExecutionStatsAggregator> executionStatsAggregatorMap) {
    if (executionStatsAggregatorMap != null && opChainStats != null) {
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

  private static ResultTable toResultTable(List<TransferableBlock> queryResult, List<Pair<Integer, String>> fields,
      DataSchema sourceSchema) {
    List<Object[]> resultRows = new ArrayList<>();
    DataSchema resultSchema = toResultSchema(sourceSchema, fields);
    for (TransferableBlock transferableBlock : queryResult) {
      if (transferableBlock.isErrorBlock()) {
        throw new RuntimeException(
            "Received error query execution result block: " + transferableBlock.getDataBlock().getExceptions());
      }
      DataBlock dataBlock = transferableBlock.getDataBlock();
      int numColumns = resultSchema.getColumnNames().length;
      int numRows = dataBlock.getNumberOfRows();
      List<Object[]> rows = new ArrayList<>(dataBlock.getNumberOfRows());
      if (numRows > 0) {
        RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
        for (int colId = 0; colId < numColumns; colId++) {
          nullBitmaps[colId] = dataBlock.getNullRowIds(colId);
        }
        List<Object[]> rawRows = DataBlockUtils.extractRows(dataBlock, ObjectSerDeUtils::deserialize);
        int rowId = 0;
        for (Object[] rawRow : rawRows) {
          Object[] row = new Object[numColumns];
          // Only the masked fields should be selected out.
          int colId = 0;
          for (Pair<Integer, String> field : fields) {
            if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
              row[colId++] = null;
            } else {
              int colRef = field.left;
              if (rawRow[colRef] instanceof ByteArray) {
                row[colId++] = ((ByteArray) rawRow[colRef]).toHexString();
              } else if (resultSchema.getColumnDataType(colId) == DataSchema.ColumnDataType.TIMESTAMP) {
                row[colId++] = PinotDataType.TIMESTAMP.toTimestamp(rawRow[colRef]).toString();
              } else {
                row[colId++] = rawRow[colRef];
              }
            }
          }
          rows.add(row);
          rowId++;
        }
      }
      resultRows.addAll(rows);
    }
    return new ResultTable(resultSchema, resultRows);
  }

  private static DataSchema toResultSchema(DataSchema inputSchema, List<Pair<Integer, String>> fields) {
    String[] colNames = new String[fields.size()];
    DataSchema.ColumnDataType[] colTypes = new DataSchema.ColumnDataType[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      colNames[i] = fields.get(i).right;
      colTypes[i] = inputSchema.getColumnDataType(fields.get(i).left);
    }
    return new DataSchema(colNames, colTypes);
  }

  private static MailboxReceiveOperator createReduceStageOperator(OpChainExecutionContext context, int senderStageId) {
    return new MailboxReceiveOperator(context, RelDistribution.Type.RANDOM_DISTRIBUTED, senderStageId);
  }

  public void shutdown() {
    for (DispatchClient dispatchClient : _dispatchClientMap.values()) {
      dispatchClient.getChannel().shutdown();
    }
    _dispatchClientMap.clear();
  }

  private DispatchClient getOrCreateDispatchClient(String host, int port) {
    String key = String.format("%s_%d", host, port);
    return _dispatchClientMap.computeIfAbsent(key, k -> new DispatchClient(host, port));
  }
}
