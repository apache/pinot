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
import org.apache.calcite.util.Pair;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.reduce.ExecutionStatsAggregator;
import org.apache.pinot.core.util.trace.TracedThreadFactory;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.ExplainPlanStageVisitor;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.OperatorStats;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.apache.pinot.query.service.QueryConfig;
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

  public ResultTable submitAndReduce(long requestId, QueryPlan queryPlan,
      MailboxService<TransferableBlock> mailboxService, long timeoutMs, Map<String, String> queryOptions,
      Map<Integer, ExecutionStatsAggregator> executionStatsAggregator)
      throws Exception {
    try {
      // submit all the distributed stages.
      int reduceStageId = submit(requestId, queryPlan, timeoutMs, queryOptions);
      // run reduce stage and return result.
      return runReducer(requestId, queryPlan, reduceStageId, timeoutMs, mailboxService, executionStatsAggregator);
    } catch (Exception e) {
      cancel(requestId, queryPlan);
      throw new RuntimeException("Error executing query: " + ExplainPlanStageVisitor.explain(queryPlan), e);
    }
  }

  private void cancel(long requestId, QueryPlan queryPlan) {
    Set<DispatchClient> dispatchClientSet = new HashSet<>();
    for (Map.Entry<Integer, StageMetadata> stage : queryPlan.getStageMetadataMap().entrySet()) {
      int stageId = stage.getKey();
      // stage rooting at a mailbox receive node means reduce stage.
      if (!(queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode)) {
        List<VirtualServer> serverInstances = stage.getValue().getServerInstances();
        for (VirtualServer serverInstance : serverInstances) {
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

  private ResultTable runReducer(long requestId, QueryPlan queryPlan, int reduceStageId, long timeoutMs,
      MailboxService<TransferableBlock> mailboxService, Map<Integer, ExecutionStatsAggregator> statsAggregatorMap) {
    MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(reduceStageId);
    MailboxReceiveOperator mailboxReceiveOperator = createReduceStageOperator(mailboxService,
        queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(), requestId,
        reduceNode.getSenderStageId(), reduceStageId, reduceNode.getDataSchema(),
        new VirtualServerAddress(mailboxService.getHostname(), mailboxService.getMailboxPort(), 0), timeoutMs);
    List<DataBlock> resultDataBlocks =
        reduceMailboxReceive(mailboxReceiveOperator, timeoutMs, statsAggregatorMap, queryPlan);
    return toResultTable(resultDataBlocks, queryPlan.getQueryResultFields(),
        queryPlan.getQueryStageMap().get(0).getDataSchema());
  }

  public int submit(long requestId, QueryPlan queryPlan, long timeoutMs, Map<String, String> queryOptions)
      throws Exception {
    int reduceStageId = -1;
    Deadline deadline = Deadline.after(timeoutMs, TimeUnit.MILLISECONDS);
    BlockingQueue<AsyncQueryDispatchResponse> dispatchCallbacks = new LinkedBlockingQueue<>();
    int dispatchCalls = 0;
    for (Map.Entry<Integer, StageMetadata> stage : queryPlan.getStageMetadataMap().entrySet()) {
      int stageId = stage.getKey();
      // stage rooting at a mailbox receive node means reduce stage.
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        reduceStageId = stageId;
      } else {
        List<VirtualServer> serverInstances = stage.getValue().getServerInstances();
        for (VirtualServer serverInstance : serverInstances) {
          String host = serverInstance.getHostname();
          int servicePort = serverInstance.getQueryServicePort();
          DispatchClient client = getOrCreateDispatchClient(host, servicePort);
          dispatchCalls++;
          _executorService.submit(() -> {
            client.submit(Worker.QueryRequest.newBuilder().setStagePlan(
                QueryPlanSerDeUtils.serialize(constructDistributedStagePlan(queryPlan, stageId, serverInstance)))
                .putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_ID, String.valueOf(requestId))
                .putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS, String.valueOf(timeoutMs))
                .putAllMetadata(queryOptions).build(), stageId, serverInstance, deadline, dispatchCallbacks::offer);
          });
        }
      }
    }
    int successfulDispatchCalls = 0;
    // TODO: Cancel all dispatched requests if one of the dispatch errors out or deadline is breached.
    while (!deadline.isExpired() && successfulDispatchCalls < dispatchCalls) {
      AsyncQueryDispatchResponse resp = dispatchCallbacks.poll(
          DEFAULT_DISPATCHER_CALLBACK_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (resp != null) {
        if (resp.getThrowable() != null) {
          throw new RuntimeException(String.format("Error dispatching query to server=%s stage=%s",
              resp.getVirtualServer(), resp.getStageId()), resp.getThrowable());
        } else {
          Worker.QueryResponse response = resp.getQueryResponse();
          if (response.containsMetadata(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_ERROR)) {
            throw new RuntimeException(
                String.format("Unable to execute query plan at stage %s on server %s: ERROR: %s", resp.getStageId(),
                    resp.getVirtualServer(), response));
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

  public static DistributedStagePlan constructDistributedStagePlan(QueryPlan queryPlan, int stageId,
      VirtualServer serverInstance) {
    return new DistributedStagePlan(stageId, serverInstance, queryPlan.getQueryStageMap().get(stageId),
        queryPlan.getStageMetadataMap());
  }

  public static List<DataBlock> reduceMailboxReceive(MailboxReceiveOperator mailboxReceiveOperator, long timeoutMs) {
    return reduceMailboxReceive(mailboxReceiveOperator, timeoutMs, null, null);
  }

  public static List<DataBlock> reduceMailboxReceive(MailboxReceiveOperator mailboxReceiveOperator, long timeoutMs,
      @Nullable Map<Integer, ExecutionStatsAggregator> executionStatsAggregatorMap, QueryPlan queryPlan) {
    List<DataBlock> resultDataBlocks = new ArrayList<>();
    TransferableBlock transferableBlock;
    long timeoutWatermark = System.nanoTime() + timeoutMs * 1_000_000L;
    while (System.nanoTime() < timeoutWatermark) {
      transferableBlock = mailboxReceiveOperator.nextBlock();
      if (TransferableBlockUtils.isEndOfStream(transferableBlock) && transferableBlock.isErrorBlock()) {
        // TODO: we only received bubble up error from the execution stage tree.
        // TODO: query dispatch should also send cancel signal to the rest of the execution stage tree.
        throw new RuntimeException(
            "Received error query execution result block: " + transferableBlock.getDataBlock().getExceptions());
      }
      if (transferableBlock.isNoOpBlock()) {
        continue;
      } else if (transferableBlock.isEndOfStreamBlock()) {
        if (executionStatsAggregatorMap != null) {
          for (Map.Entry<String, OperatorStats> entry : transferableBlock.getResultMetadata().entrySet()) {
            LOGGER.info("Broker Query Execution Stats - OperatorId: {}, OperatorStats: {}", entry.getKey(),
                OperatorUtils.operatorStatsToJson(entry.getValue()));
            OperatorStats operatorStats = entry.getValue();
            ExecutionStatsAggregator rootStatsAggregator = executionStatsAggregatorMap.get(0);
            ExecutionStatsAggregator stageStatsAggregator = executionStatsAggregatorMap.get(operatorStats.getStageId());
            if (queryPlan != null) {
              StageMetadata operatorStageMetadata = queryPlan.getStageMetadataMap().get(operatorStats.getStageId());
              OperatorUtils.recordTableName(operatorStats, operatorStageMetadata);
            }
            rootStatsAggregator.aggregate(null, entry.getValue().getExecutionStats(), new HashMap<>());
            stageStatsAggregator.aggregate(null, entry.getValue().getExecutionStats(), new HashMap<>());
          }
        }
        return resultDataBlocks;
      }
      resultDataBlocks.add(transferableBlock.getDataBlock());
    }
    throw new RuntimeException("Timed out while receiving from mailbox: " + QueryException.EXECUTION_TIMEOUT_ERROR);
  }

  public static ResultTable toResultTable(List<DataBlock> queryResult, List<Pair<Integer, String>> fields,
      DataSchema sourceSchema) {
    List<Object[]> resultRows = new ArrayList<>();
    DataSchema resultSchema = toResultSchema(sourceSchema, fields);
    for (DataBlock dataBlock : queryResult) {
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
              row[colId++] = rawRow[colRef];
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

  @VisibleForTesting
  public static MailboxReceiveOperator createReduceStageOperator(MailboxService<TransferableBlock> mailboxService,
      List<VirtualServer> sendingInstances, long jobId, int stageId, int reducerStageId, DataSchema dataSchema,
      VirtualServerAddress server, long timeoutMs) {
    // timeout is set for reduce stage
    MailboxReceiveOperator mailboxReceiveOperator =
        new MailboxReceiveOperator(mailboxService, sendingInstances,
            RelDistribution.Type.RANDOM_DISTRIBUTED, server, jobId, stageId, reducerStageId, timeoutMs);
    return mailboxReceiveOperator;
  }

  public void shutdown() {
    for (DispatchClient dispatchClient : _dispatchClientMap.values()) {
      dispatchClient.getChannel().shutdown();
    }
    _dispatchClientMap.clear();
  }

  @VisibleForTesting
  DispatchClient getOrCreateDispatchClient(String host, int port) {
    String key = String.format("%s_%d", host, port);
    return _dispatchClientMap.computeIfAbsent(key, k -> new DispatchClient(host, port));
  }
}
