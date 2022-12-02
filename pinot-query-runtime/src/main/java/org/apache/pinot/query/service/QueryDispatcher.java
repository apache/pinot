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
package org.apache.pinot.query.service;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.Pair;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * {@code QueryDispatcher} dispatch a query to different workers.
 */
public class QueryDispatcher {
  private final Map<String, DispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();

  public QueryDispatcher() {
  }

  public ResultTable submitAndReduce(long requestId, QueryPlan queryPlan,
      MailboxService<TransferableBlock> mailboxService, long timeoutMs)
      throws Exception {
    // submit all the distributed stages.
    int reduceStageId = submit(requestId, queryPlan, timeoutMs);
    // run reduce stage and return result.
    MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(reduceStageId);
    MailboxReceiveOperator mailboxReceiveOperator = createReduceStageOperator(mailboxService,
        queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(), requestId,
        reduceNode.getSenderStageId(), reduceNode.getDataSchema(), mailboxService.getHostname(),
        mailboxService.getMailboxPort(), timeoutMs);
    List<DataBlock> resultDataBlocks = reduceMailboxReceive(mailboxReceiveOperator, timeoutMs);
    return toResultTable(resultDataBlocks, queryPlan.getQueryResultFields(),
        queryPlan.getQueryStageMap().get(0).getDataSchema());
  }

  public int submit(long requestId, QueryPlan queryPlan, long timeoutMs)
      throws Exception {
    int reduceStageId = -1;
    for (Map.Entry<Integer, StageMetadata> stage : queryPlan.getStageMetadataMap().entrySet()) {
      int stageId = stage.getKey();
      // stage rooting at a mailbox receive node means reduce stage.
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        reduceStageId = stageId;
      } else {
        List<ServerInstance> serverInstances = stage.getValue().getServerInstances();
        for (ServerInstance serverInstance : serverInstances) {
          String host = serverInstance.getHostname();
          int servicePort = serverInstance.getQueryServicePort();
          DispatchClient client = getOrCreateDispatchClient(host, servicePort);
          Worker.QueryResponse response = client.submit(Worker.QueryRequest.newBuilder().setStagePlan(
                  QueryPlanSerDeUtils.serialize(constructDistributedStagePlan(queryPlan, stageId, serverInstance)))
              .putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_ID, String.valueOf(requestId))
              .putMetadata(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS, String.valueOf(timeoutMs)).build());
          if (response.containsMetadata(QueryConfig.KEY_OF_SERVER_RESPONSE_STATUS_ERROR)) {
            throw new RuntimeException(
                String.format("Unable to execute query plan at stage %s on server %s: ERROR: %s", stageId,
                    serverInstance, response));
          }
        }
      }
    }
    return reduceStageId;
  }

  private DispatchClient getOrCreateDispatchClient(String host, int port) {
    String key = String.format("%s_%d", host, port);
    return _dispatchClientMap.computeIfAbsent(key, k -> new DispatchClient(host, port));
  }

  public static DistributedStagePlan constructDistributedStagePlan(QueryPlan queryPlan, int stageId,
      ServerInstance serverInstance) {
    return new DistributedStagePlan(stageId, serverInstance, queryPlan.getQueryStageMap().get(stageId),
        queryPlan.getStageMetadataMap());
  }

  public static List<DataBlock> reduceMailboxReceive(MailboxReceiveOperator mailboxReceiveOperator, long timeoutMs) {
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
        List<Object[]> rawRows = DataBlockUtils.extractRows(dataBlock);
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
      List<ServerInstance> sendingInstances, long jobId, int stageId, DataSchema dataSchema, String hostname,
      int port, long timeoutMs) {
    // timeout is set for reduce stage
    MailboxReceiveOperator mailboxReceiveOperator =
        new MailboxReceiveOperator(mailboxService, sendingInstances,
            RelDistribution.Type.RANDOM_DISTRIBUTED, hostname, port, jobId, stageId, timeoutMs);
    return mailboxReceiveOperator;
  }

  public void shutdown() {
    for (DispatchClient dispatchClient : _dispatchClientMap.values()) {
      dispatchClient._managedChannel.shutdown();
    }
    _dispatchClientMap.clear();
  }

  public static class DispatchClient {
    private final PinotQueryWorkerGrpc.PinotQueryWorkerBlockingStub _blockingStub;
    private final ManagedChannel _managedChannel;

    public DispatchClient(String host, int port) {
      ManagedChannelBuilder managedChannelBuilder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
      _managedChannel = managedChannelBuilder.build();
      _blockingStub = PinotQueryWorkerGrpc.newBlockingStub(_managedChannel);
    }

    public Worker.QueryResponse submit(Worker.QueryRequest request) {
      return _blockingStub.submit(request);
    }
  }
}
