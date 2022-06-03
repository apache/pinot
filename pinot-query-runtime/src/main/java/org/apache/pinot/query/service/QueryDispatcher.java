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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.PinotQueryWorkerGrpc;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.runtime.blocks.BaseDataBlock;
import org.apache.pinot.query.runtime.blocks.DataBlockUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.serde.QueryPlanSerDeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code QueryDispatcher} dispatch a query to different workers.
 */
public class QueryDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryDispatcher.class);

  private final Map<String, DispatchClient> _dispatchClientMap = new ConcurrentHashMap<>();

  public QueryDispatcher() {
  }

  public List<DataTable> submitAndReduce(long requestId, QueryPlan queryPlan,
      MailboxService<Mailbox.MailboxContent> mailboxService, long timeoutNano)
      throws Exception {
    // submit all the distributed stages.
    int reduceStageId = submit(requestId, queryPlan);
    // run reduce stage and return result.
    MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(reduceStageId);
    MailboxReceiveOperator mailboxReceiveOperator = createReduceStageOperator(mailboxService,
        queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
        requestId, reduceNode.getSenderStageId(), mailboxService.getHostname(),
        mailboxService.getMailboxPort());
    return reduceMailboxReceive(mailboxReceiveOperator);
  }

  public int submit(long requestId, QueryPlan queryPlan)
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
          int mailboxPort = serverInstance.getQueryMailboxPort();
          DispatchClient client = getOrCreateDispatchClient(host, servicePort);
          Worker.QueryResponse response = client.submit(Worker.QueryRequest.newBuilder()
              .setStagePlan(QueryPlanSerDeUtils.serialize(constructDistributedStagePlan(queryPlan, stageId,
                  serverInstance)))
              .putMetadata("REQUEST_ID", String.valueOf(requestId))
              .putMetadata("SERVER_INSTANCE_HOST", serverInstance.getHostname())
              .putMetadata("SERVER_INSTANCE_PORT", String.valueOf(mailboxPort)).build());
          if (response.containsMetadata("ERROR")) {
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

  public static List<DataTable> reduceMailboxReceive(MailboxReceiveOperator mailboxReceiveOperator) {
    List<DataTable> resultDataBlocks = new ArrayList<>();
    TransferableBlock transferableBlock;
    while (true) {
      transferableBlock = mailboxReceiveOperator.nextBlock();
      if (DataBlockUtils.isEndOfStream(transferableBlock)) {
        break;
      }
      if (transferableBlock.getDataBlock() != null) {
        BaseDataBlock dataTable = transferableBlock.getDataBlock();
        resultDataBlocks.add(dataTable);
      }
    }
    return resultDataBlocks;
  }

  public static MailboxReceiveOperator createReduceStageOperator(MailboxService<Mailbox.MailboxContent> mailboxService,
      List<ServerInstance> sendingInstances, long jobId, int stageId, String hostname, int port) {
    MailboxReceiveOperator mailboxReceiveOperator =
        new MailboxReceiveOperator(mailboxService, RelDistribution.Type.ANY, sendingInstances, hostname, port, jobId,
            stageId);
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
