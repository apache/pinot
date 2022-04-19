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
package org.apache.pinot.query.runtime.executor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.request.context.ThreadTimer;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.nodes.FilterNode;
import org.apache.pinot.query.planner.nodes.JoinNode;
import org.apache.pinot.query.planner.nodes.MailboxReceiveNode;
import org.apache.pinot.query.planner.nodes.MailboxSendNode;
import org.apache.pinot.query.planner.nodes.ProjectNode;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.query.runtime.blocks.DataTableBlockUtils;
import org.apache.pinot.query.runtime.operator.BroadcastJoinOperator;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * WorkerQueryExecutor is the v2 of the {@link org.apache.pinot.core.query.executor.QueryExecutor} API.
 *
 * It provides not only execution interface for {@link org.apache.pinot.core.query.request.ServerQueryRequest} but
 * also a more general {@link DistributedStagePlan}.
 */
public class WorkerQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerQueryExecutor.class);
  private PinotConfiguration _config;
  private ServerMetrics _serverMetrics;
  private MailboxService<Mailbox.MailboxContent> _mailboxService;
  private String _hostName;
  private int _port;

  public void init(PinotConfiguration config, ServerMetrics serverMetrics,
      MailboxService<Mailbox.MailboxContent> mailboxService, String hostName, int port) {
    _config = config;
    _serverMetrics = serverMetrics;
    _mailboxService = mailboxService;
    _hostName = hostName;
    _port = port;
  }

  public synchronized void start() {
    LOGGER.info("Worker query executor started");
  }

  public synchronized void shutDown() {
    LOGGER.info("Worker query executor shut down");
  }

  // TODO: split this execution from PhysicalPlanner
  public void processQuery(DistributedStagePlan queryRequest, Map<String, String> requestMetadataMap,
      ExecutorService executorService) {
    long requestId = Long.parseLong(requestMetadataMap.get("REQUEST_ID"));
    StageNode stageRoot = queryRequest.getStageRoot();
    BaseOperator<DataTableBlock> rootOperator = getOperator(requestId, stageRoot, queryRequest.getMetadataMap());
    executorService.submit(new TraceRunnable() {
      @Override
      public void runJob() {
        ThreadTimer executionThreadTimer = new ThreadTimer();
        while (!DataTableBlockUtils.isEndOfStream(rootOperator.nextBlock())) {
          LOGGER.debug("Result Block acquired");
        }
        LOGGER.info("Execution time:" + executionThreadTimer.getThreadTimeNs());
      }
    });
  }

  // TODO: split this PhysicalPlanner into a separate module
  private BaseOperator<DataTableBlock> getOperator(long requestId, StageNode stageNode,
      Map<Integer, StageMetadata> metadataMap) {
    // TODO: optimize this into a framework. (physical planner)
    if (stageNode instanceof MailboxReceiveNode) {
      MailboxReceiveNode receiveNode = (MailboxReceiveNode) stageNode;
      List<ServerInstance> sendingInstances = metadataMap.get(receiveNode.getSenderStageId()).getServerInstances();
      return new MailboxReceiveOperator(_mailboxService, RelDistribution.Type.ANY, sendingInstances, _hostName, _port,
          requestId, receiveNode.getSenderStageId());
    } else if (stageNode instanceof MailboxSendNode) {
      MailboxSendNode sendNode = (MailboxSendNode) stageNode;
      BaseOperator<DataTableBlock> nextOperator = getOperator(requestId, sendNode.getInputs().get(0), metadataMap);
      StageMetadata receivingStageMetadata = metadataMap.get(sendNode.getReceiverStageId());
      return new MailboxSendOperator(_mailboxService, nextOperator, receivingStageMetadata.getServerInstances(),
          sendNode.getExchangeType(), sendNode.getPartitionKeySelector(), _hostName, _port, requestId,
          sendNode.getStageId());
    } else if (stageNode instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) stageNode;
      BaseOperator<DataTableBlock> leftOperator = getOperator(requestId, joinNode.getInputs().get(0), metadataMap);
      BaseOperator<DataTableBlock> rightOperator = getOperator(requestId, joinNode.getInputs().get(1), metadataMap);
      return new BroadcastJoinOperator(leftOperator, rightOperator, joinNode.getCriteria());
    } else if (stageNode instanceof FilterNode) {
      throw new UnsupportedOperationException("Unsupported!");
    } else if (stageNode instanceof ProjectNode) {
      throw new UnsupportedOperationException("Unsupported!");
    } else {
      throw new UnsupportedOperationException(
          String.format("Stage node type %s is not supported!", stageNode.getClass().getSimpleName()));
    }
  }
}
