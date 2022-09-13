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
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.request.context.ThreadTimer;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.FilterOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.SortOperator;
import org.apache.pinot.query.runtime.operator.TransformOperator;
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
    BaseOperator<TransferableBlock> rootOperator = getOperator(requestId, stageRoot, queryRequest.getMetadataMap());
    executorService.submit(new TraceRunnable() {
      @Override
      public void runJob() {
        ThreadTimer executionThreadTimer = new ThreadTimer();
        while (!TransferableBlockUtils.isEndOfStream(rootOperator.nextBlock())) {
          LOGGER.debug("Result Block acquired");
        }
        LOGGER.info("Execution time:" + executionThreadTimer.getThreadTimeNs());
      }
    });
  }

  // TODO: split this PhysicalPlanner into a separate module
  // TODO: optimize this into a framework. (physical planner)
  private BaseOperator<TransferableBlock> getOperator(long requestId, StageNode stageNode,
      Map<Integer, StageMetadata> metadataMap) {
    if (stageNode instanceof MailboxReceiveNode) {
      MailboxReceiveNode receiveNode = (MailboxReceiveNode) stageNode;
      List<ServerInstance> sendingInstances = metadataMap.get(receiveNode.getSenderStageId()).getServerInstances();
      return new MailboxReceiveOperator(_mailboxService, receiveNode.getDataSchema(), sendingInstances,
          receiveNode.getExchangeType(), receiveNode.getPartitionKeySelector(), _hostName, _port, requestId,
          receiveNode.getSenderStageId());
    } else if (stageNode instanceof MailboxSendNode) {
      MailboxSendNode sendNode = (MailboxSendNode) stageNode;
      BaseOperator<TransferableBlock> nextOperator = getOperator(requestId, sendNode.getInputs().get(0), metadataMap);
      StageMetadata receivingStageMetadata = metadataMap.get(sendNode.getReceiverStageId());
      return new MailboxSendOperator(_mailboxService, sendNode.getDataSchema(), nextOperator,
          receivingStageMetadata.getServerInstances(), sendNode.getExchangeType(), sendNode.getPartitionKeySelector(),
          _hostName, _port, requestId, sendNode.getStageId());
    } else if (stageNode instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) stageNode;
      BaseOperator<TransferableBlock> leftOperator = getOperator(requestId, joinNode.getInputs().get(0), metadataMap);
      BaseOperator<TransferableBlock> rightOperator = getOperator(requestId, joinNode.getInputs().get(1), metadataMap);
      return new HashJoinOperator(leftOperator, joinNode.getInputs().get(0).getDataSchema(), rightOperator,
          joinNode.getInputs().get(1).getDataSchema(), joinNode.getDataSchema(), joinNode.getCriteria(),
          joinNode.getJoinRelType());
    } else if (stageNode instanceof AggregateNode) {
      AggregateNode aggregateNode = (AggregateNode) stageNode;
      BaseOperator<TransferableBlock> inputOperator =
          getOperator(requestId, aggregateNode.getInputs().get(0), metadataMap);
      return new AggregateOperator(inputOperator, aggregateNode.getDataSchema(), aggregateNode.getAggCalls(),
          aggregateNode.getGroupSet(), aggregateNode.getInputs().get(0).getDataSchema());
    } else if (stageNode instanceof FilterNode) {
      FilterNode filterNode = (FilterNode) stageNode;
      return new FilterOperator(getOperator(requestId, filterNode.getInputs().get(0), metadataMap),
          filterNode.getDataSchema(), filterNode.getCondition());
    } else if (stageNode instanceof ProjectNode) {
      ProjectNode projectNode = (ProjectNode) stageNode;
      return new TransformOperator(getOperator(requestId, projectNode.getInputs().get(0), metadataMap),
          projectNode.getDataSchema(), projectNode.getProjects(), projectNode.getInputs().get(0).getDataSchema());
    } else if (stageNode instanceof SortNode) {
      SortNode sortNode = (SortNode) stageNode;
      return new SortOperator(getOperator(requestId, sortNode.getInputs().get(0), metadataMap),
          sortNode.getCollationKeys(), sortNode.getCollationDirections(), sortNode.getFetch(), sortNode.getOffset(),
          sortNode.getDataSchema());
    } else {
      throw new UnsupportedOperationException(
          String.format("Stage node type %s is not supported!", stageNode.getClass().getSimpleName()));
    }
  }
}
