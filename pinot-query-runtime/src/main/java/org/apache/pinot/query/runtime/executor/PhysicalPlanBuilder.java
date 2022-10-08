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
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.transport.ServerInstance;
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
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.FilterOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.LiteralValueOperator;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.SortOperator;
import org.apache.pinot.query.runtime.operator.TransformOperator;


public class PhysicalPlanBuilder implements StageNodeVisitor<Operator<TransferableBlock>, Void> {

  private final MailboxService<Mailbox.MailboxContent> _mailboxService;
  private final String _hostName;
  private final int _port;
  private final long _requestId;
  private final Map<Integer, StageMetadata> _metadataMap;

  public PhysicalPlanBuilder(MailboxService<Mailbox.MailboxContent> mailboxService, String hostName, int port,
      long requestId, Map<Integer, StageMetadata> metadataMap) {
    _mailboxService = mailboxService;
    _hostName = hostName;
    _port = port;
    _requestId = requestId;
    _metadataMap = metadataMap;
  }

  public Operator<TransferableBlock> build(StageNode node) {
    return node.visit(this, null);
  }

  @Override
  public Operator<TransferableBlock> visitMailboxReceive(MailboxReceiveNode node, Void context) {
    List<ServerInstance> sendingInstances = _metadataMap.get(node.getSenderStageId()).getServerInstances();
    return new MailboxReceiveOperator(_mailboxService, node.getDataSchema(), sendingInstances,
        node.getExchangeType(), node.getPartitionKeySelector(), _hostName, _port, _requestId,
        node.getSenderStageId());
  }

  @Override
  public Operator<TransferableBlock> visitMailboxSend(MailboxSendNode node, Void context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, null);
    StageMetadata receivingStageMetadata = _metadataMap.get(node.getReceiverStageId());
    return new MailboxSendOperator(_mailboxService, node.getDataSchema(), nextOperator,
        receivingStageMetadata.getServerInstances(), node.getExchangeType(), node.getPartitionKeySelector(),
        _hostName, _port, _requestId, node.getStageId());
  }

  @Override
  public Operator<TransferableBlock> visitAggregate(AggregateNode node, Void context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, null);
    return new AggregateOperator(nextOperator, node.getDataSchema(), node.getAggCalls(),
        node.getGroupSet(), node.getInputs().get(0).getDataSchema());
  }

  @Override
  public Operator<TransferableBlock> visitFilter(FilterNode node, Void context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, null);
    return new FilterOperator(nextOperator, node.getDataSchema(), node.getCondition());
  }

  @Override
  public Operator<TransferableBlock> visitJoin(JoinNode node, Void context) {
    StageNode left = node.getInputs().get(0);
    StageNode right = node.getInputs().get(1);

    Operator<TransferableBlock> leftOperator = left.visit(this, null);
    Operator<TransferableBlock> rightOperator = right.visit(this, null);

    return new HashJoinOperator(leftOperator, left.getDataSchema(), rightOperator,
        right.getDataSchema(), node.getDataSchema(), node.getJoinKeys(),
        node.getJoinClauses(), node.getJoinRelType());
  }

  @Override
  public Operator<TransferableBlock> visitProject(ProjectNode node, Void context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, null);
    return new TransformOperator(nextOperator, node.getDataSchema(), node.getProjects(),
        node.getInputs().get(0).getDataSchema());
  }

  @Override
  public Operator<TransferableBlock> visitSort(SortNode node, Void context) {
    Operator<TransferableBlock> nextOperator = node.getInputs().get(0).visit(this, null);
    return new SortOperator(nextOperator, node.getCollationKeys(), node.getCollationDirections(),
        node.getFetch(), node.getOffset(), node.getDataSchema());
  }

  @Override
  public Operator<TransferableBlock> visitTableScan(TableScanNode node, Void context) {
    throw new UnsupportedOperationException("Stage node of type TableScanNode is not supported!");
  }

  @Override
  public Operator<TransferableBlock> visitValue(ValueNode node, Void context) {
    return new LiteralValueOperator(node.getDataSchema(), node.getLiteralRows());
  }
}
