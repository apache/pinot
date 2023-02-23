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
package org.apache.pinot.query.runtime.plan;

import java.util.List;
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
import org.apache.pinot.query.planner.stage.WindowNode;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.FilterOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.LiteralValueOperator;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.SortOperator;
import org.apache.pinot.query.runtime.operator.TransformOperator;
import org.apache.pinot.query.runtime.operator.WindowAggregateOperator;


/**
 * This visitor constructs a physical plan of operators from a {@link StageNode} tree. Note that
 * this works only for the intermediate stage nodes, leaf stage nodes are expected to compile into
 * v1 operators at this point in time.
 *
 * <p>This class should be used statically via {@link #build(StageNode, PlanRequestContext)}
 */
public class PhysicalPlanVisitor implements StageNodeVisitor<MultiStageOperator, PlanRequestContext> {

  private static final PhysicalPlanVisitor INSTANCE = new PhysicalPlanVisitor();

  public static OpChain build(StageNode node, PlanRequestContext context) {
    MultiStageOperator root = node.visit(INSTANCE, context);
    return new OpChain(root, context.getReceivingMailboxes(), context._server.virtualId(), context.getRequestId(),
        context.getStageId());
  }

  @Override
  public MultiStageOperator visitMailboxReceive(MailboxReceiveNode node, PlanRequestContext context) {
    List<VirtualServer> sendingInstances = context.getMetadataMap().get(node.getSenderStageId()).getServerInstances();
    MailboxReceiveOperator mailboxReceiveOperator =
        new MailboxReceiveOperator(context.getMailboxService(), sendingInstances,
            node.getExchangeType(), context.getServer(),
            context.getRequestId(), node.getSenderStageId(), node.getStageId(), context.getTimeoutMs());
    context.addReceivingMailboxes(mailboxReceiveOperator.getSendingMailbox());
    return mailboxReceiveOperator;
  }

  @Override
  public MultiStageOperator visitMailboxSend(MailboxSendNode node, PlanRequestContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    StageMetadata receivingStageMetadata = context.getMetadataMap().get(node.getReceiverStageId());
    return new MailboxSendOperator(context.getMailboxService(), nextOperator,
        receivingStageMetadata.getServerInstances(), node.getExchangeType(), node.getPartitionKeySelector(),
        context.getServer(), context.getRequestId(), node.getStageId(), node.getReceiverStageId(),
        context.getDeadlineMs());
  }

  @Override
  public MultiStageOperator visitAggregate(AggregateNode node, PlanRequestContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new AggregateOperator(nextOperator, node.getDataSchema(), node.getAggCalls(), node.getGroupSet(),
        node.getInputs().get(0).getDataSchema(), context._requestId, context._stageId, context.getServer());
  }

  @Override
  public MultiStageOperator visitWindow(WindowNode node, PlanRequestContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new WindowAggregateOperator(nextOperator, node.getGroupSet(), node.getOrderSet(),
        node.getOrderSetDirection(), node.getOrderSetNullDirection(), node.getAggCalls(), node.getLowerBound(),
        node.getUpperBound(), node.getWindowFrameType(), node.getConstants(), node.getDataSchema(),
        node.getInputs().get(0).getDataSchema(), context._requestId, context._stageId, context.getServer());
  }

  @Override
  public MultiStageOperator visitFilter(FilterNode node, PlanRequestContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new FilterOperator(nextOperator, node.getDataSchema(), node.getCondition(), context.getRequestId(),
        context.getStageId(), context.getServer());
  }

  @Override
  public MultiStageOperator visitJoin(JoinNode node, PlanRequestContext context) {
    StageNode left = node.getInputs().get(0);
    StageNode right = node.getInputs().get(1);

    MultiStageOperator leftOperator = left.visit(this, context);
    MultiStageOperator rightOperator = right.visit(this, context);

    return new HashJoinOperator(leftOperator, rightOperator, left.getDataSchema(), node, context.getRequestId(),
        context.getStageId(), context.getServer());
  }

  @Override
  public MultiStageOperator visitProject(ProjectNode node, PlanRequestContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new TransformOperator(nextOperator, node.getDataSchema(), node.getProjects(),
        node.getInputs().get(0).getDataSchema(), context.getRequestId(), context.getStageId(), context.getServer());
  }

  @Override
  public MultiStageOperator visitSort(SortNode node, PlanRequestContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new SortOperator(nextOperator, node.getCollationKeys(), node.getCollationDirections(), node.getFetch(),
        node.getOffset(), node.getDataSchema(), context.getRequestId(), context.getStageId(), context.getServer());
  }

  @Override
  public MultiStageOperator visitTableScan(TableScanNode node, PlanRequestContext context) {
    throw new UnsupportedOperationException("Stage node of type TableScanNode is not supported!");
  }

  @Override
  public MultiStageOperator visitValue(ValueNode node, PlanRequestContext context) {
    return new LiteralValueOperator(node.getDataSchema(), node.getLiteralRows(), context.getRequestId(),
        context.getStageId(), context.getServer());
  }
}
