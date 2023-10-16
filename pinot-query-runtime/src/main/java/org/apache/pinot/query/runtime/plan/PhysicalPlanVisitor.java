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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.FilterOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.IntersectOperator;
import org.apache.pinot.query.runtime.operator.LiteralValueOperator;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MinusOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.SortOperator;
import org.apache.pinot.query.runtime.operator.SortedMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.TransformOperator;
import org.apache.pinot.query.runtime.operator.UnionOperator;
import org.apache.pinot.query.runtime.operator.WindowAggregateOperator;


/**
 * This visitor constructs a physical plan of operators from a {@link PlanNode} tree. Note that
 * this works only for the intermediate stage nodes, leaf stage nodes are expected to compile into
 * v1 operators at this point in time.
 *
 * <p>This class should be used statically via {@link #walkPlanNode(PlanNode, OpChainExecutionContext)}
 */
public class PhysicalPlanVisitor implements PlanNodeVisitor<MultiStageOperator, OpChainExecutionContext> {

  private static final PhysicalPlanVisitor INSTANCE = new PhysicalPlanVisitor();

  public static OpChain walkPlanNode(PlanNode node, OpChainExecutionContext context) {
    MultiStageOperator root = node.visit(INSTANCE, context);
    return new OpChain(context, root);
  }

  @Override
  public MultiStageOperator visitMailboxReceive(MailboxReceiveNode node, OpChainExecutionContext context) {
    if (node.isSortOnReceiver()) {
      return new SortedMailboxReceiveOperator(context, node.getDistributionType(), node.getDataSchema(),
          node.getCollationKeys(), node.getCollationDirections(), node.getCollationNullDirections(),
          node.isSortOnSender(), node.getSenderStageId());
    } else {
      return new MailboxReceiveOperator(context, node.getDistributionType(), node.getSenderStageId());
    }
  }

  @Override
  public MultiStageOperator visitMailboxSend(MailboxSendNode node, OpChainExecutionContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new MailboxSendOperator(context, nextOperator, node.getDistributionType(), node.getDistributionKeys(),
        node.getCollationKeys(), node.getCollationDirections(), node.isSortOnSender(), node.getReceiverStageId());
  }

  @Override
  public MultiStageOperator visitAggregate(AggregateNode node, OpChainExecutionContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new AggregateOperator(context, nextOperator, node.getDataSchema(), node.getAggCalls(),
        node.getGroupSet(), node.getAggType(), node.getFilterArgIndices(), node.getNodeHint());
  }

  @Override
  public MultiStageOperator visitWindow(WindowNode node, OpChainExecutionContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new WindowAggregateOperator(context, nextOperator, node.getGroupSet(), node.getOrderSet(),
        node.getOrderSetDirection(), node.getOrderSetNullDirection(), node.getAggCalls(), node.getLowerBound(),
        node.getUpperBound(), node.getWindowFrameType(), node.getConstants(), node.getDataSchema(),
        node.getInputs().get(0).getDataSchema());
  }

  @Override
  public MultiStageOperator visitSetOp(SetOpNode setOpNode, OpChainExecutionContext context) {
    List<MultiStageOperator> inputs = new ArrayList<>();
    for (PlanNode input : setOpNode.getInputs()) {
      MultiStageOperator visited = input.visit(this, context);
      inputs.add(visited);
    }
    switch (setOpNode.getSetOpType()) {
      case UNION:
        return new UnionOperator(context, inputs, setOpNode.getInputs().get(0).getDataSchema());
      case INTERSECT:
        return new IntersectOperator(context, inputs, setOpNode.getInputs().get(0).getDataSchema());
      case MINUS:
        return new MinusOperator(context, inputs, setOpNode.getInputs().get(0).getDataSchema());
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public MultiStageOperator visitExchange(ExchangeNode exchangeNode, OpChainExecutionContext context) {
    throw new UnsupportedOperationException("ExchangeNode should not be visited");
  }

  @Override
  public MultiStageOperator visitFilter(FilterNode node, OpChainExecutionContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new FilterOperator(context, nextOperator, node.getDataSchema(), node.getCondition());
  }

  @Override
  public MultiStageOperator visitJoin(JoinNode node, OpChainExecutionContext context) {
    PlanNode left = node.getInputs().get(0);
    PlanNode right = node.getInputs().get(1);

    MultiStageOperator leftOperator = left.visit(this, context);
    MultiStageOperator rightOperator = right.visit(this, context);

    return new HashJoinOperator(context, leftOperator, rightOperator, left.getDataSchema(), node);
  }

  @Override
  public MultiStageOperator visitProject(ProjectNode node, OpChainExecutionContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    return new TransformOperator(context, nextOperator, node.getDataSchema(), node.getProjects(),
        node.getInputs().get(0).getDataSchema());
  }

  @Override
  public MultiStageOperator visitSort(SortNode node, OpChainExecutionContext context) {
    MultiStageOperator nextOperator = node.getInputs().get(0).visit(this, context);
    boolean isInputSorted = nextOperator instanceof SortedMailboxReceiveOperator;
    return new SortOperator(context, nextOperator, node.getCollationKeys(), node.getCollationDirections(),
        node.getCollationNullDirections(), node.getFetch(), node.getOffset(), node.getDataSchema(), isInputSorted);
  }

  @Override
  public MultiStageOperator visitTableScan(TableScanNode node, OpChainExecutionContext context) {
    throw new UnsupportedOperationException("Stage node of type TableScanNode is not supported!");
  }

  @Override
  public MultiStageOperator visitValue(ValueNode node, OpChainExecutionContext context) {
    return new LiteralValueOperator(context, node.getDataSchema(), node.getLiteralRows());
  }
}
