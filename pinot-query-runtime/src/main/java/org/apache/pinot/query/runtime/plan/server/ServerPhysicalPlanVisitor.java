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
package org.apache.pinot.query.runtime.plan.server;

import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.operator.LeafStageTransferableBlockOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PhysicalPlanVisitor;


/**
 * {@link PhysicalPlanVisitor} for leaf-stage server nodes.
 *
 * The difference is that: on a leaf-stage server node, {@link PlanNode} are split into 2 parts
 * <ul>
 *   <li>nodes that can be compiled into a single {@link org.apache.pinot.common.request.PinotQuery}.</li>
 *   <li>the rest of the nodes that cannot compiled into the first part forms an {@link OpChain}.</li>
 * </ul>
 */
public class ServerPhysicalPlanVisitor extends PhysicalPlanVisitor {

  private static final ServerPhysicalPlanVisitor INSTANCE = new ServerPhysicalPlanVisitor();

  public static OpChain walkPlanNode(PlanNode node, ServerOpChainExecutionContext context) {
    MultiStageOperator root = node.visit(INSTANCE, context);
    return new OpChain(context, root);
  }

  private MultiStageOperator visit(PlanNode node, ServerOpChainExecutionContext context) {
    if (node == context.getLeafStageBoundaryNode()) {
      return new LeafStageTransferableBlockOperator(context, context.getServerQueryRequests(),
          context.getLeafStageBoundaryNode().getDataSchema(), context.getQueryExecutor(), context.getExecutorService());
    } else {
      return null;
    }
  }

  @Override
  public MultiStageOperator visitAggregate(AggregateNode node, OpChainExecutionContext context) {
    MultiStageOperator leafOpt = visit(node, (ServerOpChainExecutionContext) context);
    return leafOpt != null ? leafOpt : super.visitAggregate(node, context);
  }

  @Override
  public MultiStageOperator visitFilter(FilterNode node, OpChainExecutionContext context) {
    MultiStageOperator leafOpt = visit(node, (ServerOpChainExecutionContext) context);
    return leafOpt != null ? leafOpt : super.visitFilter(node, context);
  }

  @Override
  public MultiStageOperator visitJoin(JoinNode node, OpChainExecutionContext context) {
    MultiStageOperator leafOpt = visit(node, (ServerOpChainExecutionContext) context);
    return leafOpt != null ? leafOpt : super.visitJoin(node, context);
  }

  @Override
  public MultiStageOperator visitMailboxReceive(MailboxReceiveNode node, OpChainExecutionContext context) {
    MultiStageOperator leafOpt = visit(node, (ServerOpChainExecutionContext) context);
    return leafOpt != null ? leafOpt : super.visitMailboxReceive(node, context);
  }

  @Override
  public MultiStageOperator visitMailboxSend(MailboxSendNode node, OpChainExecutionContext context) {
    MultiStageOperator leafOpt = visit(node, (ServerOpChainExecutionContext) context);
    return leafOpt != null ? leafOpt : super.visitMailboxSend(node, context);
  }

  @Override
  public MultiStageOperator visitProject(ProjectNode node, OpChainExecutionContext context) {
      ServerOpChainExecutionContext serverContext = (ServerOpChainExecutionContext) context;
      MultiStageOperator leafOpt = visit(node, serverContext);
      return leafOpt != null ? leafOpt : super.visitProject(node, context);
  }

  @Override
  public MultiStageOperator visitSort(SortNode node, OpChainExecutionContext context) {
      ServerOpChainExecutionContext serverContext = (ServerOpChainExecutionContext) context;
      MultiStageOperator leafOpt = visit(node, serverContext);
      return leafOpt != null ? leafOpt : super.visitSort(node, context);
  }

  @Override
  public MultiStageOperator visitTableScan(TableScanNode node, OpChainExecutionContext context) {
    MultiStageOperator leafOpt = visit(node, (ServerOpChainExecutionContext) context);
    if (leafOpt != null) {
      return leafOpt;
    } else {
      throw new UnsupportedOperationException("Cannot visit TableScanNode in server physical plan visitor!");
    }
  }

  @Override
  public MultiStageOperator visitValue(ValueNode node, OpChainExecutionContext context) {
    MultiStageOperator leafOpt = visit(node, (ServerOpChainExecutionContext) context);
    return leafOpt != null ? leafOpt : super.visitValue(node, context);
  }

  @Override
  public MultiStageOperator visitWindow(WindowNode node, OpChainExecutionContext context) {
    MultiStageOperator leafOpt = visit(node, (ServerOpChainExecutionContext) context);
    return leafOpt != null ? leafOpt : super.visitWindow(node, context);
  }

  @Override
  public MultiStageOperator visitSetOp(SetOpNode node, OpChainExecutionContext context) {
    MultiStageOperator leafOpt = visit(node, (ServerOpChainExecutionContext) context);
    return leafOpt != null ? leafOpt : super.visitSetOp(node, context);
  }

  @Override
  public MultiStageOperator visitExchange(ExchangeNode node, OpChainExecutionContext context) {
    throw new UnsupportedOperationException("Cannot visit ExchangeNode in server physical plan visitor!");
  }
}
