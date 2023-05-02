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
package org.apache.pinot.query.planner.physical;

import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SetOpNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.query.planner.stage.WindowNode;


public class DispatchablePlanVisitor implements StageNodeVisitor<Void, DispatchablePlanContext> {
  public static final DispatchablePlanVisitor INSTANCE = new DispatchablePlanVisitor();

  private DispatchablePlanVisitor() {
  }

  /**
   * Entry point for attaching dispatch metadata to a query plan. It walks through the plan via the global
   * {@link StageNode} root of the query and:
   * <ul>
   *   <li>break down the {@link StageNode}s into Stages that can run on a single worker.</li>
   *   <li>each stage is represented by a subset of {@link StageNode}s without data exchange.</li>
   *   <li>attach worker execution information including physical server address, worker ID to each stage.</li>
   * </ul>
   *
   * @param globalReceiverNode the entrypoint of the stage plan.
   * @param dispatchablePlanContext dispatchable plan context used to record the walk of the stage node tree.
   */
  public QueryPlan constructDispatchablePlan(StageNode globalReceiverNode,
      DispatchablePlanContext dispatchablePlanContext) {
    // 1. start by visiting the stage root.
    globalReceiverNode.visit(DispatchablePlanVisitor.INSTANCE, dispatchablePlanContext);
    // 2. add a special stage for the global mailbox receive, this runs on the dispatcher.
    dispatchablePlanContext.getDispatchablePlanStageRootMap().put(0, globalReceiverNode);
    // 3. add worker assignment after the dispatchable plan context is fulfilled after the visit.
    computeWorkerAssignment(globalReceiverNode, dispatchablePlanContext);
    // 4. compute the mailbox assignment for each stage.
    computeMailboxAssignment(dispatchablePlanContext);
    // 5. convert it into query plan.
    return finalizeQueryPlan(dispatchablePlanContext);
  }

  private void computeMailboxAssignment(DispatchablePlanContext dispatchablePlanContext) {
    dispatchablePlanContext.getDispatchablePlanStageRootMap().values().forEach(stageNode -> {
      stageNode.visit(MailboxAssignmentVisitor.INSTANCE, dispatchablePlanContext);
    });
  }

  private static QueryPlan finalizeQueryPlan(DispatchablePlanContext dispatchablePlanContext) {
    return new QueryPlan(dispatchablePlanContext.getResultFields(),
        dispatchablePlanContext.getDispatchablePlanStageRootMap(),
        dispatchablePlanContext.getDispatchablePlanMetadataMap());
  }

  private static DispatchablePlanMetadata getOrCreateDispatchablePlanMetadata(StageNode node,
      DispatchablePlanContext context) {
    return context.getDispatchablePlanMetadataMap().computeIfAbsent(node.getStageId(),
        (id) -> new DispatchablePlanMetadata());
  }

  private static void computeWorkerAssignment(StageNode node, DispatchablePlanContext context) {
    int stageId = node.getStageId();
    context.getWorkerManager().assignWorkerToStage(stageId, context.getDispatchablePlanMetadataMap().get(stageId),
        context.getRequestId(), context.getPlannerContext().getOptions(), context.getTableNames());
  }

  @Override
  public Void visitAggregate(AggregateNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    DispatchablePlanMetadata dispatchablePlanMetadata = getOrCreateDispatchablePlanMetadata(node, context);
    dispatchablePlanMetadata.setRequireSingleton(node.getGroupSet().size() == 0 && AggregateNode.isFinalStage(node));
    return null;
  }

  @Override
  public Void visitWindow(WindowNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    DispatchablePlanMetadata dispatchablePlanMetadata = getOrCreateDispatchablePlanMetadata(node, context);
    // TODO: Figure out a way to parallelize Empty OVER() and OVER(ORDER BY) so the computation can be done across
    //       multiple nodes.
    // Empty OVER() and OVER(ORDER BY) need to be processed on a singleton node. OVER() with PARTITION BY can be
    // distributed as no global ordering is required across partitions.
    dispatchablePlanMetadata.setRequireSingleton(node.getGroupSet().size() == 0);
    return null;
  }

  @Override
  public Void visitSetOp(SetOpNode setOpNode, DispatchablePlanContext context) {
    setOpNode.getInputs().forEach(input -> input.visit(this, context));
    getOrCreateDispatchablePlanMetadata(setOpNode, context);
    return null;
  }

  @Override
  public Void visitFilter(FilterNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, DispatchablePlanContext context) {
    node.getInputs().forEach(join -> join.visit(this, context));
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, DispatchablePlanContext context) {
    node.getSender().visit(this, context);
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    getOrCreateDispatchablePlanMetadata(node, context);

    context.getDispatchablePlanStageRootMap().put(node.getStageId(), node);
    computeWorkerAssignment(node, context);
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }

  @Override
  public Void visitSort(SortNode node, DispatchablePlanContext context) {
    node.getInputs().get(0).visit(this, context);
    DispatchablePlanMetadata dispatchablePlanMetadata = getOrCreateDispatchablePlanMetadata(node, context);
    dispatchablePlanMetadata.setRequireSingleton(node.getCollationKeys().size() > 0 && node.getOffset() != -1);
    return null;
  }

  @Override
  public Void visitTableScan(TableScanNode node, DispatchablePlanContext context) {
    DispatchablePlanMetadata dispatchablePlanMetadata = getOrCreateDispatchablePlanMetadata(node, context);
    dispatchablePlanMetadata.addScannedTable(node.getTableName());
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, DispatchablePlanContext context) {
    getOrCreateDispatchablePlanMetadata(node, context);
    return null;
  }
}
