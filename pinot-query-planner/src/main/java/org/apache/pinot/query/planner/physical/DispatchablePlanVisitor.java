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

import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.physical.colocated.GreedyShuffleRewriteVisitor;
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


public class DispatchablePlanVisitor implements PlanNodeVisitor<Void, DispatchablePlanContext> {
  public static final DispatchablePlanVisitor INSTANCE = new DispatchablePlanVisitor();

  private DispatchablePlanVisitor() {
  }

  /**
   * Entry point for attaching dispatch metadata to a query plan. It walks through the plan via the global
   * {@link PlanNode} root of the query and:
   * <ul>
   *   <li>break down the {@link PlanNode}s into Stages that can run on a single worker.</li>
   *   <li>each stage is represented by a subset of {@link PlanNode}s without data exchange.</li>
   *   <li>attach worker execution information including physical server address, worker ID to each stage.</li>
   * </ul>
   *
   * @param subPlan the entrypoint of the sub plan.
   * @param dispatchablePlanContext dispatchable plan context used to record the walk of the stage node tree.
   */
  public DispatchableSubPlan constructDispatchableSubPlan(SubPlan subPlan,
      DispatchablePlanContext dispatchablePlanContext, TableCache tableCache) {
    PlanNode subPlanRoot = subPlan.getSubPlanRoot().getFragmentRoot();
    // 1. start by visiting the sub plan fragment root.
    subPlanRoot.visit(DispatchablePlanVisitor.INSTANCE, dispatchablePlanContext);
    // 2. add a special stage for the global mailbox receive, this runs on the dispatcher.
    dispatchablePlanContext.getDispatchablePlanStageRootMap().put(0, subPlanRoot);
    // 3. add worker assignment after the dispatchable plan context is fulfilled after the visit.
    computeWorkerAssignment(subPlanRoot, dispatchablePlanContext);
    // 4. compute the mailbox assignment for each stage.
    // TODO: refactor this to be a pluggable interface.
    computeMailboxAssignment(dispatchablePlanContext);
    // 5. Run physical optimizations
    runPhysicalOptimizers(subPlanRoot, dispatchablePlanContext, tableCache);
    // 6. convert it into query plan.
    // TODO: refactor this to be a pluggable interface.
    return finalizeDispatchableSubPlan(subPlan.getSubPlanRoot(), dispatchablePlanContext);
  }

  private void computeMailboxAssignment(DispatchablePlanContext dispatchablePlanContext) {
    dispatchablePlanContext.getDispatchablePlanStageRootMap().get(0).visit(MailboxAssignmentVisitor.INSTANCE,
        dispatchablePlanContext);
  }

  // TODO: Switch to Worker SPI to avoid multiple-places where workers are assigned.
  private void runPhysicalOptimizers(PlanNode subPlanRoot, DispatchablePlanContext dispatchablePlanContext,
      TableCache tableCache) {
    if (dispatchablePlanContext.getPlannerContext().getOptions().getOrDefault("useColocatedJoin", "false")
        .equals("true")) {
      GreedyShuffleRewriteVisitor.optimizeShuffles(subPlanRoot,
          dispatchablePlanContext.getDispatchablePlanMetadataMap(), tableCache);
    }
  }

  private static DispatchableSubPlan finalizeDispatchableSubPlan(PlanFragment subPlanRoot,
      DispatchablePlanContext dispatchablePlanContext) {
    return new DispatchableSubPlan(dispatchablePlanContext.getResultFields(),
        dispatchablePlanContext.constructDispatchablePlanFragmentMap(subPlanRoot),
        dispatchablePlanContext.getTableNames());
  }

  /**
   * Convert the {@link DispatchablePlanMetadata} into dispatchable info for each stage/worker.
   */
  private static DispatchablePlanMetadata getOrCreateDispatchablePlanMetadata(PlanNode node,
      DispatchablePlanContext context) {
    return context.getDispatchablePlanMetadataMap().computeIfAbsent(node.getPlanFragmentId(),
        (id) -> new DispatchablePlanMetadata());
  }

  private static void computeWorkerAssignment(PlanNode node, DispatchablePlanContext context) {
    int planFragmentId = node.getPlanFragmentId();
    context.getWorkerManager()
        .assignWorkerToStage(planFragmentId, context.getDispatchablePlanMetadataMap().get(planFragmentId),
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
  public Void visitExchange(ExchangeNode exchangeNode, DispatchablePlanContext context) {
    throw new UnsupportedOperationException("ExchangeNode should not be visited by DispatchablePlanVisitor");
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

    context.getDispatchablePlanStageRootMap().put(node.getPlanFragmentId(), node);
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
