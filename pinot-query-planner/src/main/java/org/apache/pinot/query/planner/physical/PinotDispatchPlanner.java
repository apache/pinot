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
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.physical.colocated.GreedyShuffleRewriteVisitor;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.WorkerManager;


public class PinotDispatchPlanner {

  private final WorkerManager _workerManager;
  private final long _requestId;
  private final PlannerContext _plannerContext;

  private final TableCache _tableCache;

  public PinotDispatchPlanner(PlannerContext plannerContext, WorkerManager workerManager, long requestId,
      TableCache tableCache) {
    _plannerContext = plannerContext;
    _workerManager = workerManager;
    _requestId = requestId;
    _tableCache = tableCache;
  }

  /**
   * Entry point for attaching dispatch metadata to a {@link SubPlan}.
   * @param subPlan the entrypoint of the sub plan.
   */
  public DispatchableSubPlan createDispatchableSubPlan(SubPlan subPlan) {
    // perform physical plan conversion and assign workers to each stage.
    DispatchablePlanContext dispatchablePlanContext = new DispatchablePlanContext(_workerManager, _requestId,
        _plannerContext, subPlan.getSubPlanMetadata().getFields(), subPlan.getSubPlanMetadata().getTableNames());
    PlanNode subPlanRoot = subPlan.getSubPlanRoot().getFragmentRoot();
    // 1. start by visiting the sub plan fragment root.
    subPlanRoot.visit(DispatchablePlanVisitor.INSTANCE, dispatchablePlanContext);
    // 2. add a special stage for the global mailbox receive, this runs on the dispatcher.
    dispatchablePlanContext.getDispatchablePlanStageRootMap().put(0, subPlanRoot);
    // 3. add worker assignment after the dispatchable plan context is fulfilled after the visit.
    DispatchablePlanVisitor.computeWorkerAssignment(subPlanRoot, dispatchablePlanContext);
    // 4. compute the mailbox assignment for each stage.
    // TODO: refactor this to be a pluggable interface.
    computeMailboxAssignment(dispatchablePlanContext);
    // 5. Run physical optimizations
    runPhysicalOptimizers(subPlanRoot, dispatchablePlanContext, _tableCache);
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
}
