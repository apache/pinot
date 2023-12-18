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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.physical.colocated.GreedyShuffleRewriteVisitor;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.validation.ArrayToMvValidationVisitor;
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
    DispatchablePlanContext context = new DispatchablePlanContext(_workerManager, _requestId, _plannerContext,
        subPlan.getSubPlanMetadata().getFields(), subPlan.getSubPlanMetadata().getTableNames());
    PlanFragment rootFragment = subPlan.getSubPlanRoot();
    PlanNode rootNode = rootFragment.getFragmentRoot();
    // 1. start by visiting the sub plan fragment root.
    rootNode.visit(DispatchablePlanVisitor.INSTANCE, context);
    // 2. add a special stage for the global mailbox receive, this runs on the dispatcher.
    context.getDispatchablePlanStageRootMap().put(0, rootNode);
    // 3. add worker assignment after the dispatchable plan context is fulfilled after the visit.
    context.getWorkerManager().assignWorkers(rootFragment, context);
    // 4. compute the mailbox assignment for each stage.
    rootNode.visit(MailboxAssignmentVisitor.INSTANCE, context);
    // 5. Run physical optimizations
    runPhysicalOptimizers(rootNode, context, _tableCache);
    // 6. Run validations
    runValidations(rootFragment, context);
    // 7. convert it into query plan.
    return finalizeDispatchableSubPlan(rootFragment, context);
  }

  /**
   * Run validations on the plan. Since there is only one validator right now, don't try to over-engineer it.
   */
  private void runValidations(PlanFragment planFragment, DispatchablePlanContext context) {
    PlanNode rootPlanNode = planFragment.getFragmentRoot();
    boolean isIntermediateStage =
        context.getDispatchablePlanMetadataMap().get(rootPlanNode.getPlanFragmentId()).getScannedTables().isEmpty();
    rootPlanNode.visit(ArrayToMvValidationVisitor.INSTANCE, isIntermediateStage);
    for (PlanFragment child : planFragment.getChildren()) {
      runValidations(child, context);
    }
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
        dispatchablePlanContext.constructDispatchablePlanFragmentList(subPlanRoot),
        dispatchablePlanContext.getTableNames(),
        populateTableUnavailableSegments(dispatchablePlanContext.getDispatchablePlanMetadataMap()));
  }

  private static Map<String, Set<String>> populateTableUnavailableSegments(
      Map<Integer, DispatchablePlanMetadata> dispatchablePlanMetadataMap) {
    Map<String, Set<String>> tableToUnavailableSegments = new HashMap<>();
    dispatchablePlanMetadataMap.values().forEach(metadata -> metadata.getTableToUnavailableSegmentsMap().forEach(
        (tableName, unavailableSegments) -> tableToUnavailableSegments.computeIfAbsent(tableName, k -> new HashSet<>())
            .addAll(unavailableSegments)));
    return tableToUnavailableSegments;
  }
}
