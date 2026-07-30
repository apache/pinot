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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.physical.v2.PlanFragmentAndMailboxAssignment;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.validation.ArrayToMvValidationVisitor;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.routing.WorkerMetadata;


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
  public DispatchableSubPlan createDispatchableSubPlan(SubPlan subPlan, MultiClusterRoutingContext routingContext) {
    // perform physical plan conversion and assign workers to each stage.
    // metadata may come directly from Calcite's RelNode which has not resolved actual table names (taking
    // case-sensitivity into account) yet, so we need to ensure table names are resolved while creating the subplan.
    DispatchablePlanContext context = new DispatchablePlanContext(_workerManager, _requestId, _plannerContext,
        subPlan.getSubPlanMetadata().getFields(),
        resolveActualTableNames(subPlan.getSubPlanMetadata().getTableNames()));
    PlanFragment rootFragment = subPlan.getSubPlanRoot();
    PlanNode rootNode = rootFragment.getFragmentRoot();
    // 1. start by visiting the sub plan fragment root.
    rootNode.visit(new DispatchablePlanVisitor(_tableCache, routingContext), context);
    // 2. add a special stage for the global mailbox receive, this runs on the dispatcher.
    context.getDispatchablePlanStageRootMap().put(0, rootNode);
    // 3. add worker assignment after the dispatchable plan context is fulfilled after the visit.
    context.getWorkerManager().assignWorkers(rootFragment, context);
    // 4. compute the mailbox assignment for each stage.
    rootNode.visit(MailboxAssignmentVisitor.INSTANCE, context);
    // 5. Run validations
    runValidations(rootFragment, context);
    // 6. convert it into query plan.
    return finalizeDispatchableSubPlan(rootFragment, context);
  }

  /// Returns the actual table names taking the case-sensitivity configured within the `TableCache` instance into
  /// account.
  private Set<String> resolveActualTableNames(Set<String> tableNames) {
    Set<String> actualTableNames = new HashSet<>();
    for (String tableName : tableNames) {
      String actualTableName = _tableCache.getActualTableName(tableName);
      if (actualTableName != null) {
        actualTableNames.add(actualTableName);
      } else {
        actualTableNames.add(tableName);
      }
    }
    return actualTableNames;
  }

  public DispatchableSubPlan createDispatchableSubPlanV2(SubPlan subPlan,
      PlanFragmentAndMailboxAssignment.Result result) {
    // perform physical plan conversion and assign workers to each stage.
    DispatchablePlanContext context = new DispatchablePlanContext(_workerManager, _requestId, _plannerContext,
        subPlan.getSubPlanMetadata().getFields(), subPlan.getSubPlanMetadata().getTableNames());
    PlanFragment rootFragment = subPlan.getSubPlanRoot();
    context.getDispatchablePlanMetadataMap().putAll(result._fragmentMetadataMap);
    for (var entry : result._planFragmentMap.entrySet()) {
      context.getDispatchablePlanStageRootMap().put(entry.getKey(), entry.getValue().getFragmentRoot());
    }
    trackEmptyLeafStages(context);
    runValidations(rootFragment, context);
    return finalizeDispatchableSubPlan(rootFragment, context);
  }

  /**
   * Records empty leaf stages so that {@link DispatchablePlanContext#isAllNonReplicatedLeafStagesEmpty()} works for the
   * physical optimizer path, mirroring what {@code WorkerManager} does for the legacy path. A leaf stage is a fragment
   * that scans one or more tables; it is empty when it was assigned zero workers (no routable segments, e.g. an empty
   * table or all segments pruned by the broker). When every leaf stage is empty, this drives the empty-leaf
   * short-circuit in {@link #finalizeDispatchableSubPlan}.
   * <p>
   * When only <i>some</i> leaf stages are empty (an empty or fully-pruned table combined with a non-empty table in the
   * same query), the physical optimizer cannot yet produce a correct plan: an empty leaf is assigned zero workers, and
   * a downstream join/set-op/aggregate derives its worker set from its inputs, so the empty branch can zero out a whole
   * stage and silently drop rows. Rather than return wrong results, fail fast with an actionable error suggesting the
   * legacy (non-physical) engine, which handles this case.
   * <p>
   * Unlike the legacy path (which excludes broadcast-replicated dim leaves from this tracking via
   * {@code WorkerManager}'s early return), every table-scanning fragment is counted here. The physical optimizer does
   * not honor the {@code IS_REPLICATED} broadcast hint (a dim table is routed like any other table), and it never
   * populates {@code replicatedSegments}, so {@link #hasNonEmptyReplicatedLeaf} cannot rescue an incorrectly
   * short-circuited replicated join. Counting every leaf keeps the fail-fast conservative: a query mixing an empty
   * table with a non-empty (dim or fact) table is rejected rather than risking a wrong result.
   */
  private static void trackEmptyLeafStages(DispatchablePlanContext context) {
    int leafStages = 0;
    int emptyLeafStages = 0;
    for (DispatchablePlanMetadata metadata : context.getDispatchablePlanMetadataMap().values()) {
      if (metadata.getScannedTables().isEmpty()) {
        // Not a leaf stage (no table scan).
        continue;
      }
      leafStages++;
      context.recordLeafStageAssigned();
      if (metadata.getWorkerIdToServerInstanceMap().isEmpty()) {
        emptyLeafStages++;
        context.recordLeafStageEmpty();
      }
    }
    if (emptyLeafStages > 0 && emptyLeafStages < leafStages) {
      throw new UnsupportedOperationException("The multi-stage physical optimizer does not yet support queries that "
          + "combine an empty or fully-pruned table with a non-empty table (e.g. a join or union where one side has "
          + "no routable segments). Retry with the query option 'usePhysicalOptimizer=false'.");
    }
  }

  /**
   * Run validations on the plan. Since there is only one validator right now, don't try to over-engineer it.
   */
  private void runValidations(PlanFragment planFragment, DispatchablePlanContext context) {
    PlanNode rootPlanNode = planFragment.getFragmentRoot();
    boolean isIntermediateStage =
        context.getDispatchablePlanMetadataMap().get(rootPlanNode.getStageId()).getScannedTables().isEmpty();
    rootPlanNode.visit(ArrayToMvValidationVisitor.INSTANCE, isIntermediateStage);
    for (PlanFragment child : planFragment.getChildren()) {
      runValidations(child, context);
    }
  }

  private static DispatchableSubPlan finalizeDispatchableSubPlan(PlanFragment subPlanRoot,
      DispatchablePlanContext dispatchablePlanContext) {
    // Empty leaf stages are tracked for both engines: the legacy path in WorkerManager, and the physical optimizer
    // path in #trackEmptyLeafStages.
    boolean allLeafStagesEmpty = dispatchablePlanContext.isAllNonReplicatedLeafStagesEmpty();
    if (allLeafStagesEmpty && hasNonEmptyReplicatedLeaf(dispatchablePlanContext.getDispatchablePlanMetadataMap())) {
      allLeafStagesEmpty = false;
    }
    Map<Integer, DispatchablePlanFragment> fragmentMap =
        dispatchablePlanContext.constructDispatchablePlanFragmentMap(subPlanRoot);
    if (allLeafStagesEmpty) {
      rewriteReduceStageForEmptyLeaves(fragmentMap);
      // Drop orphan stages so EXPLAIN PLAN and stats consumers don't see unreferenced entries.
      fragmentMap.keySet().retainAll(Set.of(0));
    }
    return new DispatchableSubPlan(dispatchablePlanContext.getResultFields(),
        fragmentMap,
        dispatchablePlanContext.getTableNames(),
        populateTableUnavailableSegments(dispatchablePlanContext.getDispatchablePlanMetadataMap()),
        dispatchablePlanContext.getNumSegmentsPrunedByBroker(),
        allLeafStagesEmpty);
  }

  static boolean hasNonEmptyReplicatedLeaf(Map<Integer, DispatchablePlanMetadata> metadataMap) {
    for (DispatchablePlanMetadata metadata : metadataMap.values()) {
      Map<String, List<String>> replicatedSegments = metadata.getReplicatedSegments();
      if (replicatedSegments == null) {
        continue;
      }
      for (List<String> segments : replicatedSegments.values()) {
        if (segments != null && !segments.isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  public static void rewriteReduceStageForEmptyLeaves(Map<Integer, DispatchablePlanFragment> fragmentMap) {
    DispatchablePlanFragment reduceStage = fragmentMap.get(0);
    List<WorkerMetadata> workerMetadataList = reduceStage.getWorkerMetadataList();
    if (workerMetadataList.isEmpty()) {
      return;
    }
    PlanNode inlinedRoot = inlineAllLeafStagesEmptyInputs(reduceStage.getPlanFragment().getFragmentRoot());
    if (inlinedRoot != reduceStage.getPlanFragment().getFragmentRoot()) {
      // Inlined nodes originally belonged to leaf stages (ID 1, 2, etc.) but now execute within
      // the broker reduce stage (ID 0). PlanFragment's constructor asserts that the root's stageId
      // matches the fragmentId, so we must update all node IDs before wrapping in a new fragment.
      setStageIdRecursively(inlinedRoot, 0);
      reduceStage = DispatchablePlanFragment.copyWithRoot(reduceStage, inlinedRoot);
      fragmentMap.put(0, reduceStage);
    }
    WorkerMetadata workerMetadata = workerMetadataList.get(0);
    reduceStage.setWorkerMetadataList(List.of(
        new WorkerMetadata(workerMetadata.getWorkerId(), Map.of(), workerMetadata.getCustomProperties())));
  }

  private static PlanNode inlineAllLeafStagesEmptyInputs(PlanNode node) {
    if (node instanceof TableScanNode) {
      return new ValueNode(node.getStageId(), node.getDataSchema(), node.getNodeHint(), List.of(), List.of());
    }
    if (node instanceof MailboxReceiveNode) {
      MailboxReceiveNode mailboxReceiveNode = (MailboxReceiveNode) node;
      MailboxSendNode sender = mailboxReceiveNode.getSender();
      List<PlanNode> senderInputs = sender.getInputs();
      Preconditions.checkState(!senderInputs.isEmpty(),
          "MailboxSendNode (stageId=%s) has no inputs", sender.getStageId());
      return inlineAllLeafStagesEmptyInputs(senderInputs.get(0));
    }
    List<PlanNode> inputs = node.getInputs();
    if (inputs.isEmpty()) {
      return node;
    }
    boolean changed = false;
    List<PlanNode> inlinedInputs = new ArrayList<>(inputs.size());
    for (PlanNode input : inputs) {
      PlanNode inlinedInput = inlineAllLeafStagesEmptyInputs(input);
      inlinedInputs.add(inlinedInput);
      changed |= inlinedInput != input;
    }
    return changed ? node.withInputs(inlinedInputs) : node;
  }

  private static void setStageIdRecursively(PlanNode node, int stageId) {
    node.setStageId(stageId);
    for (PlanNode input : node.getInputs()) {
      setStageIdRecursively(input, stageId);
    }
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
