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
package org.apache.pinot.query.planner.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.SubPlanMetadata;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelNodeTreeValidator;
import org.apache.pinot.query.planner.physical.v2.PlanFragmentAndMailboxAssignment;
import org.apache.pinot.query.planner.plannode.BasePlanNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.spi.utils.CommonConstants;


/// PinotLogicalQueryPlanner walks top-down from [RelRoot] and construct a forest of trees with [PlanNode].
public class PinotLogicalQueryPlanner {
  private PinotLogicalQueryPlanner() {
  }

  /// Converts a Calcite [RelRoot] into a Pinot [SubPlan].
  public static SubPlan makePlan(RelRoot relRoot,
      @Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker, boolean useSpools,
      String hashFunction, boolean pruneUnnestColumns) {
    return makePlan(relRoot, tracker, useSpools, hashFunction, pruneUnnestColumns, false);
  }

  /// Converts a Calcite [RelRoot] into a Pinot [SubPlan], optionally capturing the optimizer's row
  /// count estimate for each node so it can later be compared against what the runtime reports.
  public static SubPlan makePlan(RelRoot relRoot,
      @Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker, boolean useSpools,
      String hashFunction, boolean pruneUnnestColumns, boolean captureEstimatedRowCounts) {
    RelToPlanNodeConverter converter = new RelToPlanNodeConverter(tracker, hashFunction,
        !CommonConstants.Helix.DEFAULT_ENABLE_CASE_INSENSITIVE, pruneUnnestColumns, captureEstimatedRowCounts);
    PlanNode rootNode = converter.toPlanNode(relRoot.rel);

    IdentityHashMap<PlanNode, Double> estimatedRowCounts = converter.getEstimatedRowCounts();
    PlanFragment rootFragment =
        planNodeToPlanFragment(rootNode, tracker, useSpools, hashFunction, estimatedRowCounts);
    return new SubPlan(rootFragment,
        new SubPlanMetadata(RelToPlanNodeConverter.getTableNamesFromRelRoot(relRoot.rel), relRoot.fields), List.of(),
        estimatedRowCounts);

    // TODO: Currently we don't support multiple sub-plans. Revisit the following logic when we add the support.
    // Fragment the stage tree into multiple SubPlans.
//    SubPlanFragmenter.Context subPlanContext = new SubPlanFragmenter.Context();
//    subPlanContext._subPlanIdToRootNodeMap.put(0, rootNode);
//    subPlanContext._subPlanIdToMetadataMap.put(0,
//        new SubPlanMetadata(RelToPlanNodeConverter.getTableNamesFromRelRoot(relRoot.rel), relRoot.fields));
//    rootNode.visit(SubPlanFragmenter.INSTANCE, subPlanContext);
//
//    Map<Integer, SubPlan> subPlanMap = new HashMap<>();
//    for (Map.Entry<Integer, PlanNode> subPlanEntry : subPlanContext._subPlanIdToRootNodeMap.entrySet()) {
//      SubPlan subPlan =
//          new SubPlan(planNodeToPlanFragment(subPlanEntry.getValue()), subPlanContext._subPlanIdToMetadataMap.get(0),
//              new ArrayList<>());
//      subPlanMap.put(subPlanEntry.getKey(), subPlan);
//    }
//    for (Map.Entry<Integer, List<Integer>> subPlanToChildrenEntry : subPlanContext._subPlanIdToChildrenMap.entrySet
//    ()) {
//      int subPlanId = subPlanToChildrenEntry.getKey();
//      List<Integer> subPlanChildren = subPlanToChildrenEntry.getValue();
//      for (int subPlanChild : subPlanChildren) {
//        subPlanMap.get(subPlanId).getChildren().add(subPlanMap.get(subPlanChild));
//      }
//    }
//    return subPlanMap.get(0);
  }

  public static Pair<SubPlan, PlanFragmentAndMailboxAssignment.Result> makePlanV2(RelRoot relRoot,
      PhysicalPlannerContext physicalPlannerContext) {
    PRelNode pRelNode = (PRelNode) relRoot.rel;
    // TODO(mse-physical): Don't emit metrics for explain statements.
    PRelNodeTreeValidator.emitMetrics(pRelNode);
    PlanFragmentAndMailboxAssignment planFragmentAndMailboxAssignment = new PlanFragmentAndMailboxAssignment();
    PlanFragmentAndMailboxAssignment.Result result =
        planFragmentAndMailboxAssignment.compute(pRelNode, physicalPlannerContext);
    PlanFragment rootFragment = result._planFragmentMap.get(0);
    SubPlan subPlan = new SubPlan(rootFragment,
        new SubPlanMetadata(RelToPlanNodeConverter.getTableNamesFromRelRoot(relRoot.rel), relRoot.fields), List.of());
    return Pair.of(subPlan, result);
  }

  private static PlanFragment planNodeToPlanFragment(
      PlanNode node, @Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker, boolean useSpools,
      String hashFunction, IdentityHashMap<PlanNode, Double> estimatedRowCounts) {
    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.Context fragmenterContext = fragmenter.createContext();
    node = node.visit(fragmenter, fragmenterContext);

    if (useSpools) {
      GroupedStages equivalentStages = EquivalentStagesFinder.findEquivalentStages(node);
      EquivalentStagesReplacer.replaceEquivalentStages(node, equivalentStages, fragmenter);
    }

    Int2ObjectOpenHashMap<PlanFragment> planFragmentMap = fragmenter.getPlanFragmentMap();
    Int2ObjectOpenHashMap<IntList> childPlanFragmentIdsMap = fragmenter.getChildPlanFragmentIdsMap();

    // Sub plan root needs to send final results back to the Broker
    // TODO: Should be SINGLETON (currently SINGLETON has to be local, so use BROADCAST_DISTRIBUTED instead)
    MailboxSendNode subPlanRootSenderNode =
        new MailboxSendNode(node.getStageId(), node.getDataSchema(), List.of(node), 0,
            PinotRelExchangeType.getDefaultExchangeType(), RelDistribution.Type.BROADCAST_DISTRIBUTED, null, false,
            null, false, hashFunction);
    PlanFragment planFragment1 = new PlanFragment(1, subPlanRootSenderNode, new ArrayList<>());
    planFragmentMap.put(1, planFragment1);
    for (Int2ObjectMap.Entry<IntList> entry : childPlanFragmentIdsMap.int2ObjectEntrySet()) {
      PlanFragment planFragment = planFragmentMap.get(entry.getIntKey());
      List<PlanFragment> childPlanFragments = planFragment.getChildren();
      IntListIterator childPlanFragmentIdIterator = entry.getValue().iterator();
      while (childPlanFragmentIdIterator.hasNext()) {
        childPlanFragments.add(planFragmentMap.get(childPlanFragmentIdIterator.nextInt()));
      }
    }
    MailboxReceiveNode rootReceiveNode = new MailboxReceiveNode(0, node.getDataSchema(), node.getStageId(),
        PinotRelExchangeType.getDefaultExchangeType(), RelDistribution.Type.BROADCAST_DISTRIBUTED, null, null, false,
        false, subPlanRootSenderNode);

    if (tracker != null) {
      RelNode rootRelNode = tracker.getCreatorOf(node);
      Preconditions.checkState(rootRelNode != null, "Root RelNode not found for PlanNode: %s", node);
      tracker.trackCreation(rootRelNode, subPlanRootSenderNode);
      Iterator<Map.Entry<? extends BasePlanNode, ExchangeNode>> it = Iterators.concat(
          fragmenter.getMailboxSendToExchangeNodeMap().entrySet().iterator(),
          fragmenter.getMailboxReceiveToExchangeNodeMap().entrySet().iterator()
      );
      while (it.hasNext()) {
        Map.Entry<? extends BasePlanNode, ExchangeNode> entry = it.next();
        ExchangeNode exchangeNode = entry.getValue();
        RelNode originalNode = tracker.getCreatorOf(exchangeNode);
        if (originalNode == null) {
          throw new IllegalStateException("Original node not found for exchange node: " + exchangeNode);
        }
        tracker.trackCreation(originalNode, entry.getKey());
      }
    }

    if (!estimatedRowCounts.isEmpty()) {
      copyEstimatesToMailboxNodes(fragmenter, node, subPlanRootSenderNode, rootReceiveNode, estimatedRowCounts);
    }

    return new PlanFragment(0, rootReceiveNode, List.of(planFragment1));
  }

  /// Carries row-count estimates across the one boundary at which fragmentation does not preserve
  /// plan node identity.
  ///
  /// [PlanFragmenter#process] rewrites ordinary nodes in place, so their estimates survive as they
  /// are. Exchanges do not: [PlanFragmenter] discards each [ExchangeNode] and builds a fresh
  /// [MailboxSendNode]/[MailboxReceiveNode] pair in its place. Without this copy, no stage root and
  /// no mailbox receive leaf would ever carry an estimate — the field would be missing from exactly
  /// the nodes a reader looks at first. The fragmenter retains the mapping back to the discarded
  /// exchange for this reason, and the transformation tracker above uses it the same way.
  ///
  /// An exchange only moves rows, so its estimate describes both halves of the pair it becomes. The
  /// outermost pair is built here rather than by the fragmenter and carries the plan root's output,
  /// so it takes the root's estimate.
  private static void copyEstimatesToMailboxNodes(PlanFragmenter fragmenter, PlanNode subPlanRoot,
      MailboxSendNode subPlanRootSenderNode, MailboxReceiveNode rootReceiveNode,
      IdentityHashMap<PlanNode, Double> estimatedRowCounts) {
    Iterator<Map.Entry<? extends BasePlanNode, ExchangeNode>> it = Iterators.concat(
        fragmenter.getMailboxSendToExchangeNodeMap().entrySet().iterator(),
        fragmenter.getMailboxReceiveToExchangeNodeMap().entrySet().iterator());
    while (it.hasNext()) {
      Map.Entry<? extends BasePlanNode, ExchangeNode> entry = it.next();
      Double estimate = estimatedRowCounts.get(entry.getValue());
      if (estimate != null) {
        estimatedRowCounts.put(entry.getKey(), estimate);
      }
    }
    Double rootEstimate = estimatedRowCounts.get(subPlanRoot);
    if (rootEstimate != null) {
      estimatedRowCounts.put(subPlanRootSenderNode, rootEstimate);
      estimatedRowCounts.put(rootReceiveNode, rootEstimate);
    }
  }
}
