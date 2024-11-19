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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.SubPlanMetadata;
import org.apache.pinot.query.planner.plannode.BasePlanNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * PinotLogicalQueryPlanner walks top-down from {@link RelRoot} and construct a forest of trees with {@link PlanNode}.
 */
public class PinotLogicalQueryPlanner {
  private PinotLogicalQueryPlanner() {
  }

  /**
   * Converts a Calcite {@link RelRoot} into a Pinot {@link SubPlan}.
   */
  public static SubPlan makePlan(RelRoot relRoot,
      @Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker) {
    PlanNode rootNode = new RelToPlanNodeConverter(tracker).toPlanNode(relRoot.rel);

    PlanFragment rootFragment = planNodeToPlanFragment(rootNode, tracker);
    return new SubPlan(rootFragment,
        new SubPlanMetadata(RelToPlanNodeConverter.getTableNamesFromRelRoot(relRoot.rel), relRoot.fields), List.of());

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

  private static PlanFragment planNodeToPlanFragment(
      PlanNode node, @Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker) {
    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.Context fragmenterContext = fragmenter.createContext();
    node = node.visit(fragmenter, fragmenterContext);
    Int2ObjectOpenHashMap<PlanFragment> planFragmentMap = fragmenter.getPlanFragmentMap();
    Int2ObjectOpenHashMap<IntList> childPlanFragmentIdsMap = fragmenter.getChildPlanFragmentIdsMap();

    // Sub plan root needs to send final results back to the Broker
    // TODO: Should be SINGLETON (currently SINGLETON has to be local, so use BROADCAST_DISTRIBUTED instead)
    MailboxSendNode subPlanRootSenderNode =
        new MailboxSendNode(node.getStageId(), node.getDataSchema(), List.of(node), null,
            PinotRelExchangeType.getDefaultExchangeType(), RelDistribution.Type.BROADCAST_DISTRIBUTED, null, false,
            null, false);
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

    return new PlanFragment(0, rootReceiveNode, Collections.singletonList(planFragment1));
  }
}
