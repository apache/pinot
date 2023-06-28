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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.PlanFragmentMetadata;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.QueryPlanMetadata;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.planner.SubPlanMetadata;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * PinotLogicalQueryPlanner walks top-down from {@link RelRoot} and construct a forest of trees with {@link PlanNode}.
 *
 * This class is non-threadsafe. Do not reuse the stage planner for multiple query plans.
 */
public class PinotLogicalQueryPlanner {

  /**
   * planQuery achieves 2 objective:
   *   1. convert Calcite's {@link RelNode} to Pinot's {@link PlanNode} format from the {@link RelRoot} of Calcite's
   *   LogicalPlanner result.
   *   2. while walking Calcite's {@link RelNode} tree, populate {@link QueryPlanMetadata}.
   *
   * @param relRoot relational plan root.
   * @return dispatchable plan.
   */
  public QueryPlan planQuery(RelRoot relRoot) {
    RelNode relRootNode = relRoot.rel;
    // Walk through RelNode tree and construct a StageNode tree.
    PlanNode globalRoot = relNodeToStageNode(relRootNode);
    QueryPlanMetadata queryPlanMetadata =
        new QueryPlanMetadata(RelToPlanNodeConverter.getTableNamesFromRelRoot(relRootNode), relRoot.fields);
    return new QueryPlan(globalRoot, queryPlanMetadata);
  }

  /**
   * Convert the Pinot plan from {@link PinotLogicalQueryPlanner#planQuery(RelRoot)} into a {@link SubPlan}.
   *
   * @param queryPlan relational plan root.
   * @return dispatchable plan.
   */
  public SubPlan makePlan(QueryPlan queryPlan) {
    PlanNode globalRoot = queryPlan.getPlanRoot();

    // Fragment the stage tree into multiple SubPlans.
    SubPlanFragmenter.Context subPlanContext = new SubPlanFragmenter.Context();
    subPlanContext._subPlanIdToRootNodeMap.put(0, globalRoot);
    subPlanContext._subPlanIdToMetadataMap.put(0,
        new SubPlanMetadata(queryPlan.getPlanMetadata().getTableNames(), queryPlan.getPlanMetadata().getFields()));
    globalRoot.visit(SubPlanFragmenter.INSTANCE, subPlanContext);

    Map<Integer, SubPlan> subPlanMap = new HashMap<>();
    for (Map.Entry<Integer, PlanNode> subPlanEntry : subPlanContext._subPlanIdToRootNodeMap.entrySet()) {
      int subPlanId = subPlanEntry.getKey();
      PlanNode subPlanRoot = subPlanEntry.getValue();
      PlanFragmenter.Context planFragmentContext = new PlanFragmenter.Context();
      planFragmentContext._planFragmentIdToRootNodeMap.put(1,
          new PlanFragment(1, subPlanRoot, new PlanFragmentMetadata(), new ArrayList<>()));
      subPlanRoot = subPlanRoot.visit(PlanFragmenter.INSTANCE, planFragmentContext);

      // Sub plan root needs to send results back to the Broker ROOT, a.k.a. the client response node. the last stage
      // only has one
      // receiver so doesn't matter what the exchange type is. setting it to SINGLETON by default.
      PlanNode subPlanRootSenderNode =
          new MailboxSendNode(subPlanRoot.getPlanFragmentId(), subPlanRoot.getDataSchema(),
              0, RelDistribution.Type.RANDOM_DISTRIBUTED, PinotRelExchangeType.getDefaultExchangeType(), null, null,
              false);
      subPlanRootSenderNode.addInput(subPlanRoot);

      PlanNode subPlanRootReceiverNode =
          new MailboxReceiveNode(0, subPlanRoot.getDataSchema(), subPlanRoot.getPlanFragmentId(),
              RelDistribution.Type.RANDOM_DISTRIBUTED, PinotRelExchangeType.getDefaultExchangeType(), null, null,
              false, false, subPlanRootSenderNode);
      subPlanRoot = subPlanRootReceiverNode;
      PlanFragment planFragment1 = planFragmentContext._planFragmentIdToRootNodeMap.get(1);
      planFragmentContext._planFragmentIdToRootNodeMap.put(1,
          new PlanFragment(1, subPlanRootSenderNode, planFragment1.getFragmentMetadata(), planFragment1.getChildren()));
      PlanFragment rootPlanFragment
          = new PlanFragment(subPlanRoot.getPlanFragmentId(), subPlanRoot, new PlanFragmentMetadata(),
          ImmutableList.of(planFragmentContext._planFragmentIdToRootNodeMap.get(1)));
      planFragmentContext._planFragmentIdToRootNodeMap.put(0, rootPlanFragment);
      for (Map.Entry<Integer, List<Integer>> planFragmentToChildrenEntry
          : planFragmentContext._planFragmentIdToChildrenMap.entrySet()) {
        int planFragmentId = planFragmentToChildrenEntry.getKey();
        List<Integer> planFragmentChildren = planFragmentToChildrenEntry.getValue();
        for (int planFragmentChild : planFragmentChildren) {
          planFragmentContext._planFragmentIdToRootNodeMap.get(planFragmentId).getChildren()
              .add(planFragmentContext._planFragmentIdToRootNodeMap.get(planFragmentChild));
        }
      }
      SubPlan subPlan = new SubPlan(planFragmentContext._planFragmentIdToRootNodeMap.get(0),
          subPlanContext._subPlanIdToMetadataMap.get(0), new ArrayList<>());
      subPlanMap.put(subPlanId, subPlan);
    }
    for (Map.Entry<Integer, List<Integer>> subPlanToChildrenEntry : subPlanContext._subPlanIdToChildrenMap.entrySet()) {
      int subPlanId = subPlanToChildrenEntry.getKey();
      List<Integer> subPlanChildren = subPlanToChildrenEntry.getValue();
      for (int subPlanChild : subPlanChildren) {
        subPlanMap.get(subPlanId).getChildren().add(subPlanMap.get(subPlanChild));
      }
    }
    return subPlanMap.get(0);
  }

  // non-threadsafe
  // TODO: add dataSchema (extracted from RelNode schema) to the StageNode.
  private PlanNode relNodeToStageNode(RelNode node) {
    PlanNode planNode = RelToPlanNodeConverter.toStageNode(node, -1);
    List<RelNode> inputs = node.getInputs();
    for (RelNode input : inputs) {
      planNode.addInput(relNodeToStageNode(input));
    }
    return planNode;
  }
}
