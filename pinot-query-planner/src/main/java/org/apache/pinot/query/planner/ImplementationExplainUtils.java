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
package org.apache.pinot.query.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.PlanNodeToRelConverter;
import org.apache.pinot.query.planner.logical.TransformationTracker;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


public class ImplementationExplainUtils {

  private ImplementationExplainUtils() {
  }

  /**
   * Modifies the given {@link RelNode} by replacing the leaf nodes with other RelNodes that contain physical
   * information (like indexes used, etc).
   *
   * Some of the new nodes may be {@link org.apache.pinot.core.plan.PinotExplainedRelNode}, so once this method is
   * called the received RelNode should not be used for execution.
   *
   * @param rootNode the root {@link RelNode} of the query plan, which may be modified.
   * @param queryStages a collection of {@link DispatchablePlanFragment}s that represent the stages of the query.
   * @param tracker a {@link TransformationTracker} that keeps track of the creator of each {@link PlanNode}.
   *                This is used to find the RelNodes that need to be substituted.
   * @param fragmentToPlanNodes a function that converts a {@link DispatchablePlanFragment} to a collection of
   *                          {@link PlanNode PlanNodes}.
   *                          This function may for example ask each server to explain its own plan.
   */
  public static void modifyRel(RelNode rootNode, Collection<DispatchablePlanFragment> queryStages,
      TransformationTracker<PlanNode, RelNode> tracker,
      Function<DispatchablePlanFragment, Collection<PlanNode>> fragmentToPlanNodes,
      RelBuilder relBuilder) {
    // extract a key node operator
    Map<DispatchablePlanFragment, PlanNode> leafNodes = queryStages.stream()
        .filter(fragment -> !fragment.getWorkerIdToSegmentsMap().isEmpty()) // ignore root and intermediate stages
        .collect(Collectors.toMap(Function.identity(), fragment -> fragment.getPlanFragment().getFragmentRoot()));

    // creates a map where each leaf node is converted into another RelNode that may contain physical information
    Map<RelNode, RelNode> leafToRel = createSubstitutionMap(leafNodes, tracker, fragmentToPlanNodes, relBuilder);

    // replace leaf operator with explain nodes
    replaceRecursive(rootNode, leafToRel);
  }

  private static Map<RelNode, RelNode> createSubstitutionMap(Map<DispatchablePlanFragment, PlanNode> leafNodes,
      TransformationTracker<PlanNode, RelNode> tracker,
      Function<DispatchablePlanFragment, Collection<PlanNode>> fragmentToPlanNodes, RelBuilder relBuilder) {
    Map<RelNode, RelNode> explainNodes = new HashMap<>(leafNodes.size());

    for (Map.Entry<DispatchablePlanFragment, PlanNode> entry : leafNodes.entrySet()) {
      DispatchablePlanFragment fragment = entry.getKey();
      PlanNode leafNode = entry.getValue();
      RelNode stageRootNode = tracker.getCreatorOf(leafNode);
      if (stageRootNode == null) {
        throw new IllegalStateException("Cannot find the corresponding RelNode for PlanNode: " + leafNode);
      }
      if (explainNodes.containsKey(stageRootNode)) {
        throw new IllegalStateException("Duplicate RelNode found in the leaf nodes: " + stageRootNode);
      }
      RelNode explainNode = explainFragment(fragmentToPlanNodes, fragment, relBuilder);
      explainNodes.put(stageRootNode, explainNode);
    }
    return explainNodes;
  }

  private static void replaceRecursive(RelNode node, Map<RelNode, RelNode> substitutionMap) {
    for (int i = 0; i < node.getInputs().size(); i++) {
      RelNode input = node.getInput(i);
      RelNode newInput = substitutionMap.get(input);
      if (newInput != null) {
        node.replaceInput(i, newInput);
      } else {
        replaceRecursive(input, substitutionMap);
      }
    }
  }

  private static RelNode explainFragment(Function<DispatchablePlanFragment, Collection<PlanNode>> fragmentToPlanNode,
      DispatchablePlanFragment fragment, RelBuilder relBuilder) {
    relBuilder.clear();
    Collection<PlanNode> planNodes = fragmentToPlanNode.apply(fragment);

    HashMap<PlanNode, Integer> planNodesMap = new HashMap<>();
    planNodes.forEach(planNode -> mergePlans(planNodesMap, planNode));

    PlanNode mergedNode;
    PlanNode fragmentRoot = fragment.getPlanFragment().getFragmentRoot();
    int stageId = fragmentRoot.getStageId();
    DataSchema schema = fragmentRoot.getDataSchema();
    switch (planNodesMap.size()) {
      case 0: {
        mergedNode =
            new ExplainedNode(stageId, schema, null, Collections.emptyList(), "Empty", Collections.emptyMap());
        break;
      }
      case 1: {
        mergedNode = planNodesMap.keySet().iterator().next();
        break;
      }
      default: {
        List<PlanNode> inputs = new ArrayList<>(planNodesMap.size());

        for (Map.Entry<PlanNode, Integer> entry : planNodesMap.entrySet()) {
          Map<String, String> attributes =
              Collections.singletonMap("servers", Integer.toString(entry.getValue()));

          inputs.add(new ExplainedNode(stageId, entry.getKey().getDataSchema(), null,
              Collections.singletonList(entry.getKey()), "ALTERNATIVE", attributes));
        }

        mergedNode = new ExplainedNode(stageId, schema, null, inputs, "INTERMEDIATE_COMBINE",
            Collections.emptyMap());
        break;
      }
    }

    return PlanNodeToRelConverter.convert(relBuilder, mergedNode);
  }

  private static void mergePlans(Map<PlanNode, Integer> planNodesMap, PlanNode planNode) {
    // TODO: Actually merge nodes
    planNodesMap.put(planNode, planNodesMap.getOrDefault(planNode, 0) + 1);
  }
}
