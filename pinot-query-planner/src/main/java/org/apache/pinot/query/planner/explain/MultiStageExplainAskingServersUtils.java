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
package org.apache.pinot.query.planner.explain;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.query.planner.logical.TransformationTracker;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.PlanNode;


public class MultiStageExplainAskingServersUtils {

  private MultiStageExplainAskingServersUtils() {
  }

  /**
   * Returns a node where the leaf nodes are replaced with other nodes that contain physical information.
   *
   * <strong>notice:</strong>The receiving node may be modified.
   *
   * Some of the new nodes may be {@link org.apache.pinot.core.plan.PinotExplainedRelNode}, so once this method is
   * called the received RelNode should not be used for execution.
   *
   * @param rootNode the root {@link RelNode} of the query plan, <strong>which may be modified</strong>.
   * @param queryStages a collection of {@link DispatchablePlanFragment}s that represent the stages of the query.
   * @param tracker a {@link TransformationTracker} that keeps track of the creator of each {@link PlanNode}.
   *                This is used to find the RelNodes that need to be substituted.
   * @param serversExplainer an {@link AskingServerStageExplainer} that is used to ask the servers for their plans.
   */
  public static RelNode modifyRel(RelNode rootNode, Collection<DispatchablePlanFragment> queryStages,
      TransformationTracker<PlanNode, RelNode> tracker, AskingServerStageExplainer serversExplainer) {
    // extract a key node operator
    Map<DispatchablePlanFragment, PlanNode> leafNodes = queryStages.stream()
        .filter(fragment -> fragment.getTableName() != null) // ignore root and intermediate stages
        .collect(Collectors.toMap(Function.identity(), fragment -> fragment.getPlanFragment().getFragmentRoot()));

    // creates a map where each leaf node is converted into another RelNode that may contain physical information
    Map<RelNode, RelNode> leafToRel = createSubstitutionMap(leafNodes, tracker, serversExplainer);

    // replace leaf operator with explain nodes
    return replace(rootNode, leafToRel);
  }

  private static Map<RelNode, RelNode> createSubstitutionMap(Map<DispatchablePlanFragment, PlanNode> leafNodes,
      TransformationTracker<PlanNode, RelNode> tracker,
      AskingServerStageExplainer serversExplainer) {
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
      RelNode explainNode = serversExplainer.explainFragment(fragment);
      explainNodes.put(stageRootNode, explainNode);
    }
    return explainNodes;
  }

  private static RelNode replace(RelNode node, Map<RelNode, RelNode> substitutionMap) {
    RelNode newInput = substitutionMap.get(node);
    if (newInput != null) {
      return newInput;
    } else {
      replaceRecursive(node, substitutionMap);
      return node;
    }
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
}
