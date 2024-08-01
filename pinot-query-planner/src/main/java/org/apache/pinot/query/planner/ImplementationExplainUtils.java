package org.apache.pinot.query.planner;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.query.planner.logical.TransformationTracker;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
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
   * @param fragmentExplainer a function that converts a {@link DispatchablePlanFragment} to a {@link PlanNode}.
   *                          This function may for example ask each server to explain its own plan.
   */
  public static void modifyRel(RelNode rootNode, Collection<DispatchablePlanFragment> queryStages,
      TransformationTracker<PlanNode, RelNode> tracker, Function<DispatchablePlanFragment, RelNode> fragmentExplainer) {
    // extract a key node operator
    Map<DispatchablePlanFragment, PlanNode> leafNodes = queryStages.stream()
        .filter(fragment -> !fragment.getWorkerIdToSegmentsMap().isEmpty()) // ignore root and intermediate stages
        .collect(Collectors.toMap(Function.identity(), fragment -> fragment.getPlanFragment().getFragmentRoot()));

    // creates a map where each leaf node is converted into another RelNode that may contain physical information
    Map<RelNode, RelNode> leafToRel = createSubstitutionMap(leafNodes, tracker, fragmentExplainer);

    // replace leaf operator with explain nodes
    replaceRecursive(rootNode, leafToRel);
  }

  private static Map<RelNode, RelNode> createSubstitutionMap(Map<DispatchablePlanFragment, PlanNode> leafNodes,
      TransformationTracker<PlanNode, RelNode> tracker, Function<DispatchablePlanFragment, RelNode> fragmentExplainer) {
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
      RelNode explainNode = fragmentExplainer.apply(fragment);
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
}
