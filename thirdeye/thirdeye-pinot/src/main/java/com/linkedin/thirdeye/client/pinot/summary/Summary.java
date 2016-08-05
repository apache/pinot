package com.linkedin.thirdeye.client.pinot.summary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Summary {
  static final NodeDimensionValuesComparator NODE_COMPARATOR = new NodeDimensionValuesComparator();
  private Cube cube;
  private int maxLevelCount;
  private int levelCount;
  private List<DPArray> dpArrays;

  public Summary(Cube cube) {
    this.cube = cube;
    this.maxLevelCount = cube.dimensions.size();
    this.levelCount = this.maxLevelCount;
  }

  public SummaryResponse computeSummary(int answerSize) {
    return computeSummary(answerSize, this.maxLevelCount);
  }

  public SummaryResponse computeSummary(int answerSize, int levelCount) {
    if (answerSize <= 0) answerSize = 1;
    if (levelCount <= 0 || levelCount > this.maxLevelCount) {
      levelCount = this.maxLevelCount;
    }
    this.levelCount = levelCount;

    dpArrays = new ArrayList<>(this.levelCount);
    for (int i = 0; i < this.levelCount; ++i) {
      dpArrays.add(new DPArray(answerSize));
    }
    HierarchyNode root = cube.getHierarchicalNodes().get(0).get(0);
    computeChildDPArray(root, 0.);


    // Check correctness of the sum of values
//    List<HierarchyNode> nodeList = new ArrayList<>(dpArrays.get(0).getAnswer());
//    nodeList.sort(NODE_COMPARATOR); // Process lower level nodes first
//    for (HierarchyNode node : nodeList) {
//      HierarchyNode parent = node;
//      while ((parent = parent.parent) != null) {
//        if (dpArrays.get(0).getAnswer().contains(parent)) {
//          parent.baselineValue += node.baselineValue;
//          parent.currentValue += node.currentValue;
//          break;
//        }
//      }
//    }
//    for (HierarchyNode node : nodeList) {
//      if (node.baselineValue != node.data.baselineValue || node.currentValue != node.data.currentValue) {
//        System.err.print("Wrong Wow values:");
//        System.err.println(node);
//      }
//    }

    List<HierarchyNode> answer = new ArrayList<>(dpArrays.get(0).getAnswer());
    return SummaryResponse.buildResponse(answer, levelCount);
  }

  static class NodeDimensionValuesComparator implements Comparator<HierarchyNode> {
    @Override
    public int compare(HierarchyNode n1, HierarchyNode n2) {
      return n1.data.dimensionValues.compareTo(n2.data.dimensionValues);
    }
  }

  /**
   * Build the summary recursively. The parentTargetRatio for the root node can be any arbitrary value.
   * The calculated answer for each invocation is put at dpArrays[node.level].
   * So, the final answer is located at dpArray[0].
   */
  private void computeChildDPArray(HierarchyNode node, double parentTargetRatio) {
    double targetRatio = node.aggregatedRatio();
    DPArray dpArray = dpArrays.get(node.level);
    dpArray.reset();

    // Compute DPArray if the current node is the lowest internal node.
    // Otherwise, merge DPArrays from its children.
    if (node.level == levelCount - 1) {
      for (HierarchyNode child : node.children) {
        insertRowToDPArray(dpArray, child, targetRatio);
        targetRatio = updateCurrentRatio(node, dpArray.getAnswer());
        dpArray.targetRatio = targetRatio;
      }
    } else {
      dpArray.targetRatio = targetRatio;
      for (HierarchyNode child : node.children) {
        computeChildDPArray(child, targetRatio);
        mergeDPArray(node, dpArray, dpArrays.get(node.level + 1));
        targetRatio = updateCurrentRatio(node, dpArray.getAnswer());
        dpArray.targetRatio = targetRatio;
      }
    }

    // Calculate the cost if the node (aggregated row) is put in the answer.
    // We do not need to do this for the root node.
    // Moreover, if a node is thinned out by its children, then aggregate all children.
    if (node.level != 0) {
      if ( !nodeIsThinnedOut(node) ) {
        targetRatio = (targetRatio + parentTargetRatio) / 2.;
        updateCost4NewRatio(dpArray, targetRatio);
        dpArray.targetRatio = targetRatio;
        Set<HierarchyNode> removedNode = new HashSet<>(dpArray.getAnswer());
        insertRowToDPArray(dpArray, node, targetRatio);
        removedNode.removeAll(dpArray.getAnswer());
        if (removedNode.size() != 0) {
          updateWowValuesDueToRemoval(node, dpArray.getAnswer(), removedNode);
          updateWowValues(node, dpArray.getAnswer());
        }
      } else {
        node.baselineValue = node.data.baselineValue;
        node.currentValue = node.data.currentValue;
        dpArray.reset();
        insertRowToDPArray(dpArray, node, parentTargetRatio);
        dpArray.targetRatio = parentTargetRatio;
      }
    } else {
      dpArray.getAnswer().add(node);
    }
  }

  // TODO: Need a better definition for "a node is thinned out by its children."
  // We also need to look into the case where parent node is much smaller than its children.
  private static boolean nodeIsThinnedOut(HierarchyNode node) {
    return Double.compare(0., node.baselineValue) == 0 || Double.compare(0., node.currentValue) == 0;
  }

  /**
   * Recompute the baseline value, current value, and ratio of the node. The change is induced by the chosen nodes in
   * the answer. The current node cannot be in the answer.
   */
  private static double updateCurrentRatio(HierarchyNode node, Set<HierarchyNode> answer) {
    node.baselineValue = node.data.baselineValue;
    node.currentValue = node.data.currentValue;
    for (HierarchyNode child : answer) {
      node.baselineValue -= child.baselineValue;
      node.currentValue -= child.currentValue;
    }
    double ratio = node.currentRatio();
//    if (Double.isInfinite(ratio) || Double.isNaN(ratio)) {
    if (Double.isInfinite(ratio)) {
      return node.aggregatedRatio();
    } else {
      return ratio;
    }
  }

  /**
   * Recompute the baseline value and current value the node. The change is induced by the chosen nodes in
   * the answer. Note that the current node may be in the answer.
   */
  private static void updateWowValues(HierarchyNode node, Set<HierarchyNode> answer) {
    node.baselineValue = node.data.baselineValue;
    node.currentValue = node.data.currentValue;
    for (HierarchyNode child : answer) {
      if (child == node) continue;
      node.baselineValue -= child.baselineValue;
      node.currentValue -= child.currentValue;
    }
  }

  /**
   * Merge the answers of the two given DPArrays. The merged answer is put in the DPArray at the left hand side.
   * After merging, the baseline and current values of the removed nodes (rows) will be add back to those of their
   * parent node.
   */
  private static void mergeDPArray(HierarchyNode parentNode, DPArray parentArray, DPArray childArray) {
    Set<HierarchyNode> removedNodes = new HashSet<>(parentArray.getAnswer());
    removedNodes.addAll(childArray.getAnswer());
    // Compute the merged answer
    double targetRatio = (parentArray.targetRatio + childArray.targetRatio) / 2.;
    updateCost4NewRatio(parentArray, targetRatio);
    List<HierarchyNode> childNodeList = new ArrayList<>(childArray.getAnswer());
    childNodeList.sort(NODE_COMPARATOR);
//    for (HierarchyNode childNode : childArray.getAnswer()) {
    for (HierarchyNode childNode : childNodeList) {
      insertRowToDPArrayWithAdaptiveRatio(parentArray, childNode, targetRatio);
    }
    // Update an internal node's baseline and current value if any of its child is removed due to the merge
    removedNodes.removeAll(parentArray.getAnswer());
    updateWowValuesDueToRemoval(parentNode, parentArray.getAnswer(), removedNodes);
  }

  /**
   * Update an internal node's baseline and current values if any of the nodes in its subtree is removed.
   * @param root The internal node to be updated.
   * @param answer The new answer.
   * @param removedNodes The nodes removed from the subtree of node.
   */
  private static void updateWowValuesDueToRemoval(HierarchyNode root, Set<HierarchyNode> answer, Set<HierarchyNode> removedNodes) {
    List<HierarchyNode> removedNodesList = new ArrayList<>(removedNodes);
    removedNodesList.sort(NODE_COMPARATOR); // Process lower level nodes first
    for (HierarchyNode removedNode : removedNodesList) {
      HierarchyNode parents = removedNode;
      while ((parents = parents.parent) != root) {
        if (answer.contains(parents)) {
          parents.baselineValue += removedNode.baselineValue;
          parents.currentValue += removedNode.currentValue;
          break;
        }
      }
    }
  }

  /**
   * Recompute costs of the nodes in a DPArray using targetRatio for calculating the cost.
   */
  private static void updateCost4NewRatio(DPArray dp, double targetRatio) {
    List<HierarchyNode> ans = new ArrayList<>(dp.getAnswer());
    ans.sort(NODE_COMPARATOR);
    dp.reset();
    for (HierarchyNode node : ans) {
      insertRowToDPArrayWithAdaptiveRatio(dp, node, targetRatio);
    }
  }

  /**
   * If the node's parent is also in the DPArray, then it's parent's current ratio is used as the target ratio for
   * calculating the cost of the node; otherwise, targetRatio is used.
   */
  private static void insertRowToDPArrayWithAdaptiveRatio(DPArray dp, HierarchyNode node, double targetRatio) {
    if (dp.getAnswer().contains(node.parent)) {
      insertRowToDPArray(dp, node, node.parent.currentRatio());
    } else {
      insertRowToDPArray(dp, node, targetRatio);
    }
  }

  /**
   * Insert the given node (i.e., the row of data) into the DPArray using the targetRatio for calculating its cost.
   */
  private static void insertRowToDPArray(DPArray dp, HierarchyNode node, double targetRatio) {
    double baselineValue = (node.data.baselineValue + node.baselineValue) / 2.;
    double currentValue = (node.data.currentValue + node.currentValue) / 2.;
//    double baselineValue = node.data.baselineValue;
//    double currentValue = node.data.currentValue;
//    double baselineValue = node.baselineValue;
//    double currentValue = node.currentValue;
    double cost = CostFunction.err4EmptyValues(baselineValue, currentValue, targetRatio);
//    node.data.targetRatios.add(targetRatio); // for development purpose

    for (int n = dp.size(); n > 0; --n) {
      double val1 = dp.slotAt(n - 1).cost;
      double val2 = dp.slotAt(n).cost + cost; // fixed r per iteration
      if (Double.compare(val1, val2) < 0) {
        dp.slotAt(n).cost = val1;
        dp.slotAt(n).ans.retainAll(dp.slotAt(n - 1).ans); // dp[n].ans = dp[n-1].ans
        dp.slotAt(n).ans.add(node);
      } else {
        dp.slotAt(n).cost = val2;
      }
    }
    dp.slotAt(0).cost = dp.slotAt(0).cost + cost;
  }

  public static void main (String[] argc) {
    String oFileName = "MLCube.json";
    int maxDimensionSize = 2;
    int answerSize = 10;

    Cube cube = null;
    try {
      cube = Cube.fromJson(oFileName);
      System.out.println("Restored Cube:");
      System.out.println(cube);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
    Summary summary = new Summary(cube);
    try {
      SummaryResponse response = summary.computeSummary(answerSize, maxDimensionSize);
      System.out.print("JSon String: ");
      System.out.println(new ObjectMapper().writeValueAsString(response));
      System.out.println("Object String: ");
      System.out.println(response.toString());
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }
}
