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

  private final RowInserter basicRowInserter = new BasicRowInserter();
  private RowInserter oneSideErrorRowInserter = basicRowInserter;
  private RowInserter leafRowInserter = basicRowInserter;

  public Summary(Cube cube) {
    this.cube = cube;
    this.maxLevelCount = cube.dimensions.size();
    this.levelCount = this.maxLevelCount;
  }

  public SummaryResponse computeSummary(int answerSize) {
    return computeSummary(answerSize, false, this.maxLevelCount);
  }

  public SummaryResponse computeSummary(int answerSize, boolean doOneSideError) {
    return computeSummary(answerSize, doOneSideError, this.maxLevelCount);
  }

  public SummaryResponse computeSummary(int answerSize, int levelCount) {
    return computeSummary(answerSize, false, this.maxLevelCount);
  }

  public SummaryResponse computeSummary(int answerSize, boolean doOneSideError, int levelCount) {
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
    if (doOneSideError) {
      oneSideErrorRowInserter =
          new OneSideErrorRowInserter(basicRowInserter, Double.compare(1., root.targetRatio()) <= 0);
      // If this cube contains only one dimension, one side error is calculated starting at leaf (detailed) level;
      // otherwise, a row at different side is removed through internal nodes.
      if (levelCount == 1) leafRowInserter = oneSideErrorRowInserter;
    }
    computeChildDPArray(root);
    List<HierarchyNode> answer = new ArrayList<>(dpArrays.get(0).getAnswer());
    SummaryResponse response = SummaryResponse.buildResponse(answer, levelCount);

    return response;
  }

  /**
   * Check correctness of the sum of wow values. The check changes the wow values, so it should only be invoked after
   * SummaryResponse is generated.
   */
  public void testCorrectnessOfWowValues() {
    List<HierarchyNode> nodeList = new ArrayList<>(dpArrays.get(0).getAnswer());
    nodeList.sort(NODE_COMPARATOR); // Process lower level nodes first
    for (HierarchyNode node : nodeList) {
      HierarchyNode parent = node;
      while ((parent = parent.parent) != null) {
        if (dpArrays.get(0).getAnswer().contains(parent)) {
          parent.baselineValue += node.baselineValue;
          parent.currentValue += node.currentValue;
          break;
        }
      }
    }
    for (HierarchyNode node : nodeList) {
      if (node.baselineValue != node.data.baselineValue || node.currentValue != node.data.currentValue) {
        System.err.println("Wrong Wow values at node: " + node.data.dimensionValues + ". Expected: "
            + node.data.baselineValue + "," + node.data.currentValue + ", actual: " + node.baselineValue + ","
            + node.currentValue);
      }
    }
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
  private void computeChildDPArray(HierarchyNode node) {
    HierarchyNode parent = node.parent;
    DPArray dpArray = dpArrays.get(node.level);
    dpArray.fullReset();
    dpArray.targetRatio = node.targetRatio();

    // Compute DPArray if the current node is the lowest internal node.
    // Otherwise, merge DPArrays from its children.
    if (node.level == levelCount - 1) {
      if (node.children.size() < dpArray.size()) {
        dpArray.setShrinkSize(Math.max(1, (node.children.size()+1)/2));
      }
      for (HierarchyNode child : node.children) {
        leafRowInserter.insertRowToDPArray(dpArray, child, node.targetRatio());
        updateWowValues(node, dpArray.getAnswer());
        dpArray.targetRatio = node.targetRatio(); // get updated ratio
      }
    } else {
      List<HierarchyNode> removedNodes = new ArrayList<>();
      boolean doRollback = false;
      do {
        doRollback = false;
        for (HierarchyNode child : node.children) {
          computeChildDPArray(child);
          removedNodes.addAll(mergeDPArray(node, dpArray, dpArrays.get(node.level + 1)));
          updateWowValues(node, dpArray.getAnswer());
          dpArray.targetRatio = node.targetRatio(); // get updated ratio
        }
        // Aggregate current node's answer if it is thinned out due to the user's answer size is too huge.
        // If the current node is kept being thinned out, it eventually aggregates all its children.
        if ( nodeIsThinnedOut(node) && dpArray.getAnswer().size() < dpArray.maxSize()) {
          doRollback = true;
          rollbackInsertions(node, dpArray.getAnswer(), removedNodes);
          removedNodes.clear();
          dpArray.setShrinkSize(Math.max(1, (dpArray.getAnswer().size()*2)/3));
          dpArray.reset();
          dpArray.targetRatio = node.targetRatio();
        }
      } while (doRollback);
    }

    // Calculate the cost if the node (aggregated row) is put in the answer.
    // We do not need to do this for the root node.
    // Moreover, if a node is thinned out by its children, it won't be inserted to the answer.
    if (node.level != 0) {
      updateWowValues(parent, dpArray.getAnswer());
      double targetRatio = parent.targetRatio();
      aggregateSmallNodes(node, dpArray, targetRatio);
      dpArray.targetRatio = targetRatio;
      if ( !nodeIsThinnedOut(node) ) {
        Set<HierarchyNode> removedNode = new HashSet<>(dpArray.getAnswer());
        basicRowInserter.insertRowToDPArray(dpArray, node, targetRatio);
        removedNode.removeAll(dpArray.getAnswer());
        if (removedNode.size() != 0) {
          updateWowValuesDueToRemoval(node, dpArray.getAnswer(), removedNode);
          updateWowValues(node, dpArray.getAnswer());
        }
      }
    } else {
      dpArray.getAnswer().add(node);
    }
  }

  // TODO: Need a better definition for "a node is thinned out by its children."
  // We also need to look into the case where parent node is much smaller than its children.
  private static boolean nodeIsThinnedOut(HierarchyNode node) {
    return Double.compare(0., node.baselineValue) == 0 && Double.compare(0., node.currentValue) == 0;
  }

  private static void rollbackInsertions(HierarchyNode node, Set<HierarchyNode> answer, List<HierarchyNode> removedNodes) {
    removedNodes.sort(NODE_COMPARATOR.reversed());
    for (HierarchyNode removedNode : removedNodes) {
      HierarchyNode parents = removedNode;
      while ((parents = parents.parent) != node) {
        if (answer.contains(parents) || removedNodes.contains(parents)) {
          parents.baselineValue -= removedNode.baselineValue;
          parents.currentValue -= removedNode.currentValue;
          break;
        }
      }
    }
    node.baselineValue = node.data.baselineValue;
    node.currentValue = node.data.currentValue;
  }

  /**
   * Merge the answers of the two given DPArrays. The merged answer is put in the DPArray at the left hand side.
   * After merging, the baseline and current values of the removed nodes (rows) will be add back to those of their
   * parent node.
   */
  private Set<HierarchyNode> mergeDPArray(HierarchyNode parentNode, DPArray parentArray, DPArray childArray) {
    Set<HierarchyNode> removedNodes = new HashSet<>(parentArray.getAnswer());
    removedNodes.addAll(childArray.getAnswer());
    // Compute the merged answer
    double targetRatio = (parentArray.targetRatio + childArray.targetRatio) / 2.;
    aggregateSmallNodes(parentNode, parentArray, targetRatio);
    List<HierarchyNode> childNodeList = new ArrayList<>(childArray.getAnswer());
    childNodeList.sort(NODE_COMPARATOR);
    for (HierarchyNode childNode : childNodeList) {
      insertRowWithAdaptiveRatio(parentArray, childNode, targetRatio);
    }
    // Update an internal node's baseline and current value if any of its child is removed due to the merge
    removedNodes.removeAll(parentArray.getAnswer());
    updateWowValuesDueToRemoval(parentNode, parentArray.getAnswer(), removedNodes);
    return removedNodes;
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
   * Update an internal node's baseline and current values if any of the nodes in its subtree is removed.
   * @param root The internal node to be updated.
   * @param answer The new answer.
   * @param removedNodes The nodes removed from the subtree of node.
   */
  private static void updateWowValuesDueToRemoval(HierarchyNode root, Set<HierarchyNode> answer,
      Set<HierarchyNode> removedNodes) {
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
  private void aggregateSmallNodes(HierarchyNode parentNode, DPArray dp, double targetRatio) {
    Set<HierarchyNode> removedNodes = new HashSet<>(dp.getAnswer());
    List<HierarchyNode> ans = new ArrayList<>(dp.getAnswer());
    ans.sort(NODE_COMPARATOR);
    dp.reset();
    for (HierarchyNode node : ans) {
      insertRowWithAdaptiveRatioNoOneSideError(dp, node, targetRatio);
    }
    removedNodes.removeAll(dp.getAnswer());
    if (removedNodes.size() != 0) {
      // Temporarily add parentNode to the answer so the values of the removed small node can successfully add back to
      // parentNode by re-using the method updateWowValuesDueToRemoval.
      dp.getAnswer().add(parentNode);
      updateWowValuesDueToRemoval(parentNode.parent, dp.getAnswer(), removedNodes);
      dp.getAnswer().remove(parentNode);
    }
  }

  /**
   * If the node's parent is also in the DPArray, then it's parent's current ratio is used as the target ratio for
   * calculating the cost of the node; otherwise, targetRatio is used.
   */
  private void insertRowWithAdaptiveRatioNoOneSideError(DPArray dp, HierarchyNode node, double targetRatio) {
    if (dp.getAnswer().contains(node.parent)) {
      // For one side error if node's parent is included in the solution, then its cost will be calculated normally.
      basicRowInserter.insertRowToDPArray(dp, node, node.parent.targetRatio());
    } else {
      basicRowInserter.insertRowToDPArray(dp, node, targetRatio);
    }
  }

  /**
   * If the node's parent is also in the DPArray, then it's parent's current ratio is used as the target ratio for
   * calculating the cost of the node; otherwise, targetRatio is used.
   */
  private void insertRowWithAdaptiveRatio(DPArray dp, HierarchyNode node, double targetRatio) {
    if (dp.getAnswer().contains(node.parent)) {
      // For one side error if node's parent is included in the solution, then its cost will be calculated normally.
      basicRowInserter.insertRowToDPArray(dp, node, node.parent.targetRatio());
    } else {
      oneSideErrorRowInserter.insertRowToDPArray(dp, node, targetRatio);
    }
  }

  private static interface RowInserter {
    public void insertRowToDPArray(DPArray dp, HierarchyNode node, double targetRatio);
  }

  private static class BasicRowInserter implements RowInserter {
    @Override
    public void insertRowToDPArray(DPArray dp, HierarchyNode node, double targetRatio) {
      double baselineValue = node.baselineValue;
      double currentValue = node.currentValue;
      double cost = CostFunction.err4EmptyValues(baselineValue, currentValue, targetRatio);

      for (int n = dp.size() - 1; n > 0; --n) {
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
  }

  /**
   * A wrapper class over BasicRowInserter. This class provide the calculation for one side error summary.
   */
  private static class OneSideErrorRowInserter implements RowInserter {
    final RowInserter basicRowInserter;
    final boolean side;

    public OneSideErrorRowInserter(RowInserter basicRowInserter, boolean side) {
      this.basicRowInserter = basicRowInserter;
      this.side = side;
    }

    @Override
    public void insertRowToDPArray(DPArray dp, HierarchyNode node, double targetRatio)  {
      // if the row has the same change trend with the top row, then insert the row
      if ( side == node.side() ) {
        basicRowInserter.insertRowToDPArray(dp, node, targetRatio);
      } else { // otherwise, it is inserted only there exists an intermediate parent besides root node
        HierarchyNode parent = node;
        while ((parent = parent.parent) != null) {
          if (dp.getAnswer().contains(parent)) {
            if ( side == parent.side() ) {
              basicRowInserter.insertRowToDPArray(dp, node, targetRatio);
              break;
            }
          }
        }
      }
    }
  }

  public static void main (String[] argc) {
    String oFileName = "Cube.json";
    int answerSize = 20;
    boolean doOneSideError = false;
    int maxDimensionSize = 4;

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
      SummaryResponse response = summary.computeSummary(answerSize, doOneSideError, maxDimensionSize);
      System.out.print("JSon String: ");
      System.out.println(new ObjectMapper().writeValueAsString(response));
      System.out.println("Object String: ");
      System.out.println(response.toString());
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    summary.testCorrectnessOfWowValues();
  }
}
