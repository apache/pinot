package com.linkedin.thirdeye.client.pinot.summary;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Summary {
  private static int N = 12;

  public static void computeSummary(List<List<HierarchyNode>> nodes) {
    HierarchyNode root = nodes.get(0).get(0);
    DPArray dpArray = buildSummary(root, .0);

    // Print summary
    List<HierarchyNode> answer = new ArrayList<>();
    for (HierarchyNode node : dpArray.getAnswer()) {
      answer.add(node);
    }
    answer.sort(new NodeDimensionValuesComparator().reversed());
    double baselineValues = root.data.baselineValue;
    double currentValues = root.data.currentValue;
    for (HierarchyNode node : answer) {
      baselineValues -= node.baselineValue;
      currentValues -= node.currentValue;
    }
    System.out.println("ALL: " + baselineValues + " " + currentValues + " " + (currentValues / baselineValues));
    for (HierarchyNode node : answer) {
      System.out.print(node.data + ": ");
      System.out.print(node.baselineValue + ", " + node.currentValue + ", ");
      System.out.println(node.currentValue / node.baselineValue);
    }
  }

  static class NodeDimensionValuesComparator implements Comparator<HierarchyNode> {
    @Override
    public int compare(HierarchyNode n1, HierarchyNode n2) {
      return n1.data.dimensionValues.compareTo(n2.data.dimensionValues);
    }
  }

  /**
   * Build the summary recursively.
   * The parentTargetRatio for the root node can be any arbitrary value.
   */
  static DPArray buildSummary(HierarchyNode node, double parentTargetRatio) {
    DPArray dpArray = new DPArray(N);
    double targetRatio = node.data.currentValue / node.data.baselineValue;
    dpArray.targetRatio = targetRatio;
    if (!node.isWorker) {
      for (HierarchyNode child : node.children) {
        DPArray childDpArray = buildSummary(child, targetRatio);
        dpArray = mergeSummary(node, dpArray, childDpArray);
        targetRatio = updateTargetRatio(node, dpArray.getAnswer());
      }
    } else {
      dpArray = buildSummary(node, dpArray);
    }

    if (!node.isRoot) {
      // if all children of the current node are picked, then aggregate them all
      if (Double.compare(0., node.baselineValue) == 0 && Double.compare(0., node.currentValue) == 0) {
        node.baselineValue = node.data.baselineValue;
        node.currentValue = node.data.currentValue;
        dpArray = new DPArray(N);
        insertRowToSummary(dpArray, node, parentTargetRatio);
      } else {
        insertRowToSummary(dpArray, node, parentTargetRatio);
        updateValues(node, dpArray.getAnswer());
      }
    }

    return dpArray;
  }

  static void updateValues(HierarchyNode node, Set<HierarchyNode> answer) {
    node.baselineValue = node.data.baselineValue;
    node.currentValue = node.data.currentValue;
    for (HierarchyNode child : answer) {
      if (child == node) continue;
      node.baselineValue -= child.baselineValue;
      node.currentValue -= child.currentValue;
    }
  }

  static double updateTargetRatio(HierarchyNode node, Set<HierarchyNode> answer) {
    node.baselineValue = node.data.baselineValue;
    node.currentValue = node.data.currentValue;
    for (HierarchyNode child : answer) {
      node.baselineValue -= child.baselineValue;
      node.currentValue -= child.currentValue;
    }
    double ratio = node.currentValue / node.baselineValue;
    if (Double.isInfinite(ratio)) {
      ratio = node.data.currentValue / node.data.baselineValue;
    }
    return ratio;
  }

  static DPArray buildSummary(HierarchyNode node, DPArray nodeDpArray) {
    double targetRatio = nodeDpArray.targetRatio;
    for (HierarchyNode child : node.children) {
      insertRowToSummary(nodeDpArray, child, targetRatio);
      targetRatio = updateTargetRatio(node, nodeDpArray.getAnswer());
      nodeDpArray.targetRatio = targetRatio;
    }
    return nodeDpArray;
  }

  static DPArray mergeSummary(HierarchyNode parentNode, DPArray parentArray, DPArray childArray) {
    double targetRatio = (parentArray.targetRatio + childArray.targetRatio) / 2.;
    Set<HierarchyNode> removedNodes = new HashSet<>(parentArray.getAnswer());
    removedNodes.addAll(childArray.getAnswer());
    for (HierarchyNode childNode : childArray.getAnswer()) {
      insertRowToSummary(parentArray, childNode, targetRatio);
    }
    // Update an internal node's baseline and current value if any of its child is remove due to the merge
    removedNodes.removeAll(parentArray.getAnswer());
    List<HierarchyNode> removedNodesList = new ArrayList<>();
    removedNodesList.addAll(removedNodes);
    removedNodesList.sort(new NodeDimensionValuesComparator()); // Process lower level nodes first
    for (HierarchyNode node : removedNodesList) {
      HierarchyNode parents = node;
      while ((parents = parents.parent) != parentNode) {
        parents.baselineValue += node.baselineValue;
        parents.currentValue += node.currentValue;
        if (parentArray.getAnswer().contains(parents)) {
          break;
        }
      }
    }
    parentArray.targetRatio = updateTargetRatio(parentNode, parentArray.getAnswer());
    return parentArray;
  }

  static void insertRowToSummary(DPArray dp, HierarchyNode node, double targetRatio) {
    double baselineValue = node.baselineValue;
    double currentValue = node.currentValue;
    double cost = CostFunction.err4EmptyValues(baselineValue, currentValue, targetRatio);
    node.data.targetRatios.add(targetRatio);

    for (int n = N; n > 0; --n) {
      double val1 = dp.get(n - 1).cost;
      double val2 = dp.get(n).cost + cost; // fixed r per iteration
      if (Double.compare(val1, val2) < 0) {
        dp.get(n).cost = val1;
        dp.get(n).ans.retainAll(dp.get(n - 1).ans); // dp[n].ans = dp[n-1].ans
        dp.get(n).ans.add(node);
      } else {
        dp.get(n).cost = val2;
      }
    }
    dp.get(0).cost = dp.get(0).cost + cost;
  }
}
