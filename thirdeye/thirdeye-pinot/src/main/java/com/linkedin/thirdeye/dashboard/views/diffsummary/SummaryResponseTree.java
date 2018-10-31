package com.linkedin.thirdeye.dashboard.views.diffsummary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.client.diffsummary.costfunctions.CostFunction;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.HierarchyNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SummaryResponseTree {
  private static final Logger LOG = LoggerFactory.getLogger(SummaryResponseTree.class);
  @JsonProperty("dimensions")
  List<String> dimensions = new ArrayList<>();

  List<HierarchyNode> hierarchicalNodes = new ArrayList<>();


  public static List<HierarchyNode> sortResponseTree(List<HierarchyNode> nodes, int levelCount, CostFunction costFunction) {
    SummaryResponseTree responseTree = new SummaryResponseTree();

    // Build the header
    Dimensions dimensions = nodes.get(0).getDimensions();
    double globalBaselineValue = nodes.get(0).getOriginalCurrentValue();
    double globalCurrentValue = nodes.get(0).getOriginalBaselineValue();
    for (HierarchyNode node : nodes) {
      if (node.getLevel() == 0) {
        globalBaselineValue = node.getOriginalBaselineValue();
        globalCurrentValue = node.getOriginalCurrentValue();
        break;
      }
    }
    for (int i = 0; i < levelCount; ++i) {
      responseTree.dimensions.add(dimensions.get(i));
    }

    List<SummaryResponseTreeNode> treeNodes = new ArrayList<>();
    // Build the response tree
    Collections.sort(nodes, Collections.reverseOrder(Summary.NODE_COMPARATOR)); // pre-order traversal
    for (HierarchyNode node : nodes) {
      SummaryResponseTreeNode treeNode = new SummaryResponseTreeNode();
      treeNode.hierarchyNode = node;
      treeNode.level = node.getLevel();
      treeNodes.add(treeNode);
    }

    //    Connecting child and parent response tree node. Note: response tree is not a perfect tree like the tree
    //    of HierarchyNodes, because in response tree a node's direct parent may be missing.
    //    In that case, we have to bootstrap the search until a higher level parent, which also exists in the response
    //    tree, is found.
    //    Pre-condition: treeNodes are sorted in the pre-order fashion when projecting the nodes back to the tree of
    //                   HierarchyNode.
    SummaryResponseTreeNode preTreeNode = null;
    for (SummaryResponseTreeNode treeNode : treeNodes) {
      if (preTreeNode != null) {
        SummaryResponseTreeNode parent = preTreeNode.getCommonParent(treeNode.getDimensionValues());
        treeNode.parent = parent;
        parent.children.add(treeNode);
      }
      preTreeNode = treeNode;
    }

    // Sort the children of each node by their cost
    sortChildNodes(treeNodes.get(0), globalBaselineValue, globalCurrentValue, costFunction);

    // Put the nodes to a flattened array
    insertChildNodes(treeNodes.get(0), responseTree.hierarchicalNodes);

    return responseTree.hierarchicalNodes;
  }

  private static void insertChildNodes(SummaryResponseTreeNode node, List<HierarchyNode> hierarchicalNodes) {
    if (node.hierarchyNode != null) {
      hierarchicalNodes.add(node.hierarchyNode);
    }
    for (SummaryResponseTreeNode child : node.children) {
      insertChildNodes(child, hierarchicalNodes);
    }
  }

  /**
   * A recursive function to sort response tree.
   */
  private static void sortChildNodes(SummaryResponseTreeNode node, double globalBaselineValue,
      double globalCurrentValue, CostFunction costFunction) {
    if (node.children.size() == 0) return;
    for (SummaryResponseTreeNode child : node.children) {
      sortChildNodes(child, globalBaselineValue, globalCurrentValue, costFunction);
    }
    double ratio = node.currentRatio();
    for (SummaryResponseTreeNode child : node.children) {
      computeCost(child, ratio, globalBaselineValue, globalCurrentValue, costFunction);
    }
    Collections.sort(node.children, Collections.reverseOrder(new SummaryResponseTreeNodeCostComparator()));
  }

  private static void computeCost(SummaryResponseTreeNode node, double targetRatio, double globalBaselineValue,
      double globalCurrentValue, CostFunction costFunction) {
    if (node.hierarchyNode != null) {
      double nodeCost = costFunction
          .computeCost(node.getBaselineValue(), node.getCurrentValue(), targetRatio, globalBaselineValue,
              globalCurrentValue);
      node.hierarchyNode.setCost(nodeCost);
      node.subTreeCost = nodeCost;
    }
    for (SummaryResponseTreeNode child : node.children) {
      computeCost(child, targetRatio, globalBaselineValue, globalCurrentValue, costFunction);
      node.subTreeCost += child.subTreeCost;
    }
  }

  public static class SummaryResponseTreeNodeCostComparator implements Comparator<SummaryResponseTreeNode> {
    @Override
    public int compare(SummaryResponseTreeNode o1, SummaryResponseTreeNode o2) {
      return Double.compare(o1.subTreeCost, o2.subTreeCost);
    }
  }

  public static class SummaryResponseTreeNode {
    HierarchyNode hierarchyNode; // If it is null, this node is a dummy node.
    double subTreeCost;
    int level;

    @JsonIgnore
    SummaryResponseTreeNode parent;

    List<SummaryResponseTreeNode> children = new ArrayList<>();

    public List<String> getDimensionValues() {
      return hierarchyNode.getDimensionValues().values();
    }

    public void setLevel(int level) {
      this.level = level;
    }

    public int getLevel() {
      return level;
    }

    public double getBaselineValue() {
      return hierarchyNode.getBaselineValue();
    }

    public double getCurrentValue() {
      return hierarchyNode.getCurrentValue();
    }

    public double currentRatio() {
      if (hierarchyNode != null) {
        return hierarchyNode.currentRatio();
      } else {
        SummaryResponseTreeNode parent = this;
        do {
          if (parent.hierarchyNode == null) {
            parent = parent.parent;
          } else {
            break;
          }
        } while (true);
        return parent.currentRatio();
      }
    }

    @JsonIgnore
    /**
     * Return the common parent node according to the given dimension values. There always exists one common
     * parent: the root of the tree.
     */
    public SummaryResponseTreeNode getCommonParent(List<String> otherDimensionValues) {
      // Calculate the level of the common parent
      int targetLevel = 0;
      Iterator<String> otherIte = otherDimensionValues.iterator();
      for (targetLevel = 0; targetLevel < getLevel(); ++targetLevel) {
        if (otherIte.hasNext()) {
          String otherDimensionValue = otherIte.next();
          if ( !getDimensionValues().get(targetLevel).equals(otherDimensionValue) ) {
            break;
          } // else continue
        } else {
          break;
        }
      }

      // Return the common parent
      SummaryResponseTreeNode node = this;
      SummaryResponseTreeNode preNode = this;
      while (true) {
        if (node.getLevel() == targetLevel) {
          return node;
        } if (node.getLevel() < targetLevel) {
          // current node becomes the missing common parent and move itself to a new node
          SummaryResponseTreeNode newChildNode = new SummaryResponseTreeNode();
          swapContent(preNode, newChildNode);
          newChildNode.parent = preNode;
          preNode.children.add(newChildNode);
          preNode.level = targetLevel;
          return preNode;
        } else {
          preNode = node;
          node = node.parent;
        }
      }
    }

    private void swapContent(SummaryResponseTreeNode A, SummaryResponseTreeNode B) {
      HierarchyNode tmpNode = A.hierarchyNode;
      A.hierarchyNode = B.hierarchyNode;
      B.hierarchyNode = tmpNode;
      List<SummaryResponseTreeNode> tmpChildren = A.children;
      A.children = B.children;
      B.children = tmpChildren;
      int tmpLevel = A.level;
      A.level = B.level;
      B.level = tmpLevel;
    }
  }
}
