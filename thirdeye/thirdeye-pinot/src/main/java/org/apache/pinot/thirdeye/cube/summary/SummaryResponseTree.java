/*
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

package org.apache.pinot.thirdeye.cube.summary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.thirdeye.cube.cost.CostFunction;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.node.CubeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SummaryResponseTree {
  private static final Logger LOG = LoggerFactory.getLogger(SummaryResponseTree.class);

  @JsonProperty("dimensions")
  private List<String> dimensions = new ArrayList<>();
  private List<CubeNode> hierarchicalNodes = new ArrayList<>();


  public static List<CubeNode> sortResponseTree(List<CubeNode> nodes, int levelCount, CostFunction costFunction) {
    SummaryResponseTree responseTree = new SummaryResponseTree();

    // Build the header
    Dimensions dimensions = nodes.get(0).getDimensions();
    double topBaselineValue = nodes.get(0).getOriginalBaselineValue();
    double topCurrentValue = nodes.get(0).getOriginalCurrentValue();
    double topBaselineSize = nodes.get(0).getOriginalBaselineSize();
    double topCurrentSize = nodes.get(0).getOriginalCurrentSize();
    for (CubeNode node : nodes) {
      if (node.getLevel() == 0) {
        topBaselineValue = node.getOriginalBaselineValue();
        topCurrentValue = node.getOriginalCurrentValue();
        topBaselineSize = node.getOriginalBaselineSize();
        topCurrentSize = node.getOriginalCurrentSize();
        break;
      }
    }
    for (int i = 0; i < levelCount; ++i) {
      responseTree.dimensions.add(dimensions.get(i));
    }

    List<SummaryResponseTreeNode> treeNodes = new ArrayList<>();
    // Build the response tree
    nodes.sort(Collections.reverseOrder(Summary.NODE_COMPARATOR)); // pre-order traversal
    for (CubeNode node : nodes) {
      SummaryResponseTreeNode treeNode = new SummaryResponseTreeNode();
      treeNode.cubeNode = node;
      treeNode.level = node.getLevel();
      treeNodes.add(treeNode);
    }

    //    Connecting child and parent response tree node. Note: response tree is not a perfect tree like the tree
    //    of HierarchyNodes, because in response tree a node's direct parent may be missing.
    //    In that case, we have to bootstrap the search until a higher level parent, which also exists in the response
    //    tree, is found.
    //    Pre-condition: treeNodes are sorted in the pre-order fashion when projecting the nodes back to the tree of
    //                   CubeNode.
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
    sortChildNodes(treeNodes.get(0), topBaselineValue, topCurrentValue, topBaselineSize, topCurrentSize, costFunction);

    // Put the nodes to a flattened array
    insertChildNodes(treeNodes.get(0), responseTree.hierarchicalNodes);

    return responseTree.hierarchicalNodes;
  }

  private static void insertChildNodes(SummaryResponseTreeNode node, List<CubeNode> hierarchicalNodes) {
    if (node.cubeNode != null) {
      hierarchicalNodes.add(node.cubeNode);
    }
    for (SummaryResponseTreeNode child : node.children) {
      insertChildNodes(child, hierarchicalNodes);
    }
  }

  /**
   * A recursive function to sort response tree.
   */
  private static void sortChildNodes(SummaryResponseTreeNode node, double topBaselineValue,
      double topCurrentValue, double topBaselineSize, double topCurrentSize, CostFunction costFunction) {
    if (node.children.size() == 0) return;
    for (SummaryResponseTreeNode child : node.children) {
      sortChildNodes(child, topBaselineValue, topCurrentValue, topBaselineSize, topCurrentSize, costFunction);
    }
    double ratio = node.currentChangeRatio();
    for (SummaryResponseTreeNode child : node.children) {
      computeCost(child, ratio, topBaselineValue, topCurrentValue, topBaselineSize, topCurrentSize, costFunction);
    }
    node.children.sort(Collections.reverseOrder(new SummaryResponseTreeNodeCostComparator()));
  }

  private static void computeCost(SummaryResponseTreeNode node, double targetChangeRatio, double topBaselineValue,
      double topCurrentValue, double topBaselineSize, double topCurrentSize, CostFunction costFunction) {
    if (node.cubeNode != null) {
      double nodeCost = costFunction.computeCost(targetChangeRatio, node.getBaselineValue(), node.getCurrentValue(),
          node.getBaselineSize(), node.getCurrentSize(), topBaselineValue, topCurrentValue, topBaselineSize,
          topCurrentSize);
      node.cubeNode.setCost(nodeCost);
      node.subTreeCost = nodeCost;
    }
    for (SummaryResponseTreeNode child : node.children) {
      computeCost(child, targetChangeRatio, topBaselineValue, topCurrentValue, topBaselineSize, topCurrentSize, costFunction);
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
    CubeNode cubeNode; // If it is null, this node is a dummy node.
    double subTreeCost;
    int level;

    @JsonIgnore
    SummaryResponseTreeNode parent;

    List<SummaryResponseTreeNode> children = new ArrayList<>();

    public List<String> getDimensionValues() {
      return cubeNode.getDimensionValues().values();
    }

    public void setLevel(int level) {
      this.level = level;
    }

    public int getLevel() {
      return level;
    }

    public double getBaselineValue() {
      return cubeNode.getBaselineValue();
    }

    public double getCurrentValue() {
      return cubeNode.getCurrentValue();
    }

    public double getBaselineSize() {
      return cubeNode.getBaselineSize();
    }

    public double getCurrentSize() {
      return cubeNode.getCurrentSize();
    }

    public double currentChangeRatio() {
      if (cubeNode != null) {
        return cubeNode.changeRatio();
      } else {
        SummaryResponseTreeNode parent = this;
        do {
          if (parent.cubeNode == null) {
            parent = parent.parent;
          } else {
            break;
          }
        } while (true);
        return parent.currentChangeRatio();
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
      CubeNode tmpNode = A.cubeNode;
      A.cubeNode = B.cubeNode;
      B.cubeNode = tmpNode;
      List<SummaryResponseTreeNode> tmpChildren = A.children;
      A.children = B.children;
      B.children = tmpChildren;
      int tmpLevel = A.level;
      A.level = B.level;
      B.level = tmpLevel;
    }
  }
}
