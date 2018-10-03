/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.client.diffsummary;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.util.Objects;
import org.apache.commons.lang.ObjectUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class HierarchyNode {
  private int level;
  private int index;
  private double baselineValue;
  private double currentValue;
  private double cost;
  private Row data;
  private HierarchyNode parent;
  private List<HierarchyNode> children = new ArrayList<>();

  /**
   * Constructs a root HierarchyNode whose level and index is 0 and parent pointer is null.
   *
   * @param data the data of this root node.
   */
  public HierarchyNode(Row data) {
    this.data = Preconditions.checkNotNull(data);
    this.baselineValue = data.getBaselineValue();
    this.currentValue = data.getCurrentValue();
  }

  /**
   * Constructs a HierarchyNode which is specified information.
   *
   * @param level the level of this node.
   * @param index the index of this node that is located in its parent's children list.
   * @param data the data of this node.
   * @param parent the parent of this node.
   */
  public HierarchyNode(int level, int index, Row data, HierarchyNode parent) {
    this(data);
    this.level = level;
    this.index = index;
    this.parent = Preconditions.checkNotNull(parent);
    Dimensions parentDimension = new Dimensions(parent.data.getDimensions().namesToDepth(parent.getLevel()));
    Dimensions childDimension = new Dimensions(data.getDimensions().namesToDepth(level));
    Preconditions.checkState(parentDimension.isParentOf(childDimension),
        "Current node is not a child node of the given parent node. Current and parent dimensions: ",
        data.getDimensions(), parent.data.getDimensions());
    parent.children.add(this);
  }

  public int getLevel() {
    return level;
  }

  public void resetValues() {
    this.baselineValue = this.data.getBaselineValue();
    this.currentValue = this.data.getCurrentValue();
  }

  public void removeNodeValues(HierarchyNode node) {
    this.baselineValue -= node.baselineValue;
    this.currentValue -= node.currentValue;
  }

  public void addNodeValues(HierarchyNode node) {
    this.baselineValue += node.baselineValue;
    this.currentValue += node.currentValue;
  }

  public double getBaselineValue() {
    return baselineValue;
  }

  public double getCurrentValue() {
    return currentValue;
  }

  @JsonIgnore
  public double getOriginalBaselineValue() {
    return data.getBaselineValue();
  }

  @JsonIgnore
  public double getOriginalCurrentValue() {
    return data.getCurrentValue();
  }

  public double getCost() {
    return cost;
  }

  public void setCost(double cost) {
    this.cost = cost;
  }

  @JsonIgnore
  public Dimensions getDimensions() {
    return data.getDimensions();
  }

  @JsonIgnore
  public DimensionValues getDimensionValues() {
    return data.getDimensionValues();
  }

  public HierarchyNode getParent() {
    return parent;
  }

  public int childrenSize() {
    return children.size();
  }

  public List<HierarchyNode> getChildren() {
    return Collections.unmodifiableList(children);
  }

  /**
   * Returns the ratio that is calculated by the aggregate current and aggregate baseline values of all children node.
   *
   * @return aggregated current value of all children / aggregated baseline value of all children;
   */
  public double aggregatedRatio() {
    return data.ratio();
  }

  /**
   * Returns the ratio that is calculated by currentValue and baselineValue.
   * @return currentValue / baselineValue;
   */
  public double currentRatio() {
    return currentValue / baselineValue;
  }

  /**
   * Return the ratio of the node. If the ratio is not a finite number, then it returns the aggregatedRatio.
   * If the aggregatedRatio is not a finite number, then it bootstraps to the parents until it finds a finite
   * ratio. If no finite ratio available, then it returns 1.
   */
  public double targetRatio() {
    double ratio = currentRatio();
    if (!Double.isInfinite(ratio) && Double.compare(ratio, 0d) != 0) {
      return ratio;
    } else {
      ratio = aggregatedRatio();
      if (!Double.isInfinite(ratio) && Double.compare(ratio, 0d) != 0) {
        return ratio;
      } else {
        if (parent != null) {
          return parent.targetRatio();
        } else {
          return 1.;
        }
      }
    }
  }

  /**
   * Returns the current ratio of this node is increased or decreased, i.e., returns true if ratio of the node >= 1.0.
   * If the current ratio is NAN, then the ratio of the aggregated values is used.
   *
   * Precondition: the aggregated baseline and current values cannot both be zero.
   */
  public boolean side() {
    double ratio = currentRatio();
    if (!Double.isNaN(ratio)) {
      return Double.compare(1., currentRatio()) <= 0;
    } else {
      return Double.compare(1., aggregatedRatio()) <= 0;
    }
  }

  /**
   * Check if the hierarchical tree of the given two root nodes are the same.
   *
   * @param node1 the root node of the first hierarchical tree.
   * @param node2 the root node of the second hierarchical tree.
   *
   * @return true if both hierarchical tree are the same.
   */
  public static boolean equalHierarchy(HierarchyNode node1, HierarchyNode node2) {
    return equalHierarchy(node1, null, node2, null);
  }

  private static boolean equalHierarchy(HierarchyNode node1, HierarchyNode node1Parent, HierarchyNode node2,
      HierarchyNode node2Parent) {
    boolean sameData = ObjectUtils.equals(node1, node2);
    if (sameData) {
      // Check parent reference
      if (node1Parent != null && node1.getParent() != node1Parent) {
        return false;
      }
      if (node2Parent != null && node2.getParent() != node2Parent) {
        return false;
      }

      // Check children reference
      List<HierarchyNode> children1 = node1.children;
      List<HierarchyNode> children2 = node2.children;
      if (children1.size() != children2.size()) {
        return false;
      }
      int size = children1.size();
      for (int i = 0; i < size; i++) {
        HierarchyNode child1 = children1.get(i);
        HierarchyNode child2 = children2.get(i);
        boolean sameChild = equalHierarchy(child1, node1, child2, node2);
        if (!sameChild) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns if the data of current node equals to the data of other node. The parent and children nodes are not
   * compared due to the cyclic references between parent and children nodes.
   *
   * @param o the other node.
   *
   * @return true if the data of current node equals to the data of other node.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HierarchyNode that = (HierarchyNode) o;
    return getLevel() == that.getLevel() && index == that.index
        && Double.compare(that.getBaselineValue(), getBaselineValue()) == 0
        && Double.compare(that.getCurrentValue(), getCurrentValue()) == 0
        && Double.compare(that.getCost(), getCost()) == 0 && Objects.equals(data, that.data);
  }

  /**
   * Returns the hash code that is generated base on the data of this node.
   *
   * @return the hash code that is generated base on the data of this node.
   */
  @Override
  public int hashCode() {
    return Objects
        .hash(getLevel(), index, getBaselineValue(), getCurrentValue(), getCost(), data);
  }

  private String toStringAsParent() {
    return MoreObjects.toStringHelper(this).add("level", level).add("index", index).add("baselineValue", baselineValue)
        .add("currentValue", currentValue).add("cost", cost).add("data", data).toString();
  }

  private static String toStringAsParent(HierarchyNode node) {
    if (node == null) {
      return "null";
    } else {
      return node.toStringAsParent();
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("level", level).add("index", index).add("baselineValue", baselineValue)
        .add("currentValue", currentValue).add("cost", cost).add("data", data).add("parent", toStringAsParent(parent))
        .toString();
  }
}
