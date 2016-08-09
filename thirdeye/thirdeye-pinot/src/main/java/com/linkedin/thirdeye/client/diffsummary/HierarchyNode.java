package com.linkedin.thirdeye.client.diffsummary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonIgnore;


public class HierarchyNode {
  int level;
  int index;
  double baselineValue;
  double currentValue;
  Row data;
  HierarchyNode parent;
  List<HierarchyNode> children = new ArrayList<>();

  public HierarchyNode() { }

  public HierarchyNode(int level, int index, Row data, HierarchyNode parent) {
    this.level = level;
    this.index = index;
    this.baselineValue = data.baselineValue;
    this.currentValue = data.currentValue;
    this.data = data;
    this.parent = parent;
  }

  public int getLevel() {
    return level;
  }

  public void resetValues() {
    this.baselineValue = this.data.baselineValue;
    this.currentValue = this.data.currentValue;
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
    return data.baselineValue;
  }

  @JsonIgnore
  public double getOriginalCurrentValue() {
    return data.currentValue;
  }

  @JsonIgnore
  public Dimensions getDimensions() {
    return data.dimensions;
  }

  @JsonIgnore
  public DimensionValues getDimensionValues() {
    return data.dimensionValues;
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

  public double aggregatedRatio() {
    return data.ratio();
  }

  public double currentRatio() {
    return currentValue / baselineValue;
  }

  /**
   * Return the ratio of the node. If the ratio is not a finite number, then it returns the aggragatedRatio.
   * If the aggregatedRaio is not a finite number, then it bootstraps to the parents until it finds a finite
   * ratio. If no finite ratio available, then it returns 1.
   */
  public double targetRatio() {
    double ratio = currentRatio();
    if (Double.isFinite(ratio)) {
      return ratio;
    } else {
      ratio = aggregatedRatio();
      if (Double.isFinite(ratio)) {
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

  public String toIDString() {
    return "(" + level + "," + index + ")";
  }

  public String toString() {
    ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
    tsb.append(data.dimensionValues)//.append(data.baselineValue).append(data.currentValue)
       .append(baselineValue).append(currentValue).append("ratio", currentRatio());
    if (parent != null) {
      tsb.append("\t parent").append(parent.data.dimensionValues).append(parent.baselineValue).append(parent.currentValue)
      .append("ratio", parent.currentRatio());
    }
    return tsb.toString();
  }
}