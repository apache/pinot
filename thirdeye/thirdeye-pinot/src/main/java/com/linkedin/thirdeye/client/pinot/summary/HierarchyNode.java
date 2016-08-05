package com.linkedin.thirdeye.client.pinot.summary;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


class HierarchyNode {
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

  public double aggregatedRatio() {
    return data.ratio();
  }

  public double currentRatio() {
    return currentValue / baselineValue;
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