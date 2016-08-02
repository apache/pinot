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
  boolean isRoot = false;
  boolean isWorker = false;
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

  public String toIDString() {
    return "(" + level + "," + index + ")";
  }

  public String toString() {
    ToStringBuilder sb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
    sb.append(level).append(index).append(data.dimensionValues.toString());
    if (parent != null) {
      sb.append(parent.toIDString());
    } else { sb.append(parent); }
    if (isRoot || isWorker) {
      StringBuilder ssb = new StringBuilder();
      if (isRoot) ssb.append('r');
      if (isWorker) ssb.append('w');
      sb.append("type", ssb.toString());
    }
    return sb.toString();
  }
}