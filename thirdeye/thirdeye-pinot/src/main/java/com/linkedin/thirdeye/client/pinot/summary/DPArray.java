package com.linkedin.thirdeye.client.pinot.summary;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


public class DPArray {
  double targetRatio;
  List<DPSlot> slots;

  public DPArray(int size) {
    slots = new ArrayList<>(size);
    for (int i = 0; i < size; ++i) {
      slots.add(new DPSlot());
    }
  }

  public DPSlot slotAt(int index) {
    return slots.get(index);
  }

  public int size() {
    return slots.size()-1;
  }

  public Set<HierarchyNode> getAnswer() {
    return slots.get(slots.size() - 1).ans;
  }

  public void reset() {
    targetRatio = 0.;
    for (DPSlot slot : slots) {
      slot.cost = 0.;
      slot.ans.clear();
    }
  }

  public String toString() {
    if (slots != null) {
      StringBuilder sb = new StringBuilder();
      for (DPSlot slot : slots) {
        sb.append(ToStringBuilder.reflectionToString(slot, ToStringStyle.SHORT_PREFIX_STYLE));
        sb.append('\n');
      }
      return sb.toString();
    } else
      return "";
  }

  public static class DPSlot {
    double cost;
    Set<HierarchyNode> ans = new HashSet<>();

    public String toString() {
      return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
  }
}
