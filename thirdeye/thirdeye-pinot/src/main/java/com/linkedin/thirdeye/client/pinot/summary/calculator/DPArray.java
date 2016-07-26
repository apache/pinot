package com.linkedin.thirdeye.client.pinot.summary.calculator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


public class DPArray {
  List<DP> cells;

  public DPArray(int N, double ratio, double ga, double gb, int numRecord) {
    cells = new ArrayList<>(N + 1);
    for (int i = 0; i <= N; ++i) {
      cells.add(new DP(ratio, ga, gb, numRecord));
    }
  }

  public DP get(int index) {
    return cells.get(index);
  }

  public int size() {
    return cells.size();
  }

  public BitSet getAnswer() {
    return (BitSet) cells.get(cells.size() - 1).ans.clone();
  }

  public String toString() {
    if (cells != null) {
      StringBuilder sb = new StringBuilder();
      for (DP cell : cells) {
        sb.append(ToStringBuilder.reflectionToString(cell, ToStringStyle.SHORT_PREFIX_STYLE));
        sb.append('\n');
      }
      return sb.toString();
    } else
      return "";
  }

  public static class DP {
    double cost;
    BitSet ans;
    double ratio;
    double subMetricA;
    double subMetricB;

    DP(double ratio, double ga, double gb, int numRecord) {
      this.ratio = ratio;
      this.ans = new BitSet(numRecord);
      this.subMetricA = ga;
      this.subMetricB = gb;
    }

    public double getCost() {
      return cost;
    }

    public BitSet getAns() {
      return ans;
    }

    public double getRatio() {
      return ratio;
    }

    public double getSubMetricA() {
      return subMetricA;
    }

    public double getSubMetricB() {
      return subMetricB;
    }

    public String toString() {
      return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
  }
}
