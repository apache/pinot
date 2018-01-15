package com.linkedin.thirdeye.client.diffsummary;

import com.google.common.base.MoreObjects;

public class DimNameValueCostEntry implements Comparable<DimNameValueCostEntry>{
  private double contributionFactor;
  private String dimName;
  private String dimValue;
  private double cost;
  private double currentValue;
  private double baselineValue;

  public DimNameValueCostEntry(String dimension, String dimValue, double dimValueCost, double contributionFactor,
      double currentValue, double baselineValue) {
    this.dimName = dimension;
    this.dimValue = dimValue;
    this.cost = dimValueCost;
    this.contributionFactor = contributionFactor;
    this.currentValue = currentValue;
    this.baselineValue = baselineValue;
  }

  public double getContributionFactor() {
    return contributionFactor;
  }

  public void setContributionFactor(double contributionFactor) {
    this.contributionFactor = contributionFactor;
  }

  public String getDimName() {
    return dimName;
  }

  public void setDimName(String dimName) {
    this.dimName = dimName;
  }

  public String getDimValue() {
    return dimValue;
  }

  public void setDimValue(String dimValue) {
    this.dimValue = dimValue;
  }

  public double getCost() {
    return cost;
  }

  public void setCost(double cost) {
    this.cost = cost;
  }

  public double getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(double currentValue) {
    this.currentValue = currentValue;
  }

  public double getBaselineValue() {
    return baselineValue;
  }

  public void setBaselineValue(double baselineValue) {
    this.baselineValue = baselineValue;
  }

  @Override
  public int compareTo(DimNameValueCostEntry that) {
    return Double.compare(this.cost, that.cost);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Entry").add("dim", String.format("%s:%s", dimName, dimValue))
        .add("baselineVal", baselineValue).add("currentVal", currentValue).add("delta", currentValue - baselineValue)
        .add("ratio", String.format("%.2f", currentValue / baselineValue))
        .add("sizeFactor", String.format("%.2f", contributionFactor)).add("cost", String.format("%.2f", cost))
        .toString();
  }
}

