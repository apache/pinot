package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

public class ValuesContainer {
  // TODO: add percentage change, cummulative values etc here
  double[] currentValues;
  double[] baselineValues;
  String[] percentageChange;

  String[] cumulativePercentageChange;

  public double[] getBaselineValues() {
    return baselineValues;
  }

  public void setBaselineValues(double[] baselineValues) {
    this.baselineValues = baselineValues;
  }

  public double[] getCurrentValues() {
    return currentValues;
  }

  public void setCurrentValues(double[] currentValues) {
    this.currentValues = currentValues;
  }

  public String[] getPercentageChange() {
    return percentageChange;
  }

  public void setPercentageChange(String[] percentageChange) {
    this.percentageChange = percentageChange;
  }


}
