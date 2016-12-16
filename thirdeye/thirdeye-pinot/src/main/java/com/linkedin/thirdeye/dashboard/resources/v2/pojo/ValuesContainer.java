package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

public class ValuesContainer {
  // TODO: add percentage change, cummulative values etc here
  double[] currentValues;
  double[] baselineValues;
  double[] percentageChange;

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

  public double[] getPercentageChange() {
    return percentageChange;
  }

  public void setPercentageChange(double[] percentageChange) {
    this.percentageChange = percentageChange;
  }
}
