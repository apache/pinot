package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

public class ValuesContainer {
  // TODO: add percentage change, cummulative values etc here
  double[] currentValues;
  double[] baselineValues;
  String[] percentageChange;

  double[] cumulativeCurrentValues;
  double[] cumulativeBaselineValues;
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

  public double[] getCumulativeBaselineValues() {
    return cumulativeBaselineValues;
  }

  public void setCumulativeBaselineValues(double[] cumulativeBaselineValues) {
    this.cumulativeBaselineValues = cumulativeBaselineValues;
  }

  public double[] getCumulativeCurrentValues() {
    return cumulativeCurrentValues;
  }

  public void setCumulativeCurrentValues(double[] cumulativeCurrentValues) {
    this.cumulativeCurrentValues = cumulativeCurrentValues;
  }

  public String[] getCumulativePercentageChange() {
    return cumulativePercentageChange;
  }

  public void setCumulativePercentageChange(String[] cumulativePercentageChange) {
    this.cumulativePercentageChange = cumulativePercentageChange;
  }
}
