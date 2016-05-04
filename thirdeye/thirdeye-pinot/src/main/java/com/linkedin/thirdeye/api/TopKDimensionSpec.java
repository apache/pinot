package com.linkedin.thirdeye.api;

public class TopKDimensionSpec {

  String dimensionName;
  int top;
  String metricName;

  public TopKDimensionSpec() {

  }

  public TopKDimensionSpec(String dimensionName, int top, String metricName) {
    this.dimensionName = dimensionName;
    this.top = top;
    this.metricName = metricName;
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public void setDimensionName(String dimensionName) {
    this.dimensionName = dimensionName;
  }

  public int getTop() {
    return top;
  }

  public void setTop(int top) {
    this.top = top;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public String toString() {
    return "{ dimensionName : " + dimensionName + ", top : " + top + ", metricName : " + metricName
        + " }";
  }

}
