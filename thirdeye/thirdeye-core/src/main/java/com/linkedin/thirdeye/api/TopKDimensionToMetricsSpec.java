package com.linkedin.thirdeye.api;

import java.util.Map;

public class TopKDimensionToMetricsSpec {

  String dimensionName;
  Map<String, Integer> topk;

  public TopKDimensionToMetricsSpec() {

  }

  public TopKDimensionToMetricsSpec(String dimensionName, Map<String, Integer> topk) {
    this.dimensionName = dimensionName;
    this.topk = topk;
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public void setDimensionName(String dimensionName) {
    this.dimensionName = dimensionName;
  }

  public Map<String, Integer> getTopk() {
    return topk;
  }

  public void setTopk(Map<String, Integer> topk) {
    this.topk = topk;
  }

  public String toString() {
    return "{ dimensionName : " + dimensionName + ", topk : " + topk + " }";
  }

}
