package com.linkedin.thirdeye.reporting.api;

import java.util.Map;

public class AliasSpec {

  private Map<String, Map<String, String>> dimensionValues;
  private Map<String, String> metric;
  private Map<String, String> dimension;

  public AliasSpec() {
  }


  public AliasSpec(Map<String, Map<String, String>> dimensionValues, Map<String, String> metric,
      Map<String, String> dimension) {
    this.dimensionValues = dimensionValues;
    this.metric = metric;
    this.dimension = dimension;
  }


  public Map<String, Map<String, String>> getDimensionValues() {
    return dimensionValues;
  }

  public Map<String, String> getMetric() {
    return metric;
  }

  public Map<String, String> getDimension() {
    return dimension;
  }


  public void setDimensionValues(Map<String, Map<String, String>> dimensionValues) {
    this.dimensionValues = dimensionValues;
  }


  public void setMetric(Map<String, String> metric) {
    this.metric = metric;
  }


  public void setDimension(Map<String, String> dimension) {
    this.dimension = dimension;
  }



}
