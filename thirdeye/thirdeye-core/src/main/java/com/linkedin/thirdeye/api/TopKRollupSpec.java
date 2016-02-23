package com.linkedin.thirdeye.api;

import java.util.List;
import java.util.Map;

public class TopKRollupSpec {

  Map<String, Double> threshold;
  List<TopKDimensionSpec> topKDimensionSpec;
  Map<String, String> exceptions;

  public TopKRollupSpec() {

  }

  public Map<String, Double> getThreshold() {
    return threshold;
  }

  public void setThreshold(Map<String, Double> threshold) {
    this.threshold = threshold;
  }

  public List<TopKDimensionSpec> getTopKDimensionSpec() {
    return topKDimensionSpec;
  }

  public void setTopKDimensionSpec(List<TopKDimensionSpec> topKDimensionSpec) {
    this.topKDimensionSpec = topKDimensionSpec;
  }

  public Map<String, String> getExceptions() {
    return exceptions;
  }

  public void setExceptions(Map<String, String> exceptions) {
    this.exceptions = exceptions;
  }



}
