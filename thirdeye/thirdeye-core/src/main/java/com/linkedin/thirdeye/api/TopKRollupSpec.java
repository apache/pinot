package com.linkedin.thirdeye.api;

import java.util.List;
import java.util.Map;

public class TopKRollupSpec {

  Map<String, Double> threshold;
  List<TopKDimensionSpec> topKDimensionSpec;

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

}
