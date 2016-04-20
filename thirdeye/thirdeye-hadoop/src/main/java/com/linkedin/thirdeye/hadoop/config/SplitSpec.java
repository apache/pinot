package com.linkedin.thirdeye.hadoop.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SplitSpec {
  private int threshold = 1000;
  private List<String> order;

  public SplitSpec() {
  }

  public SplitSpec(int threshold, List<String> order) {
    this.threshold = threshold;
    this.order = order;
  }

  @JsonProperty
  public int getThreshold() {
    return threshold;
  }

  @JsonProperty
  public List<String> getOrder() {
    return order;
  }
}
