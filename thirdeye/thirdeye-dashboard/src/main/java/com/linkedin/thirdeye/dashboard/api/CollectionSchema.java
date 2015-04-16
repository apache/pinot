package com.linkedin.thirdeye.dashboard.api;

import com.google.common.base.Objects;

import java.util.List;

public class CollectionSchema {
  private List<String> dimensions;
  private List<String> metrics;

  public CollectionSchema() {}

  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(CollectionSchema.class)
        .add("dimensions", dimensions)
        .add("metrics", metrics)
        .toString();
  }
}
