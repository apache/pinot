package com.linkedin.thirdeye.dashboard.api;

import com.google.common.base.Objects;

import java.util.List;
import java.util.Map;

public class CollectionSchema {
  private List<String> dimensions;
  private List<String> dimensionAliases;
  private List<String> metrics;
  private List<String> metricAliases;

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

  public List<String> getMetricAliases() {
    return metricAliases;
  }

  public void setMetricAliases(List<String> metricAliases) {
    this.metricAliases = metricAliases;
  }

  public List<String> getDimensionAliases() {
    return dimensionAliases;
  }

  public void setDimensionAliases(List<String> dimensionAliases) {
    this.dimensionAliases = dimensionAliases;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(CollectionSchema.class)
        .add("dimensions", dimensions)
        .add("dimensionAliases", dimensionAliases)
        .add("metrics", metrics)
        .add("metricAliases", metricAliases)
        .toString();
  }
}
