package com.linkedin.thirdeye.dashboard.api;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;

public class CollectionSchema {
  private List<String> dimensions;
  private List<String> dimensionAliases;
  private List<String> metrics;
  private List<String> metricAliases;

  public CollectionSchema() {
  }

  public static CollectionSchema fromStarTreeConfig(StarTreeConfig config) {
    CollectionSchema schema = new CollectionSchema();
    List<DimensionSpec> dimensionSpecs = config.getDimensions();
    ArrayList<String> dimensions = new ArrayList<>(dimensionSpecs.size());
    ArrayList<String> dimensionAliases = new ArrayList<>(dimensionSpecs.size());
    for (DimensionSpec spec : dimensionSpecs) {
      dimensions.add(spec.getName());
      dimensionAliases.add(spec.getAlias());
    }
    schema.setDimensions(dimensions);
    schema.setDimensionAliases(dimensionAliases);

    List<MetricSpec> metricSpecs = config.getMetrics();
    ArrayList<String> metrics = new ArrayList<>(metricSpecs.size());
    ArrayList<String> metricAliases = new ArrayList<>(metricSpecs.size());
    for (MetricSpec spec : metricSpecs) {
      metrics.add(spec.getName());
      metricAliases.add(spec.getAlias());
    }

    schema.setMetrics(metrics);
    schema.setMetricAliases(metricAliases);
    return schema;
  }

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
    return Objects.toStringHelper(CollectionSchema.class).add("dimensions", dimensions)
        .add("dimensionAliases", dimensionAliases).add("metrics", metrics)
        .add("metricAliases", metricAliases).toString();
  }
}
