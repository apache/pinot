package com.linkedin.thirdeye.dashboard.views;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.dashboard.api.MetricTable;

import io.dropwizard.views.View;

public class DimensionViewContributors extends View {

  private final List<String> metrics;
  private final List<String> dimensions;
  private final MetricTable metricTotalTable;
  private final Map<String, Map<String, MetricTable>> dimensionTables;

  public DimensionViewContributors(List<String> metrics, MetricTable metricTotalTable,
      Map<String, Map<String, MetricTable>> tables) {
    super("dimensions/breakdown.ftl");
    this.metrics = metrics;
    this.metricTotalTable = metricTotalTable;
    this.dimensions = new ArrayList<>(tables.keySet());
    this.dimensionTables = tables;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public MetricTable getMetricTotalTable() {
    return metricTotalTable;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public Map<String, Map<String, MetricTable>> getDimensionTables() {
    return dimensionTables;
  }

}
