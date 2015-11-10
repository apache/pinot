package com.linkedin.thirdeye.dashboard.views;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;

import com.linkedin.thirdeye.dashboard.api.MetricTable;

import io.dropwizard.views.View;

public class DimensionViewContributors extends View {

  private final List<String> metrics;
  private final List<String> dimensions;
  private final Map<String, MetricTable> metricTotalTable;
  private final Map<Pair<String, String>, Map<String, MetricTable>> dimensionValueTables;

  public DimensionViewContributors(List<String> metrics, List<String> dimensions,
      Map<String, MetricTable> metricTotalTable,
      Map<Pair<String, String>, Map<String, MetricTable>> tables) {
    super("dimensions/breakdown.ftl");
    this.metrics = new ArrayList<>(metrics);
    Collections.sort(this.metrics);
    this.metricTotalTable = metricTotalTable;
    this.dimensions = new ArrayList<>(dimensions);
    Collections.sort(this.dimensions);
    this.dimensionValueTables = tables;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public Map<String, MetricTable> getMetricTotalTable() {
    return metricTotalTable;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  /*
   * Recommended not to use this. Key = (metric, dimension)
   */
  public Map<Pair<String, String>, Map<String, MetricTable>> getDimensionValueTables() {
    return dimensionValueTables;
  }

  public Map<String, MetricTable> getDimensionValueTable(String metric, String dimension) {
    Map<String, MetricTable> result =
        dimensionValueTables.get(new Pair<String, String>(metric, dimension));
    return (result == null ? Collections.<String, MetricTable> emptyMap() : result);
  }

}
