package com.linkedin.thirdeye.dashboard.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;

import java.io.IOException;
import java.util.List;

public class HeatMap implements Comparable<HeatMap> {
  private final ObjectMapper objectMapper;
  private final String metric;
  private final String metricAlias;
  private final String dimension;
  private final String dimensionAlias;
  private final List<HeatMapCell> cells;
  private final List<String> statsNames;

  public HeatMap(ObjectMapper objectMapper, String metric, String metricAlias, String dimension,
      String dimensionAlias, List<HeatMapCell> cells, List<String> statsNames) {
    this.objectMapper = objectMapper;
    this.metric = metric;
    this.metricAlias = metricAlias;
    this.dimension = dimension;
    this.dimensionAlias = dimensionAlias;
    this.cells = cells;
    this.statsNames = statsNames;
  }

  public String getMetric() {
    return metric;
  }

  public String getDimension() {
    return dimension;
  }

  public String getDimensionAlias() {
    return dimensionAlias;
  }

  public String getMetricAlias() {
    return metricAlias;
  }

  public List<String> getStatsNames() {
    return statsNames;
  }

  public String getStatsNamesJson() throws IOException {
    return objectMapper.writeValueAsString(statsNames);
  }

  public List<HeatMapCell> getCells() {
    return cells;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(HeatMap.class).add("metric", metric)
        .add("metricAlias", metricAlias).add("dimension", dimension)
        .add("dimensionAlias", dimensionAlias).add("statsNames", statsNames).toString();
  }

  @Override
  public int compareTo(HeatMap o) {
    if (o.getMetric().equals(metric)) {
      return o.getDimension().compareTo(dimension);
    }
    return o.getMetric().compareTo(metric);
  }
}
