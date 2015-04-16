package com.linkedin.thirdeye.dashboard.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;

import java.io.IOException;
import java.util.List;

public class HeatMap implements Comparable<HeatMap> {
  private final ObjectMapper objectMapper;
  private final String metric;
  private final String dimension;
  private final List<String> statsNames;
  private final List<HeatMapCell> cells;

  public HeatMap(ObjectMapper objectMapper,
                 String metric,
                 String dimension,
                 List<HeatMapCell> cells,
                 List<String> statsNames) {
    this.objectMapper = objectMapper;
    this.metric = metric;
    this.dimension = dimension;
    this.statsNames = statsNames;
    this.cells = cells;
  }

  public String getMetric() {
    return metric;
  }

  public String getDimension() {
    return dimension;
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
    return Objects.toStringHelper(HeatMap.class)
        .add("metric", metric)
        .add("dimension", dimension)
        .add("statsNames", statsNames)
        .toString();
  }

  @Override
  public int compareTo(HeatMap o) {
    if (o.getMetric().equals(metric)) {
      return o.getDimension().compareTo(dimension);
    }
    return o.getMetric().compareTo(metric);
  }
}
