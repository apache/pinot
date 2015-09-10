package com.linkedin.thirdeye.dashboard.views;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;


public class FunnelHeatMapView {
  private final String name;
  private final List<String> metricLabels;
  private final List<Pair<Long, Number[]>> table;
  private final Map<String, Integer> metricIndex;

  public FunnelHeatMapView(String name, List<String> metricLabels, List<Pair<Long, Number[]>> table) {
    this.metricLabels = metricLabels;
    this.table = table;
    int counter = 1;
    this.metricIndex = new LinkedHashMap<String, Integer>();
    this.name = name;
    for (String metric : this.metricLabels) {
      metricIndex.put(metric, new Integer(counter++));

    }
  }

  public String getName() {
    return name;
  }

  public Map<String, Integer> getMetricIndex() {
    return metricIndex;
  }

  public List<String> getMetricLabels() {
    return metricLabels;
  }

  public List<Pair<Long, Number[]>> getTable() {
    return table;
  }
}
