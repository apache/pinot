package com.linkedin.thirdeye.dashboard.views;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;

import com.linkedin.thirdeye.dashboard.api.funnel.FunnelSpec;


public class FunnelHeatMapView {
  private final String name;

  private final List<String> metricLabels;
  private final Map<String, String> aliasToActualMap;
  private final List<Pair<Long, Number[]>> table;
  private final Map<String, Integer> metricIndex;
  private final String current;
  private final String baseline;

  public FunnelHeatMapView(FunnelSpec spec, List<Pair<Long, Number[]>> table, DateTime current, DateTime baseline) {
    this.metricLabels = spec.getActualMetricNames();
    this.table = table;
    int counter = 1;
    this.metricIndex = new LinkedHashMap<String, Integer>();
    this.name = spec.getName();
    for (String metric : this.metricLabels) {
      metricIndex.put(metric, new Integer(counter++));
    }
    aliasToActualMap = spec.getAliasToActualMetrics();
    this.current = current.toLocalDate().toString();
    this.baseline = baseline.toLocalDate().toString();
  }

  public String getCurrent() {
    return current;
  }

  public String getBaseline() {
    return baseline;
  }

  public Map<String, String> getAliasToActualMap() {
    return aliasToActualMap;
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
