package com.linkedin.thirdeye.dashboard.views;

import io.dropwizard.views.View;
import org.apache.commons.math3.util.Pair;

import java.util.List;

public class CustomFunnelTabularView extends View {
  private final List<String> metricLabels;
  private final List<Pair<Long, Number[]>> table;

  public CustomFunnelTabularView(List<String> metricLabels, List<Pair<Long, Number[]>> table) {
    super("custom-funnel-tabular.ftl");
    this.metricLabels = metricLabels;
    this.table = table;
  }

  public List<String> getMetricLabels() {
    return metricLabels;
  }

  public List<Pair<Long, Number[]>> getTable() {
    return table;
  }
}
