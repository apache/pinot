package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class WowSummary {
  Multimap<String, MetricSummary> metricAliasToMetricSummariesMap = ArrayListMultimap.create();

  public Multimap<String, MetricSummary> getMetricAliasToMetricSummariesMap() {
    return metricAliasToMetricSummariesMap;
  }

  public void setMetricAliasToMetricSummariesMap(Multimap<String, MetricSummary> metricAliasToMetricSummariesMap) {
    this.metricAliasToMetricSummariesMap = metricAliasToMetricSummariesMap;
  }


}
