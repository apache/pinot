package com.linkedin.thirdeye.reporting.api;

import java.util.List;

public class FilterSpec {
  private long metricThreshold;
  private List<String> includeDimensions;

  public FilterSpec() {
  }

  public FilterSpec(long metricThreshold, List<String> includeDimensions) {
    this.metricThreshold = metricThreshold;
    this.includeDimensions = includeDimensions;
  }

  public long getMetricThreshold() {
    return metricThreshold;
  }

  public List<String> getIncludeDimensions() {
    return includeDimensions;
  }

  public void setMetricThreshold(long metricThreshold) {
    this.metricThreshold = metricThreshold;
  }

  public void setIncludeDimensions(List<String> includeDimensions) {
    this.includeDimensions = includeDimensions;
  }

}
