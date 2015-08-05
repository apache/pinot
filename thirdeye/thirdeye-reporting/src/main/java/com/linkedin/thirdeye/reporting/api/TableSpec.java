package com.linkedin.thirdeye.reporting.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TableSpec {

  private List<String> metrics;
  private String groupBy;
  private FilterSpec filter;
  private Map<String, String> fixedDimensions;
  private int baselineSize;
  private TimeUnit baselineUnit;

  public TableSpec() {

  }

  public TableSpec(List<String> metrics, String groupBy, FilterSpec filter, Map<String, String> fixedDimensions,
      int baselineSize, TimeUnit baselineUnit) {
    super();
    this.metrics = metrics;
    this.groupBy = groupBy;
    this.filter = filter;
    this.fixedDimensions = fixedDimensions;
    this.baselineSize = baselineSize;
    this.baselineUnit = baselineUnit;
  }


  public List<String> getMetrics() {
    return metrics;
  }


  public String getGroupBy() {
    return groupBy;
  }


  public FilterSpec getFilter() {
    return filter;
  }


  public Map<String, String> getFixedDimensions() {
    return fixedDimensions;
  }


  public int getBaselineSize() {
    return baselineSize;
  }


  public TimeUnit getBaselineUnit() {
    return baselineUnit;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  public void setGroupBy(String groupBy) {
    this.groupBy = groupBy;
  }

  public void setFilter(FilterSpec filter) {
    this.filter = filter;
  }

  public void setFixedDimensions(Map<String, String> fixedDimensions) {
    this.fixedDimensions = fixedDimensions;
  }

  public void setBaselineSize(int baselineSize) {
    this.baselineSize = baselineSize;
  }

  public void setBaselineUnit(TimeUnit baselineUnit) {
    this.baselineUnit = baselineUnit;
  }


}
