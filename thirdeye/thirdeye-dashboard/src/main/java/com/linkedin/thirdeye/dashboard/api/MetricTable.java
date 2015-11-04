package com.linkedin.thirdeye.dashboard.api;

import java.util.List;

import org.apache.commons.lang.StringUtils;

public class MetricTable {
  private final List<MetricDataRow> rows;
  private final List<MetricDataRow> cumulativeRows;

  public MetricTable(List<MetricDataRow> rows, List<MetricDataRow> cumulativeRows) {
    this.rows = rows;
    this.cumulativeRows = cumulativeRows;
  }

  public List<MetricDataRow> getRows() {
    return rows;
  }

  public List<MetricDataRow> getCumulativeRows() {
    return cumulativeRows;
  }

  // Debugging only: prints rows
  @Override
  public String toString() {
    return StringUtils.join(rows, ",");
  }
}
