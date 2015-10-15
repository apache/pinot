package com.linkedin.thirdeye.dashboard.api;

import java.util.List;
import java.util.Map;


public class MetricTable {
  private final Map<String, String> dimensionValues;
  private final List<MetricTableRow> rows;
  private final List<MetricTableRow> cumulativeRows;

  public MetricTable(Map<String, String> dimensionValues, List<MetricTableRow> rows,
      List<MetricTableRow> cumulativeRows) {
    this.dimensionValues = dimensionValues;
    this.rows = rows;
    this.cumulativeRows = cumulativeRows;
  }

  public Map<String, String> getDimensionValues() {
    return dimensionValues;
  }

  public List<MetricTableRow> getRows() {
    return rows;
  }

  public List<MetricTableRow> getCumulativeRows() {
    return cumulativeRows;
  }
}
