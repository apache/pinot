package com.linkedin.thirdeye.dashboard.api;

import java.util.List;
import java.util.Map;

public class MetricTable {
  private final Map<String, String> dimensionValues;
  private final List<MetricTableRow> rows;

  public MetricTable(Map<String, String> dimensionValues, List<MetricTableRow> rows) {
    this.dimensionValues = dimensionValues;
    this.rows = rows;
  }

  public Map<String, String> getDimensionValues() {
    return dimensionValues;
  }

  public List<MetricTableRow> getRows() {
    return rows;
  }
}
