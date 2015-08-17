package com.linkedin.thirdeye.dashboard.api;

import java.util.List;
import java.util.Map;

public class DimensionTable {
  private final List<String> metricNames;
  private final Map<String, String> metricAliases;
  private final String dimensionName;
  private final String dimensionAlias;
  private final List<DimensionTableRow> rows;

  public DimensionTable(List<String> metricNames,
                        Map<String, String> metricAliases,
                        String dimensionName,
                        String dimensionAlias,
                        List<DimensionTableRow> rows) {
    this.metricNames = metricNames;
    this.metricAliases = metricAliases;
    this.dimensionName = dimensionName;
    this.dimensionAlias = dimensionAlias;
    this.rows = rows;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  public Map<String, String> getMetricAliases() {
    return metricAliases;
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public String getDimensionAlias() {
    return dimensionAlias;
  }

  public List<DimensionTableRow> getRows() {
    return rows;
  }
}
