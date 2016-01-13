package com.linkedin.thirdeye.reporting.api;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.linkedin.thirdeye.reporting.api.anomaly.AnomalyReportTable;

public class AliasSpec {

  private Map<String, Map<String, String>> dimensionValues;
  private Map<String, String> metric;
  private Map<String, String> dimension;

  public AliasSpec() {
  }

  public AliasSpec(Map<String, Map<String, String>> dimensionValues, Map<String, String> metric,
      Map<String, String> dimension) {
    this.dimensionValues = dimensionValues;
    this.metric = metric;
    this.dimension = dimension;
  }

  public Map<String, Map<String, String>> getDimensionValues() {
    return dimensionValues;
  }

  public Map<String, String> getMetric() {
    return metric;
  }

  public Map<String, String> getDimension() {
    return dimension;
  }

  public void setDimensionValues(Map<String, Map<String, String>> dimensionValues) {
    this.dimensionValues = dimensionValues;
  }

  public void setMetric(Map<String, String> metric) {
    this.metric = metric;
  }

  public void setDimension(Map<String, String> dimension) {
    this.dimension = dimension;
  }

  public static void alias(ReportConfig reportConfig, List<Table> tables,
      Map<String, AnomalyReportTable> anomalyReportTables) {

    if (reportConfig.getAliases() == null) {
      return;
    }

    AliasSpec aliasSpec = reportConfig.getAliases();
    Map<String, String> metricMap = aliasSpec.getMetric();
    Map<String, String> dimensionMap = aliasSpec.getDimension();
    Map<String, Map<String, String>> dimensionValuesMap = aliasSpec.getDimensionValues();

    for (Table table : tables) {

      // alias dimension values
      String groupByDimension = table.getTableSpec().getGroupBy();
      if (groupByDimension != null && dimensionValuesMap != null
          && dimensionValuesMap.containsKey(groupByDimension)) {
        Map<String, String> dimensionValues = dimensionValuesMap.get(groupByDimension);
        for (TableReportRow tableReportRow : table.getTableReportRows()) {
          GroupBy groupBy = tableReportRow.getGroupByDimensions();
          if (dimensionValues.containsKey(groupBy.getDimension())) {
            groupBy.setDimension(dimensionValues.get(groupBy.getDimension()));
          }
        }
      }

      // alias metrics
      if (metricMap != null) {
        for (TableReportRow tableReportRow : table.getTableReportRows()) {
          for (ReportRow reportRow : tableReportRow.getRows()) {
            if (metricMap.containsKey(reportRow.getMetric())) {
              reportRow.setMetric(metricMap.get(reportRow.getMetric()));
            }
          }
        }
      }

      // alias groupby dimension
      if (dimensionMap != null && dimensionMap.containsKey(table.getTableSpec().getGroupBy())) {
        table.getTableSpec().setGroupBy(dimensionMap.get(table.getTableSpec().getGroupBy()));
      }
    }

    if (anomalyReportTables != null && metricMap != null) {
      for (Entry<String, String> entry : metricMap.entrySet()) {
        if (anomalyReportTables.containsKey(entry.getKey())) {
          anomalyReportTables.put(entry.getValue(), anomalyReportTables.remove(entry.getKey()));
        }
      }
    }
  }

}
