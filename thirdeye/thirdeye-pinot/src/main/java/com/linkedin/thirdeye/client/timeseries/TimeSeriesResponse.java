package com.linkedin.thirdeye.client.timeseries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.linkedin.thirdeye.client.MetricFunction;

public class TimeSeriesResponse {
  private final int numRows;
  private final List<MetricFunction> metricFunctions;
  private final List<String> metrics;
  private final List<String> dimensionNames;
  private final List<TimeSeriesRow> rows;

  public TimeSeriesResponse(List<MetricFunction> metricFunctions, List<String> dimensionNames,
      List<TimeSeriesRow> rows) {
    rows = new ArrayList<>(rows);
    Collections.sort(rows);
    this.rows = Collections.unmodifiableList(rows);
    this.metricFunctions = metricFunctions;
    metrics = new ArrayList<>();
    for (MetricFunction function : metricFunctions) {
      metrics.add(function.getMetricName());
    }
    this.dimensionNames = dimensionNames;
    numRows = rows.size();
  }

  public int getNumRows() {
    return numRows;
  }

  public List<MetricFunction> getMetricFunctions() {
    return metricFunctions;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public List<TimeSeriesRow> getRows() {
    return rows;
  }

  public TimeSeriesRow getRow(int index) {
    return rows.get(index);
  }
}
