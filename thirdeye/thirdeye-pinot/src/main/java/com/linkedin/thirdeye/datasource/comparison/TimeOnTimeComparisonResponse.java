package com.linkedin.thirdeye.datasource.comparison;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.linkedin.thirdeye.datasource.comparison.Row.Metric;

public class TimeOnTimeComparisonResponse {

  int numRows;

  Set<String> metrics = new TreeSet<>();

  Set<String> dimensions = new TreeSet<>();

  private final List<Row> rows;

  public TimeOnTimeComparisonResponse(List<Row> rows) {
    this.rows = rows;
    for (Row row : rows) {
      for (Metric metric : row.metrics) {
        metrics.add(metric.metricName);
      }
      dimensions.add(row.dimensionName);
    }
    numRows = rows.size();
  }

  public int getNumRows() {
    return numRows;
  }

  public Set<String> getMetrics() {
    return metrics;
  }

  public Set<String> getDimensions() {
    return dimensions;
  }

  public Row getRow(int index) {
    return rows.get(index);
  }

  static class Builder {
    List<Row> rows = new ArrayList<>();

    public void add(Row row) {
      rows.add(row);
    }

    TimeOnTimeComparisonResponse build() {
      return new TimeOnTimeComparisonResponse(rows);
    }
  }
}
