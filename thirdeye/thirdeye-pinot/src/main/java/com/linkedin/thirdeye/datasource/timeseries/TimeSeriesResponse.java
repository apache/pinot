package com.linkedin.thirdeye.datasource.timeseries;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.linkedin.thirdeye.datasource.timeseries.TimeSeriesRow.TimeSeriesMetric;

public class TimeSeriesResponse {
  int numRows;
  private final Set<String> metrics = new TreeSet<>();
  private final Set<List<String>> dimensions = new HashSet<>();
  private final List<TimeSeriesRow> rows;

  public TimeSeriesResponse(List<TimeSeriesRow> rows) {
    this.rows = rows;
    for (TimeSeriesRow row : rows) {
      for (TimeSeriesMetric metric : row.getMetrics()) {
        metrics.add(metric.getMetricName());
      }
      dimensions.add(row.getDimensionNames());
    }
    numRows = rows.size();
  }

  public int getNumRows() {
    return numRows;
  }

  public Set<String> getMetrics() {
    return metrics;
  }

  public Set<List<String>> getDimensions() {
    return dimensions;
  }

  public TimeSeriesRow getRow(int index) {
    return rows.get(index);
  }

  public List<TimeSeriesRow> getRows() {
    return ImmutableList.copyOf(rows);
  }

  public static class Builder {
    List<TimeSeriesRow> rows = new ArrayList<>();

    public void add(TimeSeriesRow row) {
      rows.add(row);
    }

    TimeSeriesResponse build() {
      return new TimeSeriesResponse(rows);
    }
  }
}
