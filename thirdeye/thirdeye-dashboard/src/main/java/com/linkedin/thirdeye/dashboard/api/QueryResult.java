package com.linkedin.thirdeye.dashboard.api;

import com.sun.jersey.api.NotFoundException;

import java.util.List;
import java.util.Map;

public class QueryResult {
  private Map<String, Map<String, Number[]>> data;
  private List<String> dimensions;
  private List<String> metrics;

  public QueryResult() {
  }

  public Map<String, Map<String, Number[]>> getData() {
    return data;
  }

  public void setData(Map<String, Map<String, Number[]>> data) {
    this.data = data;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  public QueryResult checkEmpty() throws NotFoundException {
    if (data.isEmpty()) {
      throw new NotFoundException("No dimension combinations in result");
    }

    boolean allEmpty = true;

    for (Map<String, Number[]> series : data.values()) {
      if (!series.isEmpty()) {
        allEmpty = false;
        break;
      }
    }

    if (allEmpty) {
      throw new NotFoundException("No data for any dimension combination");
    }

    return this;
  }
}
