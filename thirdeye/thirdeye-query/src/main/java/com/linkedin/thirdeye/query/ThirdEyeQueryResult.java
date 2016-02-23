package com.linkedin.thirdeye.query;

import com.google.common.base.Objects;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThirdEyeQueryResult {
  private List<String> dimensions;
  private List<String> metrics;
  private Map<DimensionKey, MetricTimeSeries> data = new HashMap<>();

  protected void addData(DimensionKey dimensionKey, MetricTimeSeries timeSeries) {
    MetricTimeSeries existing = data.get(dimensionKey);
    if (existing == null) {
      existing = new MetricTimeSeries(timeSeries.getSchema());
      data.put(dimensionKey, existing);
    }
    existing.aggregate(timeSeries);
  }

  public Map<DimensionKey, MetricTimeSeries> getData() {
    return data;
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

  @Override
  public String toString() {
    return Objects.toStringHelper(ThirdEyeQueryResult.class).add("dimensions", dimensions)
        .add("metrics", metrics).add("data", data).toString();
  }
}
