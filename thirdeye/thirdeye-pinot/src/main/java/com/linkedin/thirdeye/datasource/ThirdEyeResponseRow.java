package com.linkedin.thirdeye.datasource;

import com.google.common.base.Preconditions;
import java.util.List;

public class ThirdEyeResponseRow {

  private long timestamp;
  private List<String> dimensions;
  private List<Double> metrics;

  public ThirdEyeResponseRow(long timestamp, List<String> dimensions, List<Double> metrics) {
    Preconditions.checkNotNull(dimensions);
    Preconditions.checkNotNull(metrics);

    this.timestamp = timestamp;
    this.dimensions = dimensions;
    this.metrics = metrics;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public List<Double> getMetrics() {
    return metrics;
  }

}
