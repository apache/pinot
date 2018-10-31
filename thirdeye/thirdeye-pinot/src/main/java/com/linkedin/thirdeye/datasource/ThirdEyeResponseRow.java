package com.linkedin.thirdeye.datasource;

import java.util.List;

public class ThirdEyeResponseRow {

  int timeBucketId;
  List<String> dimensions;
  List<Double> metrics;

  public ThirdEyeResponseRow(int timeBucketId, List<String> dimensions, List<Double> metrics) {
    super();
    this.timeBucketId = timeBucketId;
    this.dimensions = dimensions;
    this.metrics = metrics;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public List<Double> getMetrics() {
    return metrics;
  }
  
  public int getTimeBucketId() {
    return timeBucketId;
  }

}
