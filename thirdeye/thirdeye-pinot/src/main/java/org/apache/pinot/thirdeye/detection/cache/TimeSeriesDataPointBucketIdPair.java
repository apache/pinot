package org.apache.pinot.thirdeye.detection.cache;

public class TimeSeriesDataPointBucketIdPair {
  TimeSeriesDataPoint dataPoint;
  int timeBucketId;

  public TimeSeriesDataPointBucketIdPair(TimeSeriesDataPoint dataPoint, int timeBucketId) {
    this.dataPoint = dataPoint;
    this.timeBucketId = timeBucketId;
  }

  public TimeSeriesDataPoint getDataPoint() {
    return dataPoint;
  }

  public int getTimeBucketId() {
    return timeBucketId;
  }
}
