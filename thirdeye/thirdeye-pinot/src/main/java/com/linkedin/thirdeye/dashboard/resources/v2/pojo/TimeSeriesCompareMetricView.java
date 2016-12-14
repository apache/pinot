package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

import java.util.List;

/**
 * A time series compare view for given metric
 */
public class TimeSeriesCompareMetricView {
  String metricName;
  long metricId;
  long start;
  long end;

  List<Long> timeBucketsCurrent;
  List<Long> timeBucketsBaseline;

  List<Double> currentValues;
  List<Double> baselineValues;

  public List<Double> getBaselineValues() {
    return baselineValues;
  }

  public void setBaselineValues(List<Double> baselineValues) {
    this.baselineValues = baselineValues;
  }

  public List<Double> getCurrentValues() {
    return currentValues;
  }

  public void setCurrentValues(List<Double> currentValues) {
    this.currentValues = currentValues;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public List<Long> getTimeBucketsBaseline() {
    return timeBucketsBaseline;
  }

  public void setTimeBucketsBaseline(List<Long> timeBucketsBaseline) {
    this.timeBucketsBaseline = timeBucketsBaseline;
  }

  public List<Long> getTimeBucketsCurrent() {
    return timeBucketsCurrent;
  }

  public void setTimeBucketsCurrent(List<Long> timeBucketsCurrent) {
    this.timeBucketsCurrent = timeBucketsCurrent;
  }
}
