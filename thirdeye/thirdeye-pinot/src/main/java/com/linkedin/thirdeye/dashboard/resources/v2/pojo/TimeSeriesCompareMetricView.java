package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A time series compare view for given metric
 */
public class TimeSeriesCompareMetricView {
  String metricName;
  long metricId;
  long start;
  long end;
  boolean inverseMetric = false;

  List<Long> timeBucketsCurrent;
  List<Long> timeBucketsBaseline;

  Map<String, ValuesContainer> subDimensionContributionMap = new LinkedHashMap<>();

  public TimeSeriesCompareMetricView() {

  }
  public TimeSeriesCompareMetricView(String metricName, long metricId, long start, long end) {
    this(metricName, metricId, start, end, false, null, null);
  }

  public TimeSeriesCompareMetricView(String metricName, long metricId, long start, long end, boolean inverseMetric) {
    this(metricName, metricId, start, end, inverseMetric, null, null);
  }

  public TimeSeriesCompareMetricView(String metricName, long metricId, long start, long end, boolean inverseMetric,
      List<Long> currentTimeBuckets, List<Long> baselineTimeBuckets) {
    this.metricName = metricName;
    this.metricId = metricId;
    this.start = start;
    this.end = end;
    this.inverseMetric = inverseMetric;
    this.timeBucketsCurrent = currentTimeBuckets;
    this.timeBucketsBaseline = baselineTimeBuckets;
  }

  public Map<String, ValuesContainer> getSubDimensionContributionMap() {
    return subDimensionContributionMap;
  }

  public void setSubDimensionContributionMap(
      Map<String, ValuesContainer> subDimensionContributionMap) {
    this.subDimensionContributionMap = subDimensionContributionMap;
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
  public boolean isInverseMetric() {
    return inverseMetric;
  }
  public void setInverseMetric(boolean inverseMetric) {
    this.inverseMetric = inverseMetric;
  }


}
