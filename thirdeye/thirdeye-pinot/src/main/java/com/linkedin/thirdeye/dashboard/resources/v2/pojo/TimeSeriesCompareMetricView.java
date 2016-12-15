package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

import java.util.ArrayList;
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

  List<Long> timeBucketsCurrent;
  List<Long> timeBucketsBaseline;

  ValuesWrapper overAllValues;

  Map<String, ValuesWrapper> subDimensionContributionMap = new LinkedHashMap<>();

  public TimeSeriesCompareMetricView() {

  }
  public TimeSeriesCompareMetricView(String metricName, long metricId, long start, long end) {
    this(metricName, metricId, start, end, null, null);
  }

  public TimeSeriesCompareMetricView(String metricName, long metricId, long start, long end,
      List<Long> currentTimeBuckets, List<Long> baselineTimeBuckets) {
    this.metricName = metricName;
    this.metricId = metricId;
    this.start = start;
    this.end = end;
    this.timeBucketsCurrent = currentTimeBuckets;
    this.timeBucketsBaseline = baselineTimeBuckets;
  }

  public Map<String, ValuesWrapper> getSubDimensionContributionMap() {
    return subDimensionContributionMap;
  }

  public void setSubDimensionContributionMap(
      Map<String, ValuesWrapper> subDimensionContributionMap) {
    this.subDimensionContributionMap = subDimensionContributionMap;
  }

  public ValuesWrapper getOverAllValues() {
    return overAllValues;
  }

  public void setOverAllValues(ValuesWrapper values) {
    this.overAllValues = values;
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

  public static class ValuesWrapper {

    List<Double> currentValues = new ArrayList<>();
    List<Double> baselineValues = new ArrayList<>();

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
  }

}
