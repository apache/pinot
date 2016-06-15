package com.linkedin.thirdeye.client.timeseries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;
import org.joda.time.DateTime;

public class TimeSeriesRow implements Comparable<TimeSeriesRow> {
  private final long start;
  private final long end;
  private final String dimensionName;
  private final String dimensionValue;
  private final List<TimeSeriesMetric> metrics;

  private static final String DELIM = "\t\t";

  private TimeSeriesRow(Builder builder) {
    this.start = builder.start.getMillis();
    this.end = builder.end.getMillis();
    this.dimensionName = builder.dimensionName;
    this.dimensionValue = builder.dimensionValue;
    this.metrics = builder.metrics;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public String getDimensionValue() {
    return dimensionValue;
  }

  public List<TimeSeriesMetric> getMetrics() {
    return metrics;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(start).append(DELIM).append(end).append(DELIM).append(dimensionName).append(DELIM)
        .append(dimensionValue);
    for (TimeSeriesMetric metric : metrics) {
      sb.append(DELIM).append(metric.getValue());
    }
    return sb.toString();
  }

  @Override
  public int compareTo(TimeSeriesRow o) {

    int startDiff = ObjectUtils.compare(this.start, o.start);
    if (startDiff != 0)
      return startDiff;

    int dimensionNameDiff = ObjectUtils.compare(this.dimensionName, o.dimensionName);
    if (dimensionNameDiff != 0)
      return dimensionNameDiff;
    int dimensionValueDiff = ObjectUtils.compare(this.dimensionValue, o.dimensionValue);
    if (dimensionValueDiff != 0)
      return dimensionValueDiff;
    List<TimeSeriesMetric> thisMetrics = this.metrics;

    List<TimeSeriesMetric> otherMetrics = o.metrics;
    int thisMetricsSize = thisMetrics.size();
    int otherMetricsSize = otherMetrics.size();
    int metricSizeDiff = ObjectUtils.compare(thisMetricsSize, otherMetricsSize);
    if (metricSizeDiff != 0)
      return metricSizeDiff;
    for (int i = 0; i < thisMetricsSize; i++) {
      TimeSeriesMetric thisMetric = thisMetrics.get(i);
      TimeSeriesMetric otherMetric = otherMetrics.get(i);
      int metricDiff = ObjectUtils.compare(thisMetric, otherMetric);
      if (metricDiff != 0) {
        return metricDiff;
      }
    }
    return 0;

  }

  static class Builder {
    private DateTime start;
    private DateTime end;
    private String dimensionName = "all";
    private String dimensionValue = "all";
    private final List<TimeSeriesMetric> metrics = new ArrayList<>();

    public void setStart(DateTime start) {
      this.start = start;
    }

    public void setEnd(DateTime end) {
      this.end = end;
    }

    public void setDimensionName(String dimensionName) {
      this.dimensionName = dimensionName;
    }

    public void setDimensionValue(String dimensionValue) {
      this.dimensionValue = dimensionValue;
    }

    public void addMetric(String metricName, double value) {
      this.metrics.add(new TimeSeriesMetric(metricName, value));
    }

    public void addMetrics(TimeSeriesMetric... metrics) {
      Collections.addAll(this.metrics, metrics);
    }

    public TimeSeriesRow build() {
      return new TimeSeriesRow(this);
    }

    public void clear() {
      setStart(null);
      setEnd(null);
      setDimensionName(null);
      setDimensionValue(null);
      this.metrics.clear();
    }
  }

  public static class TimeSeriesMetric implements Comparable<TimeSeriesMetric> {
    private final String metricName;
    private final Double value;

    public TimeSeriesMetric(String metricName, Double value) {
      this.metricName = metricName;
      this.value = value;
    }

    @Override
    public int compareTo(TimeSeriesMetric otherMetric) {
      // shouldn't ever need to compare by value
      return this.metricName.compareTo(otherMetric.metricName);
    }

    public String getMetricName() {
      return metricName;
    }

    public Double getValue() {
      return value;
    }

  }
}
