package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.CacheUtils;


public class TimeSeriesDataPoint {

  private String metricUrn;
  private long timestamp;
  private long metricId;
  private String dataValue;

  public TimeSeriesDataPoint(String metricUrn, long timestamp, long metricId, String dataValue) {
    this.metricUrn = metricUrn;
    this.timestamp = timestamp;
    this.metricId = metricId;
    this.dataValue = dataValue;
  }

  public String getMetricUrn() { return metricUrn; }
  public long getTimestamp() { return timestamp; }
  public long getMetricId() { return metricId; }
  public String getDataValue() { return dataValue; }

  public void setMetricUrn(String metricUrn) {
    this.metricUrn = metricUrn;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public void setDataValue(String dataValue) {
    this.dataValue = dataValue;
  }

  public String getDocumentKey() {
    return metricId + "_" + timestamp;
  }

  public String getMetricUrnHash() {
    return CacheUtils.hashMetricUrn(metricUrn);
  }

  public static TimeSeriesDataPoint from(String[] dataPoint, String metricUrn) {
    long timestamp = Long.valueOf(dataPoint[dataPoint.length - 1]);
    long metricId = MetricEntity.fromURN(metricUrn).getId();
    String dataValue = dataPoint[1];
    return new TimeSeriesDataPoint(metricUrn, timestamp, metricId, dataValue);
  }

  public static TimeSeriesDataPoint from(String[] dataPoint, long metricId, String metricUrn) {
    long timestamp = Long.valueOf(dataPoint[dataPoint.length - 1]);
    String dataValue = dataPoint[1];
    return new TimeSeriesDataPoint(metricUrn, timestamp, metricId, dataValue);
  }
}
