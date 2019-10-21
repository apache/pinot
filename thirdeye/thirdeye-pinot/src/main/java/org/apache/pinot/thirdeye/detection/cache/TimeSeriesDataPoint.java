package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.CacheUtils;


public class TimeSeriesDataPoint {

  private String metricUrn;
  private String timestamp;
  private String metricId;
  private String dataValue;

  public TimeSeriesDataPoint(String metricUrn, String timestamp, String metricId, String dataValue) {
    this.metricUrn = metricUrn;
    this.timestamp = timestamp;
    this.metricId = metricId;
    this.dataValue = dataValue;
  }

  public String getMetricUrn() { return metricUrn; }
  public String getTimestamp() { return timestamp; }
  public String getMetricId() { return metricId; }
  public String getDataValue() { return dataValue; }

  public String getDocumentKey() {
    return metricId + "_" + timestamp;
  }

  public String getDimensionKey() {
    return CacheUtils.hashMetricUrn(metricUrn);
  }

  public static TimeSeriesDataPoint from(String[] dataPoint, String metricUrn) {
    String timestamp = dataPoint[dataPoint.length - 1];
    String metricId = String.valueOf(MetricEntity.fromURN(metricUrn).getId());
    String dataValue = dataPoint[1];
    return new TimeSeriesDataPoint(metricUrn, timestamp, metricId, dataValue);
  }
}
