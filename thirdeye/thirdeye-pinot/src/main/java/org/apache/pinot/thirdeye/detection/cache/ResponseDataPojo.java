package org.apache.pinot.thirdeye.detection.cache;

public class ResponseDataPojo {
  String time;
  String value;
  String metricName;

  public ResponseDataPojo(String time, String value, String metricName) {
    this.time = time;
    this.value = value;
    this.metricName = metricName;
  }

  public String getTime() { return time; }
  public String getValue() { return value; }
  public String getMetricName() { return metricName; }
}
