package com.linkedin.thirdeye.anomaly.events;

import java.util.List;
import java.util.Map;

public abstract class Event {
  String metric;
  String service;

  long startTime;
  long endTime;

  Map<String, List<String>> targetDimensionMap;

  public String getMetric() {
    return metric;
  }

  public Map<String, List<String>> getTargetDimensionMap() {
    return targetDimensionMap;
  }

  public void setTargetDimensionMap(Map<String, List<String>> targetDimensionMap) {
    this.targetDimensionMap = targetDimensionMap;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getService() {
    return service;
  }

  public void setService(String service) {
    this.service = service;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
}
