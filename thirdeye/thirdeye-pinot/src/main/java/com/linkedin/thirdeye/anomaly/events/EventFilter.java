package com.linkedin.thirdeye.anomaly.events;

import java.util.List;
import java.util.Map;

public class EventFilter {
  
  EventType eventType;
  String serviceName;
  long startTime;
  long endTime;
  Map<String, List<String>> dimensionValuesMap;

  public Map<String, List<String>> getDimensionValuesMap() {
    return dimensionValuesMap;
  }

  public void setDimensionValuesMap(Map<String, List<String>> dimensionValuesMap) {
    this.dimensionValuesMap = dimensionValuesMap;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public EventType getEventType() {
    return eventType;
  }

  public void setEventType(EventType eventType) {
    this.eventType = eventType;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
}
