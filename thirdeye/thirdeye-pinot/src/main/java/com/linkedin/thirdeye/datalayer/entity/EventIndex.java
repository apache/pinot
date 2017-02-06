package com.linkedin.thirdeye.datalayer.entity;

import com.linkedin.thirdeye.anomaly.events.EventType;

public class EventIndex extends AbstractIndexEntity {
  String name;
  EventType eventType;
  long startTime;
  long endTime;
  String metricName;
  String serviceName;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
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

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
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
