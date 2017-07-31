package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

public class RootCauseEventEntity extends RootCauseEntity {
  long start;
  long end;
  String eventType;
  String details;

  public RootCauseEventEntity() {
    // left blank
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public String getDetails() {
    return details;
  }

  public void setDetails(String details) {
    this.details = details;
  }

}
