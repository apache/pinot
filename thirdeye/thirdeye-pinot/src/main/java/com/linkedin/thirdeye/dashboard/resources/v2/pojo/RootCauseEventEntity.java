package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

import java.util.ArrayList;
import java.util.List;


public class RootCauseEventEntity extends RootCauseEntity {
  long start;
  long end;
  String eventType;
  String details;
  List<RootCauseEntity> relatedEntities = new ArrayList<>();

  public RootCauseEventEntity() {
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

  public List<RootCauseEntity> getRelatedEntities() {
    return relatedEntities;
  }

  public void setRelatedEntities(List<RootCauseEntity> relatedEntities) {
    this.relatedEntities = relatedEntities;
  }

  public void addRelatedEntity(RootCauseEntity e) {
    this.relatedEntities.add(e);
  }

}
