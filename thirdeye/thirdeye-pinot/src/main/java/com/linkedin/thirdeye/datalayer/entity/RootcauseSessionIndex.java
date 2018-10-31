package com.linkedin.thirdeye.datalayer.entity;

public class RootcauseSessionIndex extends AbstractIndexEntity {
  private String name;
  private String owner;
  private Long previousId;
  private Long anomalyRangeStart;
  private Long anomalyRangeEnd;
  private Long created;
  private Long updated;
  private Long anomalyId;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public Long getPreviousId() {
    return previousId;
  }

  public void setPreviousId(Long previousId) {
    this.previousId = previousId;
  }

  public Long getAnomalyRangeStart() {
    return anomalyRangeStart;
  }

  public void setAnomalyRangeStart(Long anomalyRangeStart) {
    this.anomalyRangeStart = anomalyRangeStart;
  }

  public Long getAnomalyRangeEnd() {
    return anomalyRangeEnd;
  }

  public void setAnomalyRangeEnd(Long anomalyRangeEnd) {
    this.anomalyRangeEnd = anomalyRangeEnd;
  }

  public Long getCreated() {
    return created;
  }

  public void setCreated(Long created) {
    this.created = created;
  }

  public Long getUpdated() {
    return updated;
  }

  public void setUpdated(Long updated) {
    this.updated = updated;
  }

  public Long getAnomalyId() {
    return anomalyId;
  }

  public void setAnomalyId(Long anomalyId) {
    this.anomalyId = anomalyId;
  }
}
