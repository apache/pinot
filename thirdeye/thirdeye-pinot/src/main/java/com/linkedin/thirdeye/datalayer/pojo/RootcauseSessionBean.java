package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;


/**
 * RootcauseSessionBean holds information for stored rootcause investigation reports. Each session is immutable once
 * stored. Any modifications are stored in a new session with a backpointer to the previous version.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RootcauseSessionBean extends AbstractBean {
  private String name;
  private String text;
  private String owner;
  private Long previousId;
  private Long anomalyRangeStart;
  private Long anomalyRangeEnd;
  private Long baselineRangeStart;
  private Long baselineRangeEnd;
  private Long created;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
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

  public Long getBaselineRangeStart() {
    return baselineRangeStart;
  }

  public void setBaselineRangeStart(Long baselineRangeStart) {
    this.baselineRangeStart = baselineRangeStart;
  }

  public Long getBaselineRangeEnd() {
    return baselineRangeEnd;
  }

  public void setBaselineRangeEnd(Long baselineRangeEnd) {
    this.baselineRangeEnd = baselineRangeEnd;
  }

  public Long getCreated() {
    return created;
  }

  public void setCreated(Long created) {
    this.created = created;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RootcauseSessionBean)) {
      return false;
    }
    RootcauseSessionBean that = (RootcauseSessionBean) o;
    return Objects.equals(name, that.name) && Objects.equals(text, that.text) && Objects.equals(owner, that.owner)
        && Objects.equals(previousId, that.previousId) && Objects.equals(anomalyRangeStart, that.anomalyRangeStart)
        && Objects.equals(anomalyRangeEnd, that.anomalyRangeEnd) && Objects.equals(baselineRangeStart,
        that.baselineRangeStart) && Objects.equals(baselineRangeEnd, that.baselineRangeEnd) && Objects.equals(created,
        that.created);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, text, owner, previousId, anomalyRangeStart, anomalyRangeEnd, baselineRangeStart,
        baselineRangeEnd, created);
  }
}
