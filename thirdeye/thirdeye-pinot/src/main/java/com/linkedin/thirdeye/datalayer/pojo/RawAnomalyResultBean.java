package com.linkedin.thirdeye.datalayer.pojo;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;

import org.apache.commons.lang.ObjectUtils;
import org.joda.time.DateTime;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;

@MappedSuperclass
public class RawAnomalyResultBean extends AbstractBean implements Comparable<RawAnomalyResultBean> {

  @Transient
  private Long functionId;

  @Transient
  private Long AnomalyFeedbackId;

  @Column(name = "start_time_utc", nullable = false)
  private Long startTime;

  @Column(name = "end_time_utc", nullable = true)
  private Long endTime;

  @Column(name = "dimensions", nullable = false)
  private String dimensions;

  // significance level
  @Column(name = "score", nullable = false)
  private double score;

  // severity
  @Column(name = "weight", nullable = false)
  private double weight;

  @Column(name = "properties", nullable = true)
  private String properties;

  @Column(name = "message", nullable = true)
  private String message;

  @Column(name = "creation_time_utc", nullable = false)
  private Long creationTimeUtc;

  @Column(name = "data_missing")
  private boolean dataMissing;

  @Column
  private boolean merged;

  public RawAnomalyResultBean() {
    creationTimeUtc = DateTime.now().getMillis();
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTimeUtc) {
    this.startTime = startTimeUtc;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTimeUtc) {
    this.endTime = endTimeUtc;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public String getProperties() {
    return properties;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Long getCreationTimeUtc() {
    return creationTimeUtc;
  }

  public void setCreationTimeUtc(Long creationTimeUtc) {
    this.creationTimeUtc = creationTimeUtc;
  }


  public boolean isDataMissing() {
    return dataMissing;
  }

  public void setDataMissing(boolean dataMissing) {
    this.dataMissing = dataMissing;
  }

  public boolean isMerged() {
    return merged;
  }

  public void setMerged(boolean merged) {
    this.merged = merged;
  }

  public Long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(Long functionId) {
    this.functionId = functionId;
  }

  public Long getAnomalyFeedbackId() {
    return AnomalyFeedbackId;
  }

  public void setAnomalyFeedbackId(Long anomalyFeedbackId) {
    AnomalyFeedbackId = anomalyFeedbackId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("startTimeUtc", startTime)
        .add("dimensions", dimensions).add("endTimeUtc", endTime).add("score", score)
        .add("weight", weight).add("properties", properties).add("message", message)
        .add("creationTimeUtc", creationTimeUtc).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RawAnomalyResultBean)) {
      return false;
    }
    RawAnomalyResultBean r = (RawAnomalyResultBean) o;
    return Objects.equals(getId(), r.getId()) && Objects.equals(startTime, r.getStartTime())
        && Objects.equals(dimensions, r.getDimensions()) && Objects.equals(endTime, r.getEndTime())
        && Objects.equals(score, r.getScore()) && Objects.equals(weight, r.getWeight())
        && Objects.equals(properties, r.getProperties()) && Objects.equals(message, r.getMessage());
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), dimensions, startTime, endTime, score, weight, properties,
        message);
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

  @Override
  public int compareTo(RawAnomalyResultBean o) {
    // compare by dimension, -startTime, functionId, id
    int diff = ObjectUtils.compare(getDimensions(), o.getDimensions());
    if (diff != 0) {
      return diff;
    }
    diff = -ObjectUtils.compare(startTime, o.getStartTime()); // inverted to sort by
    // decreasing time
    if (diff != 0) {
      return diff;
    }
    return ObjectUtils.compare(getId(), o.getId());
  }
}
