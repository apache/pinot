package com.linkedin.thirdeye.datalayer.pojo;

import java.util.List;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;

import org.apache.commons.lang.ObjectUtils;

@MappedSuperclass
public class MergedAnomalyResultBean extends AbstractBean
    implements Comparable<MergedAnomalyResultBean> {

  @Transient
  private Long functionId;

  @Transient
  private Long anomalyFeedbackId;

  @Transient
  private List<Long> rawAnomalyIdList;

  @Column(name = "collection")
  private String collection;

  @Column(name = "metric")
  private String metric;

  @Column(name = "dimensions")
  private String dimensions;

  @Column(name = "start_time", nullable = false)
  private Long startTime;

  @Column(name = "end_time", nullable = false)
  private Long endTime;

  // significance level
  @Column(name = "score", nullable = false)
  private double score;

  // severity
  @Column(name = "weight", nullable = false)
  private double weight;

  @Column(name = "created_time", nullable = false)
  private Long createdTime;

  @Column(name = "message")
  private String message;

  @Column(name = "notified")
  private boolean notified;

  
  public Long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(Long functionId) {
    this.functionId = functionId;
  }

  public Long getAnomalyFeedbackId() {
    return anomalyFeedbackId;
  }

  public void setAnomalyFeedbackId(Long anomalyFeedbackId) {
    this.anomalyFeedbackId = anomalyFeedbackId;
  }

  public List<Long> getRawAnomalyIdList() {
    return rawAnomalyIdList;
  }

  public void setRawAnomalyIdList(List<Long> rawAnomalyIdList) {
    this.rawAnomalyIdList = rawAnomalyIdList;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }

  public Long getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(Long createdTime) {
    this.createdTime = createdTime;
  }

  public boolean isNotified() {
    return notified;
  }

  public void setNotified(boolean notified) {
    this.notified = notified;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  /**
   * Weight is Severity
   * @return
   */
  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), startTime, endTime, collection, metric, dimensions, score);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MergedAnomalyResultBean)) {
      return false;
    }
    MergedAnomalyResultBean m = (MergedAnomalyResultBean) o;
    return Objects.equals(getId(), m.getId()) && Objects.equals(startTime, m.getStartTime())
        && Objects.equals(endTime, m.getEndTime()) && Objects.equals(collection, m.getCollection())
        && Objects.equals(metric, m.getMetric()) && Objects.equals(dimensions, m.getDimensions());
  }

  @Override
  public int compareTo(MergedAnomalyResultBean o) {
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
