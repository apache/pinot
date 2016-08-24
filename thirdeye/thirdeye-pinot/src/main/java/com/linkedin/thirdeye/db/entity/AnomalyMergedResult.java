package com.linkedin.thirdeye.db.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import org.apache.commons.lang.ObjectUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "anomaly_merged_results")
public class AnomalyMergedResult extends AbstractBaseEntity implements Comparable<AnomalyMergedResult> {

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

  @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER, orphanRemoval = true)
  @JoinColumn(name = "anomaly_feedback_id")
  private AnomalyFeedback feedback;

  @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
  @JoinTable(name = "anomaly_merged_results_mapping", joinColumns = @JoinColumn(name = "anomaly_merged_result_id"),
      inverseJoinColumns = @JoinColumn(name = "anomaly_result_id"))
  private List<AnomalyResult> anomalyResults = new ArrayList<>();

  @ManyToOne(fetch = FetchType.EAGER)
  @JoinColumn(name = "function_id")
  private AnomalyFunctionSpec function;

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

  public AnomalyFeedback getFeedback() {
    return feedback;
  }

  public void setFeedback(AnomalyFeedback feedback) {
    this.feedback = feedback;
  }

  public List<AnomalyResult> getAnomalyResults() {
    return anomalyResults;
  }

  public void setAnomalyResults(List<AnomalyResult> anomalyResults) {
    this.anomalyResults = anomalyResults;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public AnomalyFunctionSpec getFunction() {
    return function;
  }

  public void setFunction(AnomalyFunctionSpec function) {
    this.function = function;
  }

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
  public boolean equals (Object o) {
      if (!(o instanceof AnomalyMergedResult)) {
        return false;
      }
    AnomalyMergedResult m = (AnomalyMergedResult) o;
    return Objects.equals(getId(), m.getId()) && Objects.equals(startTime, m.getStartTime())
        && Objects.equals(endTime, m.getEndTime()) && Objects.equals(collection, m.getCollection())
        && Objects.equals(metric, m.getMetric()) && Objects.equals(dimensions, m.getDimensions());
  }

  @Override
  public int compareTo(AnomalyMergedResult o) {
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
    diff = ObjectUtils.compare(getFunction().getId(), o.getFunction().getId());
    if (diff != 0) {
      return diff;
    }
    return ObjectUtils.compare(getId(), o.getId());
  }
}
