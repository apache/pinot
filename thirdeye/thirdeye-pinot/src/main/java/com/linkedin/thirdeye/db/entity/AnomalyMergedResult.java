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
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name = "anomaly_merged_results")
public class AnomalyMergedResult extends AbstractBaseEntity {

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

  @Column(name = "score", nullable = false)
  private double score;

  @Column(name = "weight", nullable = false)
  private double weight;

  @Column(name = "created_time", nullable = false)
  private Long createdTime;

  @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER, orphanRemoval = true)
  @JoinColumn(name="anomaly_feedback_id")
  private AnomalyFeedback feedback;

  @OneToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
  private List<AnomalyResult> anomalyResults = new ArrayList<>();

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

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
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

  @Override
  public int hashCode() {
    return Objects.hash(getId(), startTime, endTime, collection, metric, dimensions, score, weight);
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
}
