package com.linkedin.thirdeye.db.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MergedAnomalyResult extends AbstractBaseEntity {

  private String metric;

  private String dimensions;

  private Long startTime;

  private Long endTime;

  private double score;

  private double weight;

  private String properties;

  private String message;

  private Long creationTime;

  private AnomalyFeedback feedback;

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

  public Long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(Long creationTime) {
    this.creationTime = creationTime;
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

  @Override
  public int hashCode() {
    return Objects
        .hash(getId(), startTime, endTime, dimensions, score, weight, properties, dimensions);
  }

  @Override
  public boolean equals (Object o) {
      if (!(o instanceof MergedAnomalyResult)) {
        return false;
      }
    MergedAnomalyResult m = (MergedAnomalyResult) o;
      return Objects.equals(getId(), m.getId())
          && Objects.equals(startTime, m.getStartTime())
          && Objects.equals(endTime, m.getEndTime())
          && Objects.equals(dimensions, m.getDimensions());
  }
}
