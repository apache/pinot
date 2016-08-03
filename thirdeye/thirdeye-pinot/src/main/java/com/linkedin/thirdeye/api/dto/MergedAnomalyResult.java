package com.linkedin.thirdeye.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeStrategy;
import com.linkedin.thirdeye.db.entity.AnomalyFeedback;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MergedAnomalyResult {
  private Long startTime;

  private Long endTime;

  private String dimensions;

  private double score;

  private double weight;

  private String properties;

  private String message;

  private Long creationTime;

  private AnomalyFeedback feedback;

  private AnomalyFunctionSpec function;

  private AnomalyMergeStrategy mergeStrategy;

  private List<AnomalyResult> anomalyResults = new ArrayList<>();

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

  public AnomalyFunctionSpec getFunction() {
    return function;
  }

  public void setFunction(AnomalyFunctionSpec function) {
    this.function = function;
  }

  public AnomalyMergeStrategy getMergeStrategy() {
    return mergeStrategy;
  }

  public void setMergeStrategy(AnomalyMergeStrategy mergeStrategy) {
    this.mergeStrategy = mergeStrategy;
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
        .hash(startTime, endTime, dimensions, score, weight, properties, function, dimensions,
            mergeStrategy);
  }

  @Override
  public boolean equals (Object o) {
      if (!(o instanceof MergedAnomalyResult)) {
        return false;
      }
    MergedAnomalyResult m = (MergedAnomalyResult) o;
      return Objects.equals(startTime, m.getStartTime())
          && Objects.equals(endTime, m.getEndTime())
          && Objects.equals(function, m.getFunction())
          && Objects.equals(mergeStrategy, m.getMergeStrategy())
          && Objects.equals(dimensions, m.getDimensions());
  }
}
