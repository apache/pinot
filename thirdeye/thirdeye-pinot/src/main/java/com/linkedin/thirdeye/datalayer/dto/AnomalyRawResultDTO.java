package com.linkedin.thirdeye.datalayer.dto;

public class AnomalyRawResultDTO extends AbstractDTO {

  private Long startTime;

  private Long endTime;

  private String dimensions;

  // significance level
  private double score;

  // severity
  private double weight;

  private String properties;

  private String message;

  private Long createdTime;

  private AnomalyFeedbackDTO feedback;

  private boolean dataMissing;

  private boolean merged;

  private AnomalyFunctionDTO function;

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public Long getFunctionId() {
    return function.getId();
  }

  public String getMetric() {
    return function.getMetric();
  }

  public String getCollection() {
    return function.getCollection();
  }

  public String getFilters() {
    return function.getFilters();
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

}
