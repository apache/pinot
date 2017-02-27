package com.linkedin.thirdeye.datalayer.pojo;

import com.linkedin.thirdeye.api.DimensionMap;
import java.util.Objects;

import org.apache.commons.lang.ObjectUtils;
import org.joda.time.DateTime;

import com.google.common.base.MoreObjects;


public class RawAnomalyResultBean extends AbstractBean implements Comparable<RawAnomalyResultBean> {

  private Long functionId;
  private Long AnomalyFeedbackId;
  private Long startTime;
  private Long endTime;
  private DimensionMap dimensions;

  // significance level
  private double score;

  // severity
  private double weight;
  private String properties;
  private String message;
  private Long creationTimeUtc;
  private boolean dataMissing;
  private boolean merged;
  private double avgCurrentVal;
  private double avgBaselineVal;

  // display metric, anomaly function ex
  private String displayMetric;
  private String displayCollection;

  public double getAvgCurrentVal(){
    return this.avgCurrentVal;
  }

  public double getAvgBaselineVal(){
    return this.avgBaselineVal;
  }

  public void setAvgCurrentVal(double val){
    this.avgCurrentVal = val;
  }

  public void setAvgBaselineVal(double val){
    this.avgBaselineVal = val;
  }

  public RawAnomalyResultBean() {
    creationTimeUtc = DateTime.now().getMillis();
  }

  public DimensionMap getDimensions() {
    return dimensions;
  }

  public void setDimensions(DimensionMap dimensions) {
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

  public String getDisplayMetric() {
    return displayMetric;
  }

  public void setDisplayMetric(String displayMetric) {
    this.displayMetric = displayMetric;
  }

  public String getDisplayCollection() {
    return displayCollection;
  }

  public void setDisplayCollection(String displayCollection) {
    this.displayCollection = displayCollection;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("startTimeUtc", startTime)
        .add("dimensions", dimensions).add("endTimeUtc", endTime).add("score", score)
        .add("weight", weight).add("properties", properties).add("message", message)
        .add("creationTimeUtc", creationTimeUtc)
        .add("currentVal", avgCurrentVal).add("baseLineVal", avgBaselineVal).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RawAnomalyResultBean)) {
      return false;
    }
    RawAnomalyResultBean r = (RawAnomalyResultBean) o;
    return Objects.equals(startTime, r.getStartTime())
        && Objects.equals(dimensions, r.getDimensions()) && Objects.equals(endTime, r.getEndTime())
        && Objects.equals(score, r.getScore()) && Objects.equals(weight, r.getWeight())
        && Objects.equals(properties, r.getProperties()) && Objects.equals(message, r.getMessage())
        && Objects.equals(avgBaselineVal, r.getAvgBaselineVal()) && Objects.equals(avgCurrentVal, r.getAvgCurrentVal());
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

  @Override
  public int hashCode() {
    return Objects.hash(dimensions, startTime, endTime, score, weight, properties,
        message, avgBaselineVal, avgCurrentVal);
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
