package com.linkedin.thirdeye.db.entity;
import java.util.Objects;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.apache.commons.lang.ObjectUtils;
import org.joda.time.DateTime;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

@Entity
@Table(name = "anomaly_results")
public class AnomalyResult extends AbstractBaseEntity implements Comparable<AnomalyResult> {

  // TODO : User AnomalyFunctionSpec instead of id
  @Column(name = "function_id", nullable = false)
  private long functionId;

  @Column(name = "function_type", nullable = false)
  private String functionType;

  @Column(name = "function_properties", nullable = false)
  private String functionProperties;

  @Column(name = "collection", nullable = false)
  private String collection;

  @Column(name = "start_time_utc", nullable = false)
  private Long startTimeUtc;

  @Column(name = "end_time_utc", nullable = true)
  private Long endTimeUtc;

  @Column(name = "dimensions", nullable = false)
  private String dimensions;

  @Column(name = "metric", nullable = false)
  private String metric;

  @Column(name = "score", nullable = false)
  private double score;

  @Column(name = "weight", nullable = false)
  private double weight;

  @Column(name = "properties", nullable = true)
  private String properties;

  @Column(name = "message", nullable = true)
  private String message;

  @Column(name = "creation_time_utc", nullable = false)
  private Long creationTimeUtc;

  @Column(name = "filters", nullable = true)
  private String filters;

  @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER, orphanRemoval = true)
  @JoinColumn(name="anomaly_feedback_id")
  private AnomalyFeedback feedback;

  public AnomalyResult() {
    creationTimeUtc = DateTime.now().getMillis();
  }

  public long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(long functionId) {
    this.functionId = functionId;
  }

  public String getFunctionType() {
    return functionType;
  }

  public void setFunctionType(String functionType) {
    this.functionType = functionType;
  }

  public String getFunctionProperties() {
    return functionProperties;
  }

  public void setFunctionProperties(String functionProperties) {
    this.functionProperties = functionProperties;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public Long getStartTimeUtc() {
    return startTimeUtc;
  }

  public void setStartTimeUtc(Long startTimeUtc) {
    this.startTimeUtc = startTimeUtc;
  }

  public Long getEndTimeUtc() {
    return endTimeUtc;
  }

  public void setEndTimeUtc(Long endTimeUtc) {
    this.endTimeUtc = endTimeUtc;
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
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

  public String getFilters() {
    return filters;
  }

  public void setFilters(String filters) {
    String sortedFilters = ThirdEyeUtils.getSortedFilters(filters);
    this.filters = sortedFilters;
  }

  public AnomalyFeedback getFeedback() {
    return feedback;
  }

  public void setFeedback(AnomalyFeedback feedback) {
    this.feedback = feedback;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("functionId", functionId)
        .add("functionType", functionType).add("functionProperties", functionProperties)
        .add("collection", collection).add("startTimeUtc", startTimeUtc)
        .add("endTimeUtc", endTimeUtc).add("dimensions", dimensions).add("metric", metric)
        .add("score", score).add("weight", weight).add("properties", properties)
        .add("message", message).add("creationTimeUtc", creationTimeUtc).add("filters", filters)
        .add("feedback", feedback).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AnomalyResult)) {
      return false;
    }
    AnomalyResult r = (AnomalyResult) o;
    return Objects.equals(functionId, r.getFunctionId())
        && Objects.equals(functionType, r.getFunctionType())
        && Objects.equals(functionProperties, r.getFunctionProperties())
        && Objects.equals(collection, r.getCollection())
        && Objects.equals(startTimeUtc, r.getStartTimeUtc())
        && Objects.equals(endTimeUtc, r.getEndTimeUtc())
        && Objects.equals(dimensions, r.getDimensions()) && Objects.equals(metric, r.getMetric())
        && Objects.equals(score, r.getScore()) && Objects.equals(weight, r.getWeight())
        && Objects.equals(properties, r.getProperties()) && Objects.equals(message, r.getMessage())
        && Objects.equals(filters, r.getFilters());
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

  @Override
  public int hashCode() {
    return Objects.hash(functionId, functionType, functionProperties, collection, startTimeUtc,
        endTimeUtc, dimensions, metric, score, weight, properties, message, filters);
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

  @Override
  public int compareTo(AnomalyResult o) {
    // compare by dimension, -startTime, functionId, id
    int diff = ObjectUtils.compare(dimensions, o.getDimensions());
    if (diff != 0) {
      return diff;
    }
    diff = -ObjectUtils.compare(startTimeUtc, o.getStartTimeUtc()); // inverted to sort by
    // decreasing time
    if (diff != 0) {
      return diff;
    }
    diff = ObjectUtils.compare(functionId, o.getFunctionId());
    if (diff != 0) {
      return diff;
    }
    return ObjectUtils.compare(getId(), o.getId());
  }
}
