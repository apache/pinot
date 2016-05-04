package com.linkedin.thirdeye.detector.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.apache.commons.lang.ObjectUtils;
import org.joda.time.DateTime;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;

@Entity
@Table(name = "anomaly_results")
@NamedQueries({
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyResult#findAll", query = "SELECT r FROM AnomalyResult r"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollection", query = "SELECT r FROM AnomalyResult r WHERE r.collection = :collection"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionAndTime", query = "SELECT r FROM AnomalyResult r WHERE r.collection = :collection "
        + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
        + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeAndFunction", query = "SELECT r FROM AnomalyResult r WHERE r.collection = :collection "
        + "AND r.functionId = :functionId "
        + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
        + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeAndMetric", query = "SELECT r FROM AnomalyResult r WHERE r.collection = :collection "
        + "AND r.metric = :metric "
        + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
        + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeFunctionIdAndMetric", query = "SELECT r FROM AnomalyResult r WHERE r.collection = :collection "
        + "AND r.functionId = :functionId " + "AND r.metric = :metric "
        + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
        + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))"),
    @NamedQuery(name = "com.linkedin.thirdeye.api.AnomalyResult#findAllByCollectionTimeMetricAndFilters", query = "SELECT r FROM AnomalyResult r WHERE r.collection = :collection "
        + "AND r.metric = :metric "
        + "AND (r.filters = :filters or (r.filters is NULL and :filters is NULL))"
        + "AND ((r.startTimeUtc >= :startTimeUtc AND r.startTimeUtc <= :endTimeUtc) "
        + "OR (r.endTimeUtc >= :startTimeUtc AND r.endTimeUtc <= :endTimeUtc))"),
})
public class AnomalyResult implements Comparable<AnomalyResult> {
  private static Joiner SEMICOLON = Joiner.on(";");
  private static Joiner EQUALS = Joiner.on("=");

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

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

  public AnomalyResult() {
    creationTimeUtc = DateTime.now().getMillis();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
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
    this.filters = filters;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", id).add("functionId", functionId)
        .add("functionType", functionType).add("functionProperties", functionProperties)
        .add("collection", collection).add("startTimeUtc", startTimeUtc)
        .add("endTimeUtc", endTimeUtc).add("dimensions", dimensions).add("metric", metric)
        .add("score", score).add("weight", weight).add("properties", properties)
        .add("message", message).add("creationTimeUtc", creationTimeUtc).add("filters", filters)
        .toString();
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
    return ObjectUtils.compare(id, o.getId());
  }

  public static String encodeCompactedProperties(Properties props) {
    List<String> parts = new ArrayList<String>();
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      parts.add(EQUALS.join(entry.getKey(), entry.getValue()));
    }
    return SEMICOLON.join(parts);
  }
}
