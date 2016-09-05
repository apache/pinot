package com.linkedin.thirdeye.datalayer.pojo;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import java.util.Properties;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.apache.commons.lang.ObjectUtils;
import org.joda.time.DateTime;

import com.google.common.base.MoreObjects;

@Entity
@Table(name = "anomaly_results")
public class RawAnomalyResultBean extends AbstractBean implements Comparable<RawAnomalyResultBean> {

  private static Joiner SEMICOLON = Joiner.on(";");
  private static Joiner EQUALS = Joiner.on("=");

  @Column(name = "start_time_utc", nullable = false)
  private Long startTimeUtc;

  @Column(name = "end_time_utc", nullable = true)
  private Long endTimeUtc;

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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("startTimeUtc", startTimeUtc)
        .add("dimensions", dimensions).add("endTimeUtc", endTimeUtc).add("score", score)
        .add("weight", weight).add("properties", properties).add("message", message)
        .add("creationTimeUtc", creationTimeUtc).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RawAnomalyResultBean)) {
      return false;
    }
    RawAnomalyResultBean r = (RawAnomalyResultBean) o;
    return Objects.equals(getId(), r.getId()) && Objects.equals(startTimeUtc, r.getStartTimeUtc())
        && Objects.equals(dimensions, r.getDimensions())
        && Objects.equals(endTimeUtc, r.getEndTimeUtc()) && Objects.equals(score, r.getScore())
        && Objects.equals(weight, r.getWeight()) && Objects.equals(properties, r.getProperties())
        && Objects.equals(message, r.getMessage());
    // Intentionally omit creationTimeUtc, since start/end are the truly significant dates for
    // anomalies
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), dimensions, startTimeUtc, endTimeUtc, score, weight, properties,
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
    diff = -ObjectUtils.compare(startTimeUtc, o.getStartTimeUtc()); // inverted to sort by
    // decreasing time
    if (diff != 0) {
      return diff;
    }
    return ObjectUtils.compare(getId(), o.getId());
  }

  public static String encodeCompactedProperties(Properties props) {
    List<String> parts = new ArrayList<>();
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      parts.add(EQUALS.join(entry.getKey(), entry.getValue()));
    }
    return SEMICOLON.join(parts);
  }
}
