package com.linkedin.thirdeye.api;

import com.google.common.base.MoreObjects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Entity
@Table(name = "anomaly_functions")
@NamedQueries({
    @NamedQuery(
        name = "com.linkedin.thirdeye.api.AnomalyFunctionSpec#findAll",
        query = "SELECT af FROM AnomalyFunctionSpec af"),
    @NamedQuery(
        name = "com.linkedin.thirdeye.api.AnomalyFunctionSpec#findAllByCollection",
        query = "SELECT af FROM AnomalyFunctionSpec af WHERE af.collection = :collection"),
    @NamedQuery(
        name = "com.linkedin.thirdeye.api.AnomalyFunctionSpec#toggleActive",
        query = "UPDATE AnomalyFunctionSpec set isActive = :isActive WHERE id = :id"
    )
})
public class AnomalyFunctionSpec {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Column(name = "collection", nullable = false)
  private String collection;

  @Column(name = "metric", nullable = false)
  private String metric;

  @Column(name = "type", nullable = false)
  private String type;

  @Column(name = "is_active", nullable = false)
  private boolean isActive = true;

  @Column(name = "properties", nullable = true)
  private String properties;

  @Column(name = "cron", nullable = false)
  private String cron;

  @Column(name = "bucket_size", nullable = false)
  private Integer bucketSize;

  @Column(name = "bucket_unit", nullable = false)
  private TimeUnit bucketUnit;

  @Column(name = "window_size", nullable = false)
  private Integer windowSize;

  @Column(name = "window_unit", nullable = false)
  private TimeUnit windowUnit;

  @Column(name = "window_delay", nullable = false)
  private Integer windowDelay;

  @Column(name = "explore_dimensions", nullable = true)
  private String exploreDimensions;

  public AnomalyFunctionSpec() {}

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public boolean getIsActive() {
    return isActive;
  }

  public void setIsActive(boolean isActive) {
    this.isActive = isActive;
  }

  public String getProperties() {
    return properties;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public Integer getBucketSize() {
    return bucketSize;
  }

  public void setBucketSize(Integer bucketSize) {
    this.bucketSize = bucketSize;
  }

  public TimeUnit getBucketUnit() {
    return bucketUnit;
  }

  public void setBucketUnit(TimeUnit bucketUnit) {
    this.bucketUnit = bucketUnit;
  }

  public Integer getWindowSize() {
    return windowSize;
  }

  public void setWindowSize(int windowSize) {
    this.windowSize = windowSize;
  }

  public TimeUnit getWindowUnit() {
    return windowUnit;
  }

  public void setWindowUnit(TimeUnit windowUnit) {
    this.windowUnit = windowUnit;
  }

  public Integer getWindowDelay() {
    return windowDelay;
  }

  public void setWindowDelay(int windowDelay) {
    this.windowDelay = windowDelay;
  }

  public String getExploreDimensions() {
    return exploreDimensions;
  }

  public void setExploreDimensions(String exploreDimensions) {
    this.exploreDimensions = exploreDimensions;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AnomalyFunctionSpec)) {
      return false;
    }
    AnomalyFunctionSpec af = (AnomalyFunctionSpec) o;
    return Objects.equals(id, af.getId())
        && Objects.equals(collection, af.getCollection())
        && Objects.equals(metric, af.getMetric())
        && Objects.equals(type, af.getType())
        && Objects.equals(isActive, af.getIsActive())
        && Objects.equals(cron, af.getCron())
        && Objects.equals(properties, af.getProperties())
        && Objects.equals(bucketSize, af.getBucketSize())
        && Objects.equals(bucketUnit, af.getBucketUnit())
        && Objects.equals(windowSize, af.getWindowSize())
        && Objects.equals(windowUnit, af.getWindowUnit())
        && Objects.equals(windowDelay, af.getWindowDelay())
        && Objects.equals(exploreDimensions, af.getExploreDimensions());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id,
        collection,
        metric,
        type,
        isActive,
        cron,
        properties,
        bucketSize,
        bucketUnit,
        windowSize,
        windowUnit,
        windowDelay,
        exploreDimensions);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("collection", collection)
        .add("metric", metric)
        .add("type", type)
        .add("isActive", isActive)
        .add("cron", cron)
        .add("properties", properties)
        .add("bucketSize", bucketSize)
        .add("bucketUnit", bucketUnit)
        .add("windowSize", windowSize)
        .add("windowUnit", windowUnit)
        .add("windowDelay", windowDelay)
        .add("exploreDimensions", exploreDimensions)
        .toString();
  }
}
