package com.linkedin.thirdeye.dbi.entity.data;

import com.linkedin.thirdeye.dbi.entity.base.AbstractJsonEntity;

public class AnomalyFunction extends AbstractJsonEntity {
  String name;
  boolean active;
  long metricId;
  String collection;
  String metric;

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
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

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
