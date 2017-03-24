package com.linkedin.thirdeye.datalayer.entity;

@Deprecated
public class EmailConfigurationIndex extends AbstractIndexEntity {
  String collection;
  String metric;
  boolean active;

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


  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }
}
