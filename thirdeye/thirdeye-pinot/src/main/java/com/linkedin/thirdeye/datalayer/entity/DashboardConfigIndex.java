package com.linkedin.thirdeye.datalayer.entity;

public class DashboardConfigIndex extends AbstractIndexEntity {
  String name;
  String dataset;
  boolean active;

  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public String getDataset() {
    return dataset;
  }
  public void setDataset(String dataset) {
    this.dataset = dataset;
  }
  public boolean isActive() {
    return active;
  }
  public void setActive(boolean active) {
    this.active = active;
  }
}
