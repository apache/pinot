package com.linkedin.thirdeye.datalayer.entity;

public class DatasetConfigIndex extends AbstractIndexEntity {
  String dataset;
  boolean active;

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
