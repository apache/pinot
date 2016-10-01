package com.linkedin.thirdeye.datalayer.entity;

public class DashboardConfigIndex extends AbstractIndexEntity {
  String name;
  String dataset;

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



}
