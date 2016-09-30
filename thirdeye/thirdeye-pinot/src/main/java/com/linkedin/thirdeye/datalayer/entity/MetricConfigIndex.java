package com.linkedin.thirdeye.datalayer.entity;

public class MetricConfigIndex extends AbstractIndexEntity {
  String name;
  String alias;
  String dataset;

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }


}
