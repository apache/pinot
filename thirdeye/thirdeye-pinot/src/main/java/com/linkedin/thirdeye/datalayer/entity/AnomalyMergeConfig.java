package com.linkedin.thirdeye.datalayer.entity;

public class AnomalyMergeConfig extends AbstractJsonEntity {
  String name;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
