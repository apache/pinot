package com.linkedin.thirdeye.datalayer.entity;

public class AnomalyFeedback extends AbstractJsonEntity {
  String type;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
