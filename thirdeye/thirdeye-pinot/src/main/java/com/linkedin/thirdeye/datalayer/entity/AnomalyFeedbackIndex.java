package com.linkedin.thirdeye.datalayer.entity;

public class AnomalyFeedbackIndex extends AbstractIndexEntity {
  String type;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
