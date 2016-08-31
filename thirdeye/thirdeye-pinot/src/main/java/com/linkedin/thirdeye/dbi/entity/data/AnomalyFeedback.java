package com.linkedin.thirdeye.dbi.entity.data;

import com.linkedin.thirdeye.dbi.entity.base.AbstractJsonEntity;

public class AnomalyFeedback extends AbstractJsonEntity {
  String type;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
