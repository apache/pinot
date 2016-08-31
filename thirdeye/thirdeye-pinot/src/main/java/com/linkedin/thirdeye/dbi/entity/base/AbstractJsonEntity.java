package com.linkedin.thirdeye.dbi.entity.base;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractJsonEntity extends AbstractEntity {
  /**
   * keep it in threadlocal if obj-mapper configuration is needed to change
   */
  final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  // TODO: make this DTO typed

  protected String jsonVal;

  public String getJsonVal() {
    return jsonVal;
  }

  public void setJsonVal(String jsonVal) {
    this.jsonVal = jsonVal;
  }
}
