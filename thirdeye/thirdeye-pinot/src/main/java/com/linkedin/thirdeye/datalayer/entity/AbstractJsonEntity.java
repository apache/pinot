package com.linkedin.thirdeye.datalayer.entity;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractJsonEntity extends AbstractEntity {
  /**
   * keep it in threadlocal if obj-mapper configuration is needed to change
   */
  final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  // TODO: make this DTO typed
  
  protected String jsonVal;
  
  protected String beanClass;

  public String getJsonVal() {
    return jsonVal;
  }

  public void setJsonVal(String jsonVal) {
    this.jsonVal = jsonVal;
  }

  public String getBeanClass() {
    return beanClass;
  }

  public void setBeanClass(String beanClass) {
    this.beanClass = beanClass;
  }
}
