package com.linkedin.thirdeye.datalayer.entity;


public abstract class AbstractJsonEntity extends AbstractEntity {
  
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
