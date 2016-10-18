package com.linkedin.thirdeye.datalayer.entity;

public class GenericJsonEntity extends AbstractJsonEntity {

  @Override
  public String toString() {
    return "GenericJsonEntity [id=" + id + ", beanClass=" + beanClass + ", version=" + version
        + ", createTime=" + createTime + ", updateTime=" + updateTime + ", jsonVal=" + jsonVal
        + "]";
  }

 
}
