package com.linkedin.thirdeye.datalayer.entity;

public abstract class AbstractIndexEntity extends AbstractEntity {

  protected Long baseId;

  public Long getBaseId() {
    return baseId;
  }

  public void setBaseId(Long baseId) {
    this.baseId = baseId;
  }
}
