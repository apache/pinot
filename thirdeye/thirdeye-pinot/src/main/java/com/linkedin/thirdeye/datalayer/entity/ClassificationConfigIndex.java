package com.linkedin.thirdeye.datalayer.entity;

public class ClassificationConfigIndex extends AbstractIndexEntity {
  String name;
  long mainFunctionId;
  boolean active;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getMainFunctionId() {
    return mainFunctionId;
  }

  public void setMainFunctionId(long mainFunctionId) {
    this.mainFunctionId = mainFunctionId;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }
}
