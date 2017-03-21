package com.linkedin.thirdeye.datalayer.entity;

public class AnomalyFunctionExIndex extends AbstractIndexEntity {
  String name;
  boolean active;
  String className;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }
}
