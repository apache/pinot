package com.linkedin.thirdeye.datalayer.entity;

public class DetectionConfigIndex extends AbstractIndexEntity {
  String className;
  String name;

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
