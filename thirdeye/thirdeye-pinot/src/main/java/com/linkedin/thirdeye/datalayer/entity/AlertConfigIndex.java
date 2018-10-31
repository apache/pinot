package com.linkedin.thirdeye.datalayer.entity;

public class AlertConfigIndex extends AbstractIndexEntity {
  String name;
  String application;
  boolean active;

  public String getApplication() {
    return application;
  }

  public void setApplication(String application) {
    this.application = application;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
