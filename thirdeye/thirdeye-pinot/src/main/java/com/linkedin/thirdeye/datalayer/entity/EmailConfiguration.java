package com.linkedin.thirdeye.datalayer.entity;

public class EmailConfiguration extends AbstractJsonEntity {
  String name;
  boolean active;

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
