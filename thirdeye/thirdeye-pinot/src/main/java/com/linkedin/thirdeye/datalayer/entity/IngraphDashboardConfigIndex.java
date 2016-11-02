package com.linkedin.thirdeye.datalayer.entity;

public class IngraphDashboardConfigIndex extends AbstractIndexEntity {
  String name;
  boolean bootstrap;

  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public boolean isBootstrap() {
    return bootstrap;
  }
  public void setBootstrap(boolean bootstrap) {
    this.bootstrap = bootstrap;
  }


}
