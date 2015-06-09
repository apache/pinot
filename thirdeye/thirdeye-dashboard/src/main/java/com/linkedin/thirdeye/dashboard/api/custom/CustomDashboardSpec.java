package com.linkedin.thirdeye.dashboard.api.custom;

import java.util.List;

public class CustomDashboardSpec {
  private String collection;
  private List<CustomDashboardComponentSpec> components;

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public List<CustomDashboardComponentSpec> getComponents() {
    return components;
  }

  public void setComponents(List<CustomDashboardComponentSpec> components) {
    this.components = components;
  }
}
