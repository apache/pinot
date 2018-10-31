package com.linkedin.thirdeye.datalayer.entity;

public class ConfigIndex extends AbstractIndexEntity {
  private String namespace;
  private String name;

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
