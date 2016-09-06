package com.linkedin.thirdeye.api;

import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DimensionSpec {
  private String name;
  private String alias;
  private DimensionType type;
  private Properties config;

  public DimensionSpec() {
  }

  public DimensionSpec(String name) {
    this.name = name;
  }

  public DimensionSpec(String name, DimensionType type, Properties config) {
    this.name = name;
    this.type = type;
    this.config = config;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getAlias() {
    return alias;
  }

  @JsonProperty
  public DimensionType getType() {
    return type;
  }

  @JsonProperty
  public Properties getConfig() {
    return config;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DimensionSpec)) {
      return false;
    }

    DimensionSpec d = (DimensionSpec) o;

    return name.equals(d.getName());
  }
}
