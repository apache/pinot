package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricSpec {
  private String name;
  private String alias;
  private MetricType type;

  public MetricSpec() {
  }

  public MetricSpec(String name, MetricType type) {
    this.name = name;
    this.type = type;
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
  public MetricType getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MetricSpec)) {
      return false;
    }

    MetricSpec m = (MetricSpec) o;

    return name.equals(m.getName()) && type.equals(m.getType());
  }
}
