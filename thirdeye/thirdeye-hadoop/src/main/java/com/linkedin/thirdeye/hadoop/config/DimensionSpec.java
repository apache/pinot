package com.linkedin.thirdeye.hadoop.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DimensionSpec {
  private String name;
  private String alias;

  public DimensionSpec() {
  }


  public DimensionSpec(String name) {
    this.name = name;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getAlias() {
    return alias;
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
