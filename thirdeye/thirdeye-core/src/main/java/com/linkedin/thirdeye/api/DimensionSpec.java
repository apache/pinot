package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DimensionSpec
{
  private String name;

  public DimensionSpec() {}

  public DimensionSpec(String name)
  {
    this.name = name;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof DimensionSpec))
    {
      return false;
    }

    DimensionSpec d = (DimensionSpec) o;

    return name.equals(d.getName());
  }
}
