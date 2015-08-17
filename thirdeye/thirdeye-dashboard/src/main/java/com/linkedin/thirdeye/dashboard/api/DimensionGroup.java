package com.linkedin.thirdeye.dashboard.api;

import java.util.List;
import java.util.Objects;

public class DimensionGroup {
  private String name;
  private String dimension;
  private String regex;
  private List<String> values;

  public DimensionGroup() {}

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDimension() {
    return dimension;
  }

  public void setDimension(String dimension) {
    this.dimension = dimension;
  }

  public String getRegex() {
    return regex;
  }

  public void setRegex(String regex) {
    this.regex = regex;
  }

  public List<String> getValues() {
    return values;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DimensionGroup)) {
      return false;
    }
    DimensionGroup g = (DimensionGroup) o;
    return Objects.equals(name, g.getName())
        && Objects.equals(dimension, g.getDimension())
        && Objects.equals(regex, g.getRegex())
        && Objects.equals(values, g.getValues());
  }
}
