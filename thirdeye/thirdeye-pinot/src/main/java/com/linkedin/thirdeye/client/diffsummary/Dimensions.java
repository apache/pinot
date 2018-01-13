package com.linkedin.thirdeye.client.diffsummary;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;

import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonProperty;


public class Dimensions {
  @JsonProperty("names")
  private ImmutableList<String> names;

  Dimensions() {
    names = ImmutableList.of();
  }

  public Dimensions(List<String> names) {
    this.names = ImmutableList.copyOf(names);
  }

  public int size() {
    return names.size();
  }

  public String get(int index) {
    return names.get(index);
  }

  public List<String> allDimensions() {
    return names;
  }

  public List<String> groupByStringsAtLevel(int level) {
    return names.subList(0, level);
  }

  public List<String> groupByStringsAtTop() {
    return Collections.emptyList();
  }

  public List<String> groupByStringsAtLeaf() {
    return allDimensions();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Dimensions that = (Dimensions) o;
    return Objects.equals(names, that.names);
  }

  @Override
  public int hashCode() {
    return Objects.hash(names);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
