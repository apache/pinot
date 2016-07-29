package com.linkedin.thirdeye.client.pinot.summary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Dimensions {
  @JsonProperty("names")
  List<String> names;

  Dimensions() {
    names = new ArrayList<String>();
  }

  Dimensions(List<String> names) {
    this.names = names;
  }

  public int size() {
    return names.size();
  }

  public String get(int index) {
    return names.get(index);
  }

  public List<String> getGroupByStringsAtLevel(int level) {
    return names.subList(0, level);
  }

  public List<String> getGroupByStringsAtTop() {
    return Collections.<String>emptyList();
  }

  public List<String> getGroupByStringsAtLeaf() {
    return names;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}