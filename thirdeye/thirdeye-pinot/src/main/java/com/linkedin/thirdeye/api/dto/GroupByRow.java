package com.linkedin.thirdeye.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GroupByRow<K, V> {
  K groupBy;
  V value;

  public K getGroupBy() {
    return groupBy;
  }

  public void setGroupBy(K groupBy) {
    this.groupBy = groupBy;
  }

  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }
}
