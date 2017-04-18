package com.linkedin.thirdeye.anomaly.alert.grouping;

import java.util.Objects;

public class GroupKey<T> {
  protected T key;

  public GroupKey() {
  }

  public GroupKey(T key) {
    this.key = key;
  }

  public void setKey(T key) {
    this.key = key;
  }

  T getKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupKey<?> groupKey = (GroupKey<?>) o;
    return Objects.equals(getKey(), groupKey.getKey());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKey());
  }

  @Override
  public String toString() {
    return key.toString();
  }
}
