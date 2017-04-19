package com.linkedin.thirdeye.anomaly.alert.grouping;

import java.util.Objects;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Group key is wrapper class that is used to retrieve the specified group of anomalies from AlertGrouper.
 *
 * @param <T> The class type of the actual entity to identify different groups. For instance, the actual key could be a
 *            DimensionMap.
 */
public class GroupKey<T> {
  private T key;

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
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
