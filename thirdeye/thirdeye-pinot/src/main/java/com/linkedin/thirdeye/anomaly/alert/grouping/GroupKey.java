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
   // The default empty group key that is used to represent the rolled up group, which collects the anomalies from
   // groups that contains only one anomaly.
  public static final GroupKey EMPTY_KEY = new GroupKey();

  // The actual object to distinguish group key from each other.
  private final T key;

  /**
   * Constructs an empty group key, i.e., the key is null.
   */
  public GroupKey() {
    this.key = null;
  }

  /**
   * Constructs a group key with the given key.
   *
   * @param key the actual object that is used to distinguish group key from each other.
   */
  public GroupKey(T key) {
    this.key = key;
  }

  /**
   * Returns the actual object that is used to distinguish group key from each other.
   * @return
   */
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
