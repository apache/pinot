package com.linkedin.thirdeye.rootcause.util;

import java.util.Objects;


public final class FilterPredicate {
  final String key;
  final String operator;
  final String value;

  public FilterPredicate(String key, String operator, String value) {
    this.key = key;
    this.operator = operator;
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilterPredicate that = (FilterPredicate) o;
    return Objects.equals(key, that.key) && Objects.equals(operator, that.operator) && Objects.equals(value,
        that.value);
  }

  @Override
  public int hashCode() {

    return Objects.hash(key, operator, value);
  }
}
