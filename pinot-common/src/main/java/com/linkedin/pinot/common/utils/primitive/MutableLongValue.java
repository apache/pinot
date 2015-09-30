package com.linkedin.pinot.common.utils.primitive;

import java.io.Serializable;


/**
 * Simple wrapper for a mutable long value.
 */
public class MutableLongValue extends Number implements Serializable, Comparable<MutableLongValue> {
  private long value;

  public MutableLongValue(long value) {
    this.value = value;
  }

  public MutableLongValue() {
    value = 0L;
  }

  @Override
  public int intValue() {
    return (int) value;
  }

  @Override
  public long longValue() {
    return value;
  }

  @Override
  public float floatValue() {
    return value;
  }

  @Override
  public double doubleValue() {
    return value;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }

  public void addToValue(long otherValue) {
    value += otherValue;
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }

  @Override
  public int compareTo(MutableLongValue o) {
    return Long.compare(value, o.value);
  }
}
