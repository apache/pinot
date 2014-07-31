package com.linkedin.pinot.core.query.aggregation.data;

import com.linkedin.pinot.common.query.response.AggregationResult;


/**
 * Implementation of AggregationResult to hold long value.
 *
 */
public class LongContainer implements AggregationResult {
  long _value = 0;

  public LongContainer() {
  }

  public LongContainer(Number value) {
    _value = value.longValue();
  }

  public void set(Number value) {
    _value = value.longValue();
  }

  public long get() {
    return _value;
  }

  public void increment(LongContainer longContainer) {
    _value += longContainer.get();
  }

  public void increment(Number value) {
    _value += value.longValue();
  }

  public String toString() {
    return String.valueOf(_value);
  }
}
