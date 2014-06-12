package com.linkedin.pinot.query.aggregation.data;

import com.linkedin.pinot.query.aggregation.AggregationResult;


/**
 * Implementation of AggregationResult to hold double value.
 *
 */
public class DoubleContainer implements AggregationResult {
  double _value = 0;

  public DoubleContainer() {
  }

  public DoubleContainer(Number value) {
    _value = value.doubleValue();
  }

  public void setValue(Number value) {
    _value = value.doubleValue();
  }

  public double getValue() {
    return _value;
  }

  public void increment(DoubleContainer doubleContainer) {
    _value += doubleContainer.getValue();
  }

  public void increment(Number value) {
    _value += value.doubleValue();
  }

  public String toString() {
    return String.valueOf(_value);
  }
}
