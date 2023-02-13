package org.apache.pinot.segment.local.upsert;

@SuppressWarnings({"rawtypes"})
public class ComparisonValue {
  private final Comparable _comparisonValue;
  private final boolean _isNull;

  public ComparisonValue(Comparable comparisonValue) {
    this(comparisonValue, false);
  }
  public ComparisonValue(Comparable comparisonValue, boolean isNull) {
    _comparisonValue = comparisonValue;
    _isNull = isNull;
  }

  public Comparable getComparisonValue() {
    return _comparisonValue;
  }

  public boolean isNull() {
    return _isNull;
  }
}

