package org.apache.pinot.core.query.aggregation.function;

public class FilterableAggregation {
  private boolean _isFilteredAggregation;

  public void setFilteredAggregation(boolean isFilteredAggregation) {
    _isFilteredAggregation = isFilteredAggregation;
  }

  public boolean isFilteredAggregation() {
    return _isFilteredAggregation;
  }
}
