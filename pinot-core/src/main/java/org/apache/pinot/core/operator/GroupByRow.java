package org.apache.pinot.core.operator;

import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;


public class GroupByRow {
  private String _stringKey;
  private String[] _arrayKey;
  private Object[] _aggregationResults;

  public GroupByRow(String stringKey, Object[] aggregationResults) {
    _stringKey = stringKey;
    _arrayKey = stringKey.split(",");
    _aggregationResults = aggregationResults;
  }

  public String[] getArrayKey() {
    return _arrayKey;
  }

  public void setArrayKey(String[] arrayKey) {
    _arrayKey = arrayKey;
  }

  public Object[] getAggregationResults() {
    return _aggregationResults;
  }

  public void setAggregationResults(Object[] aggregationResults) {
    _aggregationResults = aggregationResults;
  }

  public String getStringKey() {
    return _stringKey;
  }

  public void setStringKey(String stringKey) {
    _stringKey = stringKey;
  }

  public void merge(GroupByRow rowToMerge, AggregationFunction[] aggregationFunctions, int numAggregationFunctions) {
    Object[] resultToMerge = rowToMerge.getAggregationResults();
    for (int i = 0; i < numAggregationFunctions; i++) {
      _aggregationResults[i] = aggregationFunctions[i].merge(_aggregationResults[i], resultToMerge[i]);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    GroupByRow that = (GroupByRow) o;

    return EqualityUtils.isEqual(_stringKey, that._stringKey) && EqualityUtils.isEqual(_aggregationResults,
        that._aggregationResults);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_stringKey);
    result = EqualityUtils.hashCodeOf(result, _aggregationResults);
    return result;
  }
}
