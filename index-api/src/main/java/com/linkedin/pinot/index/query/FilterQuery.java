package com.linkedin.pinot.index.query;

import java.util.List;


/**
 * 
 * A FilterQuery is a combination of multiple operations.
 * And, Or Filter will have a list of FilterQuery; Equality, Not, Range, Regex query will take a list of values.
 * FilterQuery is processed by each segment.
 *  
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class FilterQuery {
  private String _column = null;
  private List<String> _value = null;
  private FilterOperator _operator = null;
  private List<FilterQuery> _nestedFilterQueryList = null;

  public void setColumn(String column) {
    this._column = column;
  }

  public void setValue(List<String> value) {
    this._value = value;
  }

  public void setOperator(FilterOperator operator) {
    this._operator = operator;
  }

  public void setNestedFilterQueries(List<FilterQuery> nestedFilterQueryList) {
    this._nestedFilterQueryList = nestedFilterQueryList;
  }

  public String getColumn() {
    return _column;
  }

  public List<String> getValue() {
    return _value;
  }

  public FilterOperator getOperator() {
    return _operator;
  }

  public List<FilterQuery> getNestedFilterConditions() {
    return _nestedFilterQueryList;
  }

  /**
   * Filter Operator
   */
  public enum FilterOperator {
    AND,
    OR,
    EQUALITY,
    NOT,
    RANGE,
    REGEX;
  }
}
