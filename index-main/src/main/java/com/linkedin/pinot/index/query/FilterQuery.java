package com.linkedin.pinot.index.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * 
 * A FilterQuery is a combination of multiple operations.
 * And, Or Filter will have a list of FilterQuery; Equality, Not, Range,
 * Regex query will take a list of values.
 * FilterQuery is processed by each segment.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class FilterQuery implements Serializable {
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

  public static FilterQuery fromJson(JSONObject filterQueryJsonObject) throws JSONException {
    FilterQuery filterQuery = new FilterQuery();

    filterQuery.setOperator(FilterOperator.valueOf(filterQueryJsonObject.getString("operator").toUpperCase()));
    if ((filterQuery.getOperator() == FilterOperator.AND) || (filterQuery.getOperator() == FilterOperator.OR)) {
      List<FilterQuery> nestedFilterQueryList = new ArrayList<FilterQuery>();
      JSONArray nestedFilterQueryJSONArray = filterQueryJsonObject.getJSONArray("nestedFilter");
      for (int i = 0; i < nestedFilterQueryJSONArray.length(); ++i) {
        nestedFilterQueryList.add(fromJson(nestedFilterQueryJSONArray.getJSONObject(i)));
      }
      filterQuery.setNestedFilterQueries(nestedFilterQueryList);
    } else {
      filterQuery.setColumn(filterQueryJsonObject.getString("column"));
      List<String> valueList = new ArrayList<String>();
      JSONArray valueJsonArray = filterQueryJsonObject.getJSONArray("values");
      for (int i = 0; i < valueJsonArray.length(); ++i) {
        valueList.add(valueJsonArray.getString(i));
      }
      filterQuery.setValue(valueList);
    }
    return filterQuery;

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

  @Override
  public String toString() {
    return "FilterQuery [_column=" + _column + ", _value=" + _value + ", _operator=" + _operator
        + ", _nestedFilterQueryList=" + _nestedFilterQueryList + "]";
  }


}
