package com.linkedin.pinot.query.request;

import org.json.JSONObject;


/**
 * Query Type is derived from query used to describe the query with high level attributes.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 * 
 */

public class QueryType {
  private boolean _hasSelection = false;
  private boolean _hasFilter = false;
  private boolean _hasAggregation = false;
  private boolean _hasGroupBy = false;

  public boolean hasSelection() {
    return _hasSelection;
  }

  public void setSelection(boolean hasSelection) {
    this._hasSelection = hasSelection;
  }

  public boolean hasFilter() {
    return _hasFilter;
  }

  public void setFilter(boolean hasFilter) {
    this._hasFilter = hasFilter;
  }

  public boolean hasAggregation() {
    return _hasAggregation;
  }

  public void setAggregation(boolean hasAggregation) {
    this._hasAggregation = hasAggregation;
  }

  public boolean hasGroupBy() {
    return _hasGroupBy;
  }

  public void setGroupBy(boolean hasGroupBy) {
    this._hasGroupBy = hasGroupBy;
  }

  public static QueryType fromJson(JSONObject jsonQuery) {
    QueryType queryType = new QueryType();
    if (jsonQuery.getJSONObject("selections") != null) {
      queryType.setSelection(true);
    }
    if (jsonQuery.getJSONObject("filters") != null) {
      queryType.setFilter(true);
    }
    if ((jsonQuery.getJSONArray("aggregations") != null) && (jsonQuery.getJSONArray("aggregations").length() > 0)) {
      queryType.setAggregation(true);
    }
    if (jsonQuery.getJSONObject("groupBy") != null) {
      queryType.setGroupBy(true);
    }

    return queryType;
  }
}
