package com.linkedin.pinot.query.request;

import java.io.Serializable;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * Query Type is derived from query used to describe the query with high level attributes.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 * 
 */

public class QueryType implements Serializable {
  private boolean _hasSelection = false;
  private boolean _hasFilter = false;
  private boolean _hasAggregation = false;
  private boolean _hasGroupBy = false;
  private boolean _hasTimeInterval = false;
  private boolean _hasTimeGranularity = false;

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

  public boolean hasTimeInterval() {
    return _hasTimeInterval;
  }

  public void setTimeInterval(boolean hasTimeInterval) {
    _hasTimeInterval = hasTimeInterval;
  }

  public boolean hasTimeGranularity() {
    return _hasTimeGranularity;
  }

  public void setTimeGranularity(boolean hasTimeGranularity) {
    _hasTimeGranularity = hasTimeGranularity;
  }

  public static QueryType fromJson(JSONObject jsonQuery) throws JSONException {
    QueryType queryType = new QueryType();
    queryType.setSelection(jsonQuery.has("selections"));
    queryType.setFilter(jsonQuery.has("filters"));
    queryType.setGroupBy(jsonQuery.has("groupBy"));
    queryType.setAggregation(jsonQuery.has("aggregations") && (jsonQuery.getJSONArray("aggregations").length() > 0));
    queryType.setTimeInterval(jsonQuery.has("timeInterval"));
    queryType.setTimeGranularity(jsonQuery.has("timeGranularity"));
    return queryType;
  }
}
