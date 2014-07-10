package com.linkedin.pinot.query.request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.json.JSONObject;

import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.query.aggregation.AggregationFunction;
import com.linkedin.pinot.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.query.utils.TimeUtils;


public class Query implements Serializable {
  private QueryType _queryType;
  private QuerySource _querySource;
  private Interval _timeInterval;
  private Duration _timeGranularity;
  private FilterQuery _filterQuery;
  private List<AggregationInfo> _aggregationsInfo;
  private GroupBy _groupBy;
  private Selection _selections;

  public QueryType getQueryType() {
    return _queryType;
  }

  public void setQueryType(QueryType queryType) {
    _queryType = queryType;
  }

  public Duration getTimeGranularity() {
    return _timeGranularity;
  }

  public void setTimeGranularity(Duration timeGranularity) {
    _timeGranularity = timeGranularity;
  }

  public String getSourceName() {
    return _querySource.toString();
  }

  public void setSourceName(String querySource) {
    this._querySource = new QuerySource();
    _querySource.fromDataSourceString(querySource);
  }

  public String getResourceName() {
    return _querySource.getResourceName();
  }

  public void setResourceName(String resourceName) {
    this._querySource.setResourceName(resourceName);
  }

  public String getTableName() {
    return _querySource.getTableName();
  }

  public void setTableName(String tableName) {
    this._querySource.setTableName(tableName);
  }

  public FilterQuery getFilterQuery() {
    return _filterQuery;
  }

  public void setFilterQuery(FilterQuery filterQuery) {
    this._filterQuery = filterQuery;
  }

  public List<AggregationFunction> getAggregationFunction() {
    List<AggregationFunction> aggregationFunctions = new ArrayList<AggregationFunction>();
    for (AggregationInfo agg : _aggregationsInfo) {
      aggregationFunctions.add(AggregationFunctionFactory.get(agg));
    }
    return aggregationFunctions;
  }

  public GroupBy getGroupBy() {
    return _groupBy;
  }

  public void setGroupBy(GroupBy groupBy) {
    this._groupBy = groupBy;
  }

  public Interval getTimeInterval() {
    return _timeInterval;
  }

  public void setTimeInterval(Interval timeInterval) {
    this._timeInterval = timeInterval;
  }

  public Selection getSelections() {
    return _selections;
  }

  public void setSelections(Selection selections) {
    _selections = selections;
  }

  public List<AggregationInfo> getAggregationsInfo() {
    return _aggregationsInfo;
  }

  public void setAggregationsInfo(List<AggregationInfo> aggregationsInfo) {
    _aggregationsInfo = aggregationsInfo;
  }

  public static Query fromJson(JSONObject jsonQuery) {
    Query query = new Query();
    query.setQueryType(QueryType.fromJson(jsonQuery));
    query.setAggregationsInfo(AggregationInfo.fromJson(jsonQuery.getJSONArray("aggregations")));
    query.setSourceName(jsonQuery.getString("source"));
    query.setFilterQuery(FilterQuery.fromJson(jsonQuery.getJSONObject("filters")));
    query.setGroupBy(GroupBy.fromJson(jsonQuery.getJSONObject("groupBy")));
    query.setSelections(Selection.fromJson(jsonQuery.getJSONObject("selections")));
    query.setTimeInterval(getIntervalFromJson(jsonQuery.getJSONObject("timeInterval")));
    query.setTimeGranularity(getTimeGranularityFromJson(jsonQuery.getString("timeGranularity")));
    return query;
  }

  public static Interval getIntervalFromJson(JSONObject jsonObject) {
    try {
      DateTime start = new DateTime(jsonObject.getString("startTime"));
      DateTime end = new DateTime(jsonObject.getString("endTime"));
      return new Interval(start, end);
    } catch (Exception e) {
      return new Interval(0, System.currentTimeMillis());
    }
  }

  public static Duration getTimeGranularityFromJson(String timeGranularity) {
    long timeInMilisecond = TimeUtils.toMillis(timeGranularity);
    return new Duration(timeInMilisecond);
  }

  @Override
  public String toString() {
    return "Query [_queryType=" + _queryType + ", _sourceName=" + _querySource.toString() + ", _resourceName="
        + _querySource.getResourceName() + ", _tableName=" + _querySource.getTableName() + ", _timeInterval="
        + _timeInterval + ", _timeGranularity=" + _timeGranularity + ", _filterQuery=" + _filterQuery
        + ", _aggregationsInfo=" + _aggregationsInfo + ", _groupBy=" + _groupBy + ", _selections=" + _selections + "]";
  }
}
