package com.linkedin.pinot.query.request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.query.aggregation.AggregationFunction;
import com.linkedin.pinot.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.query.utils.TimeUtils;


public class Query implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

  private QueryType _queryType;
  private String _sourceName;
  private String _resourceName;
  private String _tableName;
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
    return _sourceName;
  }

  public void setSourceName(String sourceName) {
    this._sourceName = sourceName;
    int indexOfDot = sourceName.indexOf(".");
    if (indexOfDot > 0) {
      _resourceName = sourceName.substring(0, indexOfDot);
      _tableName = sourceName.substring(indexOfDot + 1, sourceName.length());
    } else {
      _resourceName = sourceName;
      _tableName = null;
    }
  }

  public String getResourceName() {
    return _resourceName;
  }

  public void setResourceName(String resourceName) {
    this._resourceName = resourceName;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    this._tableName = tableName;
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
      AggregationFunction agg1 = AggregationFunctionFactory.get(agg);
      if ( null == agg1)
      {
        LOGGER.error("Aggregation function is null for aggregation info :" + agg);
      }
      aggregationFunctions.add(agg1);
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
    query.setSourceName(jsonQuery.getString("source"));

    if ( jsonQuery.has("queryType"))
    {
      query.setQueryType(QueryType.valueOf(jsonQuery.getString("queryType")));
    }

    if ( jsonQuery.has("aggregations"))
    {
      query.setAggregationsInfo(AggregationInfo.fromJson(jsonQuery.getJSONArray("aggregations")));
    }
    if ( jsonQuery.has("filters"))
    {
      query.setFilterQuery(FilterQuery.fromJson(jsonQuery.getJSONObject("filters")));
    }

    if ( jsonQuery.has("groupBy"))
    {
      query.setGroupBy(GroupBy.fromJson(jsonQuery.getJSONObject("groupBy")));
    }

    if ( jsonQuery.has("selections"))
    {
      query.setSelections(Selection.fromJson(jsonQuery.getJSONObject("selections")));
    }

    if ( jsonQuery.has("timeInterval"))
    {
      query.setTimeInterval(getIntervalFromJson(jsonQuery.getJSONObject("timeInterval")));
    }

    if ( jsonQuery.has("timeGranularity"))
    {
      query.setTimeGranularity(getTimeGranularityFromJson(jsonQuery.getString("timeGranularity")));
    }
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
    return "Query [_queryType=" + _queryType + ", _sourceName=" + _sourceName + ", _resourceName=" + _resourceName
        + ", _tableName=" + _tableName + ", _timeInterval=" + _timeInterval + ", _timeGranularity=" + _timeGranularity
        + ", _filterQuery=" + _filterQuery + ", _aggregationsInfo=" + _aggregationsInfo + ", _groupBy=" + _groupBy
        + ", _selections=" + _selections + "]";
  }
}
