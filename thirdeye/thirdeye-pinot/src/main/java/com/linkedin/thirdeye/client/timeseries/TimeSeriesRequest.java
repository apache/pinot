package com.linkedin.thirdeye.client.timeseries;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.TimeRangeUtils;

public class TimeSeriesRequest {
  private String collectionName;
  private List<MetricFunction> metricFunctions;
  /**
   * Dimensions to group by. multiple dimensions will generate multiple client requests, as opposed
   * to a single request grouped by multiple dimensions (not supported).
   */
  private List<String> groupByDimensions;
  /**
   * easy way to represent AND of multiple dimensions
   * for example
   * filterMap = new HashMap
   * filterMap.put("country", "us")
   * filterMap.put("device", "android")
   * is equivalent to setting filterClause country='us' and device='android'
   */
  private Multimap<String, String> filterSet;
  /**
   * Allows one to specify complex boolean expressions, advanced usage
   */
  private String filterClause;
  // TIME RELATED PARAMETERs
  private DateTime start;
  private DateTime end;
  private TimeGranularity aggregationTimeGranularity;

  public TimeSeriesRequest() {
  }

  /** TODO use builder pattern? */
  public TimeSeriesRequest(TimeSeriesRequest timeSeriesRequest) {
    this.collectionName = timeSeriesRequest.collectionName;
    if (timeSeriesRequest.metricFunctions != null) {
      this.metricFunctions = new ArrayList<>(timeSeriesRequest.metricFunctions);
    }
    if (timeSeriesRequest.groupByDimensions != null) {
      this.groupByDimensions = new ArrayList<>(timeSeriesRequest.groupByDimensions);
    }
    if (timeSeriesRequest.filterSet != null) {
      this.filterSet = ArrayListMultimap.create(timeSeriesRequest.filterSet);
    }
    this.filterClause = timeSeriesRequest.filterClause;
    this.start = timeSeriesRequest.start;
    this.end = timeSeriesRequest.end;
    if (timeSeriesRequest.aggregationTimeGranularity != null) {
      this.aggregationTimeGranularity =
          new TimeGranularity(timeSeriesRequest.aggregationTimeGranularity.getSize(),
              timeSeriesRequest.aggregationTimeGranularity.getUnit());
    }
  }

  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public List<MetricFunction> getMetricFunctions() {
    return metricFunctions;
  }

  public void setMetricFunctions(List<MetricFunction> metricFunctions) {
    this.metricFunctions = metricFunctions;
  }

  public List<String> getGroupByDimensions() {
    return groupByDimensions;
  }

  public void setGroupByDimensions(List<String> groupByDimensions) {
    this.groupByDimensions = groupByDimensions;
  }

  public Multimap<String, String> getFilterSet() {
    return filterSet;
  }

  public void setFilterSet(Multimap<String, String> filterSet) {
    this.filterSet = filterSet;
  }

  public String getFilterClause() {
    return filterClause;
  }

  public void setFilterClause(String filterClause) {
    this.filterClause = filterClause;
  }

  /** Get start time, inclusive. */
  public DateTime getStart() {
    return start;
  }

  /** Set start time, inclusive. */
  public void setStart(DateTime start) {
    this.start = start;
  }

  /** Get end time, exclusive. */
  public DateTime getEnd() {
    return end;
  }

  /** Set end time, exclusive. */
  public void setEnd(DateTime end) {
    this.end = end;
  }

  public TimeGranularity getAggregationTimeGranularity() {
    return aggregationTimeGranularity;
  }

  public void setAggregationTimeGranularity(TimeGranularity aggregationTimeGranularity) {
    this.aggregationTimeGranularity = aggregationTimeGranularity;
  }

  public List<Range<DateTime>> getTimeRanges() {
    return TimeRangeUtils.computeTimeRanges(aggregationTimeGranularity, start, end);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("collectionName", collectionName)
        .add("metricFunctions", metricFunctions).add("groupByDimensions", groupByDimensions)
        .add("filterSet", filterSet).add("filterClause", filterClause).add("start", start)
        .add("end", end).add("aggregationTimeGranularity", aggregationTimeGranularity).toString();
  }

  public void setMetricExpressions(List<MetricExpression> metricExpressions) {
    List<MetricFunction> metricFunctions = new ArrayList<>();
    for (MetricExpression expression : metricExpressions) {
      metricFunctions.addAll(expression.computeMetricFunctions());
    }
    setMetricFunctions(metricFunctions);
  }
}
