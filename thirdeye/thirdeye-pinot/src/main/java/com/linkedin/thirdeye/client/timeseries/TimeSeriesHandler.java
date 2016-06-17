package com.linkedin.thirdeye.client.timeseries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.TimeRangeUtils;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.TimeSeriesMetric;
import com.linkedin.thirdeye.dashboard.Utils;

public class TimeSeriesHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesHandler.class);
  private final QueryCache queryCache;

  public TimeSeriesHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  public TimeSeriesResponse handle(TimeSeriesRequest timeSeriesRequest) throws Exception {
    ThirdEyeRequestBuilder builder = new ThirdEyeRequestBuilder();
    builder.setCollection(timeSeriesRequest.getCollectionName());
    List<Range<DateTime>> timeranges = new ArrayList<>();
    TimeGranularity aggregationTimeGranularity = timeSeriesRequest.getAggregationTimeGranularity();
    // time ranges
    timeranges = TimeRangeUtils.computeTimeRanges(aggregationTimeGranularity,
        timeSeriesRequest.getStart(), timeSeriesRequest.getEnd());
    // create request
    ThirdEyeRequest request = createThirdEyeRequest("timeseries", timeSeriesRequest,
        timeSeriesRequest.getStart(), timeSeriesRequest.getEnd());

    Future<ThirdEyeResponse> responseFuture = queryCache.getQueryResultAsync(request);
    ThirdEyeResponse response = responseFuture.get(60, TimeUnit.SECONDS);

    TimeSeriesResponseParser timeSeriesResponseParser = new TimeSeriesResponseParser(response,
        timeranges, timeSeriesRequest.getAggregationTimeGranularity(),
        timeSeriesRequest.getGroupByDimensions());
    List<TimeSeriesRow> rows = timeSeriesResponseParser.parseResponse();
    // compute the derived metrics
    computeDerivedMetrics(timeSeriesRequest, rows);
    return new TimeSeriesResponse(rows);
  }

  private void computeDerivedMetrics(TimeSeriesRequest timeSeriesRequest, List<TimeSeriesRow> rows)
      throws Exception {
    // compute list of derived expressions
    List<MetricFunction> metricFunctionsFromExpressions =
        Utils.computeMetricFunctionsFromExpressions(timeSeriesRequest.getMetricExpressions());
    Set<String> metricNameSet = new HashSet<>();
    for (MetricFunction function : metricFunctionsFromExpressions) {
      metricNameSet.add(function.getMetricName());
    }
    List<MetricExpression> derivedMetricExpressions = new ArrayList<>();
    for (MetricExpression expression : timeSeriesRequest.getMetricExpressions()) {
      if (!metricNameSet.contains(expression.getExpressionName())) {
        derivedMetricExpressions.add(expression);
      }
    }

    // add metric expressions
    if (derivedMetricExpressions.size() > 0) {
      Map<String, Double> valueContext = new HashMap<>();
      for (TimeSeriesRow row : rows) {
        valueContext.clear();
        List<TimeSeriesMetric> metrics = row.getMetrics();
        // baseline value
        for (TimeSeriesMetric metric : metrics) {
          valueContext.put(metric.getMetricName(), metric.getValue());
        }
        for (MetricExpression expression : derivedMetricExpressions) {
          String derivedMetricExpression = expression.getExpression();
          double derivedMetricValue =
              MetricExpression.evaluateExpression(derivedMetricExpression, valueContext);
          if (Double.isInfinite(derivedMetricValue) || Double.isNaN(derivedMetricValue)) {
            derivedMetricValue = 0;
          }

          row.getMetrics()
              .add(new TimeSeriesMetric(expression.getExpressionName(), derivedMetricValue));
        }
      }
    }
  }

  private static ThirdEyeRequest createThirdEyeRequest(String requestReference,
      TimeSeriesRequest timeSeriesRequest, DateTime start, DateTime end) {
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setCollection(timeSeriesRequest.getCollectionName());
    requestBuilder.setStartTimeInclusive(start);
    requestBuilder.setEndTimeExclusive(end);
    requestBuilder.setFilterSet(timeSeriesRequest.getFilterSet());
    requestBuilder.addGroupBy(timeSeriesRequest.getGroupByDimensions());
    requestBuilder.setGroupByTimeGranularity(timeSeriesRequest.getAggregationTimeGranularity());
    List<MetricExpression> metricExpressions = timeSeriesRequest.getMetricExpressions();
    List<MetricFunction> metricFunctionsFromExpressions =
        Utils.computeMetricFunctionsFromExpressions(metricExpressions);
    requestBuilder.setMetricFunctions(metricFunctionsFromExpressions);
    return requestBuilder.build(requestReference);
  }

  public ThirdEyeClient getClient() {
    return queryCache.getClient();
  }

}
