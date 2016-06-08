package com.linkedin.thirdeye.client.timeseries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.ThirdEyeResponseRow;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.Builder;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.TimeSeriesMetric;
import com.linkedin.thirdeye.dashboard.Utils;

public class TimeSeriesHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesHandler.class);
  private static final int DEFAULT_QUERY_TIMEOUT = 300;
  private final QueryCache queryCache;
  private TimeSeriesThirdEyeRequestGenerator requestGenerator =
      new TimeSeriesThirdEyeRequestGenerator();
  private TimeSeriesResponseParser responseParser = new TimeSeriesResponseParser();

  public TimeSeriesHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  /**
   * Handles a time series request by splitting the input into its component time buckets and
   * executing those requests.
   * @throws Exception
   */
  public TimeSeriesResponse handle(TimeSeriesRequest timeSeriesRequest) throws Exception {
    List<String> groupByDimensions = timeSeriesRequest.getGroupByDimensions();
    boolean hasGroupByDimensions = CollectionUtils.isNotEmpty(groupByDimensions);

    ThirdEyeRequestBuilder requestBuilder = new ThirdEyeRequestBuilder();
    requestBuilder.setCollection(timeSeriesRequest.getCollectionName());
    requestBuilder.setStartTimeInclusive(timeSeriesRequest.getStart());
    requestBuilder.setEndTimeExclusive(timeSeriesRequest.getEnd());
    List<MetricFunction> metricFunctionsFromExpressions =
        Utils.computeMetricFunctionsFromExpressions(timeSeriesRequest.getMetricExpressions());
    requestBuilder.setMetricFunctions(metricFunctionsFromExpressions);
    if (hasGroupByDimensions) {
      requestBuilder.setGroupBy(groupByDimensions);
    }
    requestBuilder.setGroupByTimeGranularity(timeSeriesRequest.getAggregationTimeGranularity());

    ThirdEyeRequest thirdeyeRequest = requestBuilder.build("timeseries");
    ThirdEyeResponse thirdEyeResponse = queryCache.getClient().execute(thirdeyeRequest);

    List<TimeSeriesRow> rows = new ArrayList<>();
    int numRows = thirdEyeResponse.getNumRows();
    List<Range<DateTime>> timeRanges = timeSeriesRequest.getTimeRanges();

    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdeyeResponseRow = thirdEyeResponse.getRow(i);
      TimeSeriesRow timeSeriesRow = convertToTimeSeriesRow(thirdeyeRequest, thirdeyeResponseRow, timeRanges);
      rows.add(timeSeriesRow);
    }

    List<MetricExpression> metricExpressions = timeSeriesRequest.getMetricExpressions();
    // compute list of derived expressions
    List<MetricExpression> derivedMetricExpressions = new ArrayList<>();
    for (MetricExpression expression : metricExpressions) {
      if (expression.computeMetricFunctions().size() > 1) {
        derivedMetricExpressions.add(expression);
      }
    }
    // add metric expressions
    if (derivedMetricExpressions.size() > 0) {
      Map<String, Double> metricValueContext = new HashMap<>();
      for (TimeSeriesRow row : rows) {
        metricValueContext.clear();
        List<TimeSeriesMetric> metrics = row.getMetrics();
        for (TimeSeriesMetric metric : metrics) {
          metricValueContext.put(metric.getMetricName(), metric.getValue());
        }
        for (MetricExpression expression : derivedMetricExpressions) {
          String derivedMetricExpression = expression.getExpression();
          double derivedMetricValue =
              MetricExpression.evaluateExpression(derivedMetricExpression, metricValueContext);
          row.getMetrics()
              .add(new TimeSeriesMetric(expression.getExpressionName(), derivedMetricValue));
        }
      }
    }
    return new TimeSeriesResponse(metricExpressions, groupByDimensions, rows);
  }

  private TimeSeriesRow convertToTimeSeriesRow(ThirdEyeRequest thirdeyeRequest, ThirdEyeResponseRow thirdeyeResponseRow, List<Range<DateTime>> timeRanges) {
    Builder builder = new TimeSeriesRow.Builder();
    Range<DateTime> range = timeRanges.get(thirdeyeResponseRow.getTimeBucketId());
    builder.setStart(range.lowerEndpoint());
    builder.setEnd(range.upperEndpoint());

    List<String> dimensionNames = thirdeyeRequest.getGroupBy();
    for (int i = 0; i < dimensionNames.size(); i ++) {
      builder.setDimensionName(dimensionNames.get(i));
      builder.setDimensionValue(thirdeyeResponseRow.getDimensions().get(i));
    }

    List<Double> metrics = thirdeyeResponseRow.getMetrics();
    for (int i = 0; i < metrics.size(); i ++) {
      String metricName = thirdeyeRequest.getMetricFunctions().get(i).getMetricName();
      builder.addMetric(metricName, metrics.get(i));
    }

    TimeSeriesRow timeSeriesRow = builder.build();
    return timeSeriesRow;
  }

  /**
   * Handle requests with only aggregation (ie no grouping by dimension nor time), e.g.
   * <code>
   * select sum(m1), sum(m2) from T where filters AND (time between start and end)
   * </code>
   */
  TimeSeriesRow handleAggregateOnly(TimeSeriesRequest timeSeriesRequest) throws Exception {
    ThirdEyeRequest request = requestGenerator.generateRequestsForAggregation(timeSeriesRequest);
    Map<ThirdEyeRequest, ThirdEyeResponse> queryResultMap = queryCache
        .getQueryResultsAsyncAndWait(Collections.singletonList(request), DEFAULT_QUERY_TIMEOUT);
    TimeSeriesRow row =
        responseParser.parseAggregationOnlyResponse(timeSeriesRequest, queryResultMap);
    return row;
  }

  /**
   * Handle aggregation requests with dimension grouping (but no time grouping). This method also
   * includes a request for the total and calculates an "OTHER" category if the sum of dimension
   * values does not add up to the total. e.g.
   * <code>
   * select sum(m1), sum(m2) from T where filters AND (time between start and end)
   * FOR EACH DIMENSION IN GROUP BY
   * select sum(m1), sum(m2) from T where filters AND (time between start and end) group by
   * dimension
   * If (total sum) > (sum of individual dimension value sums):
   * Add dimension value OTHER with value= (total sum) - (dimension value sums).
   * <code>
   */
  List<TimeSeriesRow> handleGroupByDimension(TimeSeriesRequest timeSeriesRequest) throws Exception {
    List<ThirdEyeRequest> requests =
        requestGenerator.generateRequestsForGroupByDimensions(timeSeriesRequest);
    Map<ThirdEyeRequest, ThirdEyeResponse> queryResultMap =
        queryCache.getQueryResultsAsyncAndWait(requests, DEFAULT_QUERY_TIMEOUT);
    List<TimeSeriesRow> rows =
        responseParser.parseGroupByDimensionResponse(timeSeriesRequest, queryResultMap);
    return rows;
  }

  private <K, V> Map<K, V> waitForFutures(Map<K, Future<V>> futuresMap) throws Exception {
    Map<K, V> responseMap = new LinkedHashMap<>();
    for (Entry<K, Future<V>> entry : futuresMap.entrySet()) {
      try {
        responseMap.put(entry.getKey(),
            entry.getValue().get(DEFAULT_QUERY_TIMEOUT, TimeUnit.SECONDS));
      } catch (TimeoutException e) {
        LOG.error("Unable to get future for key {}: {}", entry.getKey(), e);
        throw new TimeoutException(e.getMessage());
      }
    }
    return responseMap;
  }

  public QueryCache getQueryCache() {
    return queryCache;
  }

  public ThirdEyeClient getClient() {
    return queryCache.getClient();
  }

  // Methods for testing.
  void setRequestGenerator(TimeSeriesThirdEyeRequestGenerator requestGenerator) {
    this.requestGenerator = requestGenerator;
  }

  TimeSeriesThirdEyeRequestGenerator getRequestGenerator() {
    return requestGenerator;
  }

  TimeSeriesResponseParser getResponseParser() {
    return responseParser;
  }

  void setResponseParser(TimeSeriesResponseParser responseParser) {
    this.responseParser = responseParser;
  }

}
