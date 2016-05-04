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

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.QueryCache;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeResponse;

public class TimeSeriesHandler {
  private static final int DEFAULT_QUERY_TIMEOUT = 60;
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
    List<Range<DateTime>> timeRanges = timeSeriesRequest.getTimeRanges();

    Map<TimeSeriesRequest, Map<ThirdEyeRequest, Future<ThirdEyeResponse>>> subRequestQueryResponseMap =
        new HashMap<>();
    // For each time bucket, generate a smaller TimeSeriesRequest and the ThirdEyeRequests it would
    // generate.
    for (Range<DateTime> range : timeRanges) {
      TimeSeriesRequest subTimeSeriesRequest = new TimeSeriesRequest(timeSeriesRequest);
      subTimeSeriesRequest.setStart(range.lowerEndpoint());
      subTimeSeriesRequest.setEnd(range.upperEndpoint());
      List<ThirdEyeRequest> subRequests;
      if (hasGroupByDimensions) {
        subRequests = requestGenerator.generateRequestsForGroupByDimensions(subTimeSeriesRequest);
      } else {
        subRequests = Collections
            .singletonList(requestGenerator.generateRequestsForAggregation(subTimeSeriesRequest));
      }
      Map<ThirdEyeRequest, Future<ThirdEyeResponse>> queryResponseFutureMap =
          queryCache.getQueryResultsAsync(subRequests);
      subRequestQueryResponseMap.put(subTimeSeriesRequest, queryResponseFutureMap);
    }

    List<TimeSeriesRow> rows = new ArrayList<>();
    for (Entry<TimeSeriesRequest, Map<ThirdEyeRequest, Future<ThirdEyeResponse>>> entry : subRequestQueryResponseMap
        .entrySet()) {
      TimeSeriesRequest subTimeSeriesRequest = entry.getKey();
      Map<ThirdEyeRequest, ThirdEyeResponse> queryResponseMap = waitForFutures(entry.getValue());
      if (hasGroupByDimensions) {
        rows.addAll(
            responseParser.parseGroupByDimensionResponse(subTimeSeriesRequest, queryResponseMap));
      } else {
        rows.add(
            responseParser.parseAggregationOnlyResponse(subTimeSeriesRequest, queryResponseMap));
      }
    }

    List<MetricFunction> metricFunctions = timeSeriesRequest.getMetricFunctions();
    return new TimeSeriesResponse(metricFunctions, groupByDimensions, rows);
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
      responseMap.put(entry.getKey(),
          entry.getValue().get(DEFAULT_QUERY_TIMEOUT, TimeUnit.SECONDS));
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
