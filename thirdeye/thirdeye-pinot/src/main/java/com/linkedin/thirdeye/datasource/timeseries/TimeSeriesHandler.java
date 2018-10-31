/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datasource.timeseries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.datasource.timeseries.TimeSeriesRow.TimeSeriesMetric;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class TimeSeriesHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesHandler.class);
  private static final TimeSeriesResponseParser DEFAULT_TIMESERIES_RESPONSE_PARSER = new UITimeSeriesResponseParser();

  private final QueryCache queryCache;
  private ExecutorService executorService;


  public TimeSeriesHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  /**
   * Handles the given time series request using the default time series parser (i.e., {@link
   * UITimeSeriesResponseParser}.)
   *
   * @param timeSeriesRequest the request to retrieve time series.
   *
   * @return the time series for the given request.
   *
   * @throws Exception Any exception that is thrown during the retrieval.
   */
  public TimeSeriesResponse handle(TimeSeriesRequest timeSeriesRequest) throws Exception {
    return handle(timeSeriesRequest, DEFAULT_TIMESERIES_RESPONSE_PARSER);
  }

  /**
   * Handles the given time series request using the given time series parser.
   *
   * @param timeSeriesRequest the request to retrieve time series.
   *
   * @return the time series for the given request.
   *
   * @throws Exception Any exception that is thrown during the retrieval.
   */
  public TimeSeriesResponse handle(TimeSeriesRequest timeSeriesRequest,
      TimeSeriesResponseParser timeSeriesResponseParser) throws Exception {
    // Time ranges for creating ThirdEye request
    DateTime start = timeSeriesRequest.getStart();
    DateTime end = timeSeriesRequest.getEnd();
    if (timeSeriesRequest.isEndDateInclusive()) {
      // ThirdEyeRequest is exclusive endpoint, so increment by one bucket
      TimeGranularity aggregationTimeGranularity = timeSeriesRequest.getAggregationTimeGranularity();
      end = end.plus(aggregationTimeGranularity.toMillis());
    }
    // Create request
    ThirdEyeRequest request = createThirdEyeRequest("timeseries", timeSeriesRequest, start, end);
    Future<ThirdEyeResponse> responseFuture = queryCache.getQueryResultAsync(request);
    // 5 minutes timeout
    ThirdEyeResponse response = responseFuture.get(5, TimeUnit.MINUTES);
    List<TimeSeriesRow> rows = timeSeriesResponseParser.parseResponse(response);
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

          row.getMetrics().add(
              new TimeSeriesMetric(expression.getExpressionName(), derivedMetricValue));
        }
      }
    }
  }

  /**
   * An asynchrous method for handling the time series request. This method initializes executor service (if necessary)
   * and invokes the synchronous method -- handle() -- in the backend. After invoking this method, users could invoke
   * shutdownAsyncHandler() to shutdown the executor service if it is no longer needed.
   *
   * @param timeSeriesRequest the request to retrieve time series.
   * @param timeSeriesResponseParser the parser to be used to parse the result from data source
   *
   * @return a future object of time series response for the give request. Returns null if it fails to handle the
   * request.
   */
  public Future<TimeSeriesResponse> asyncHandle(final TimeSeriesRequest timeSeriesRequest,
      final TimeSeriesResponseParser timeSeriesResponseParser) {
    // For optimizing concurrency performance by reducing the access to the synchronized method
    if (executorService == null) {
      startAsyncHandler();
    }

    Future<TimeSeriesResponse> responseFuture = executorService.submit(new Callable<TimeSeriesResponse>() {
      public TimeSeriesResponse call () {
        try {
          return TimeSeriesHandler.this.handle(timeSeriesRequest, timeSeriesResponseParser);
        } catch (Exception e) {
          LOG.warn("Failed to retrieve time series of the request: {}", timeSeriesRequest);
        }
        return null;
      }
    });

    return responseFuture;
  }

  /**
   * Initializes executor service if it is null. This method is thread-safe.
   */
  private synchronized void startAsyncHandler() {
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(10);
    }
  }

  /**
   * Shutdown the executor service of this TimeSeriesHandler safely.
   */
  public void shutdownAsyncHandler() {
    AnomalyUtils.safelyShutdownExecutionService(executorService, this.getClass());
  }

  private static ThirdEyeRequest createThirdEyeRequest(String requestReference,
      TimeSeriesRequest timeSeriesRequest, DateTime start, DateTime end) {
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setStartTimeInclusive(start);
    requestBuilder.setEndTimeExclusive(end);
    requestBuilder.setFilterSet(timeSeriesRequest.getFilterSet());
    requestBuilder.addGroupBy(timeSeriesRequest.getGroupByDimensions());
    requestBuilder.setGroupByTimeGranularity(timeSeriesRequest.getAggregationTimeGranularity());
    List<MetricExpression> metricExpressions = timeSeriesRequest.getMetricExpressions();
    List<MetricFunction> metricFunctionsFromExpressions =
        Utils.computeMetricFunctionsFromExpressions(metricExpressions);
    requestBuilder.setMetricFunctions(metricFunctionsFromExpressions);
    requestBuilder.setDataSource(ThirdEyeUtils.getDataSourceFromMetricFunctions(metricFunctionsFromExpressions));
    return requestBuilder.build(requestReference);
  }

}
