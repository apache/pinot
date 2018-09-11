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

package com.linkedin.thirdeye.datasource.comparison;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.TimeRangeUtils;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.datasource.comparison.Row.Metric;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class TimeOnTimeComparisonHandler {
  private static final String CURRENT = "current";
  private static final String BASELINE = "baseline";
  private final QueryCache queryCache;

  public TimeOnTimeComparisonHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  public TimeOnTimeComparisonResponse handle(TimeOnTimeComparisonRequest comparisonRequest)
      throws Exception {
    List<Range<DateTime>> baselineTimeranges = new ArrayList<>();
    List<Range<DateTime>> currentTimeranges = new ArrayList<>();
    TimeGranularity aggregationTimeGranularity = comparisonRequest.getAggregationTimeGranularity();
    // baseline time ranges
    DateTime baselineStart = comparisonRequest.getBaselineStart();
    DateTime baselineEnd = comparisonRequest.getBaselineEnd();
    // current time ranges
    DateTime currentStart = comparisonRequest.getCurrentStart();
    DateTime currentEnd = comparisonRequest.getCurrentEnd();

    if (comparisonRequest.isEndDateInclusive()) {
      // ThirdEyeRequest is exclusive endpoint, so increment end by one bucket
      currentEnd = TimeRangeUtils.increment(currentEnd, aggregationTimeGranularity );
      baselineEnd = TimeRangeUtils.increment(baselineEnd, aggregationTimeGranularity );
    }
    baselineTimeranges =
        TimeRangeUtils.computeTimeRanges(aggregationTimeGranularity, baselineStart, baselineEnd);
    currentTimeranges =
        TimeRangeUtils.computeTimeRanges(aggregationTimeGranularity, currentStart, currentEnd);

    // create baseline request
    ThirdEyeRequest baselineRequest =
        createThirdEyeRequest(BASELINE, comparisonRequest, baselineStart, baselineEnd);
    // create current request
    ThirdEyeRequest currentRequest =
        createThirdEyeRequest(CURRENT, comparisonRequest, currentStart, currentEnd);

    List<ThirdEyeRequest> requests = new ArrayList<>();
    requests.add(baselineRequest);
    requests.add(currentRequest);
    Map<ThirdEyeRequest, Future<ThirdEyeResponse>> futureResponseMap;
    futureResponseMap = queryCache.getQueryResultsAsync(requests);
    ThirdEyeResponse baselineResponse = null;
    ThirdEyeResponse currentResponse = null;
    for (Entry<ThirdEyeRequest, Future<ThirdEyeResponse>> entry : futureResponseMap.entrySet()) {
      ThirdEyeRequest request = entry.getKey();
      Future<ThirdEyeResponse> responseFuture = entry.getValue();
      ThirdEyeResponse response = responseFuture.get(60000, TimeUnit.SECONDS);
      if (BASELINE.equals(request.getRequestReference())) {
        baselineResponse = response;
      } else if (CURRENT.equals(request.getRequestReference())) {
        currentResponse = response;
      }
    }
    TimeOnTimeResponseParser timeOnTimeResponseParser =
        new TimeOnTimeResponseParser(baselineResponse, currentResponse, baselineTimeranges,
            currentTimeranges, comparisonRequest.getAggregationTimeGranularity(),
            comparisonRequest.getGroupByDimensions());
    List<Row> rows = timeOnTimeResponseParser.parseResponse();
    // compute the derived metrics
    computeDerivedMetrics(comparisonRequest, rows);
    return new TimeOnTimeComparisonResponse(rows);
  }

  private void computeDerivedMetrics(TimeOnTimeComparisonRequest comparisonRequest, List<Row> rows)
      throws Exception {
    // compute list of derived expressions
    List<MetricFunction> metricFunctionsFromExpressions =
        Utils.computeMetricFunctionsFromExpressions(comparisonRequest.getMetricExpressions());
    Set<String> metricNameSet = new HashSet<>();
    for (MetricFunction function : metricFunctionsFromExpressions) {
      metricNameSet.add(function.getMetricName());
    }
    List<MetricExpression> derivedMetricExpressions = new ArrayList<>();
    for (MetricExpression expression : comparisonRequest.getMetricExpressions()) {
      if (!metricNameSet.contains(expression.getExpressionName())) {
        derivedMetricExpressions.add(expression);
      }
    }

    // add metric expressions
    if (derivedMetricExpressions.size() > 0) {
      Map<String, Double> baselineValueContext = new HashMap<>();
      Map<String, Double> currentValueContext = new HashMap<>();
      for (Row row : rows) {
        baselineValueContext.clear();
        currentValueContext.clear();
        List<Metric> metrics = row.getMetrics();
        // baseline value
        for (Metric metric : metrics) {
          baselineValueContext.put(metric.getMetricName(), metric.getBaselineValue());
          currentValueContext.put(metric.getMetricName(), metric.getCurrentValue());
        }
        for (MetricExpression expression : derivedMetricExpressions) {
          String derivedMetricExpression = expression.getExpression();
          double derivedMetricBaselineValue =
              MetricExpression.evaluateExpression(derivedMetricExpression, baselineValueContext);
          if (Double.isInfinite(derivedMetricBaselineValue)
              || Double.isNaN(derivedMetricBaselineValue)) {
            derivedMetricBaselineValue = 0;
          }
          double currentMetricBaselineValue =
              MetricExpression.evaluateExpression(derivedMetricExpression, currentValueContext);
          if (Double.isInfinite(currentMetricBaselineValue)
              || Double.isNaN(currentMetricBaselineValue)) {
            currentMetricBaselineValue = 0;
          }

          row.getMetrics().add(
              new Metric(expression.getExpressionName(), derivedMetricBaselineValue,
                  currentMetricBaselineValue));
        }
      }
    }
  }

  private static ThirdEyeRequest createThirdEyeRequest(String requestReference,
      TimeOnTimeComparisonRequest comparisonRequest, DateTime start, DateTime end) {
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setStartTimeInclusive(start);
    requestBuilder.setEndTimeExclusive(end);
    requestBuilder.setFilterSet(comparisonRequest.getFilterSet());
    requestBuilder.addGroupBy(comparisonRequest.getGroupByDimensions());
    requestBuilder.setGroupByTimeGranularity(comparisonRequest.getAggregationTimeGranularity());
    List<MetricExpression> metricExpressions = comparisonRequest.getMetricExpressions();
    List<MetricFunction> metricFunctionsFromExpressions =
        Utils.computeMetricFunctionsFromExpressions(metricExpressions);
    requestBuilder.setMetricFunctions(metricFunctionsFromExpressions);
    requestBuilder.setDataSource(ThirdEyeUtils.getDataSourceFromMetricFunctions(metricFunctionsFromExpressions));
    return requestBuilder.build(requestReference);
  }

}
