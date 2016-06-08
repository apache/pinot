package com.linkedin.thirdeye.client.comparison;

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
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.TimeRangeUtils;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.comparison.Row.Metric;
import com.linkedin.thirdeye.dashboard.Utils;

public class TimeOnTimeComparisonHandler {
  private final QueryCache queryCache;

  public TimeOnTimeComparisonHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  public TimeOnTimeComparisonResponse handle(TimeOnTimeComparisonRequest comparisonRequest)
      throws Exception {
    ThirdEyeRequestBuilder builder = new ThirdEyeRequestBuilder();
    builder.setCollection(comparisonRequest.getCollectionName());
    List<Range<DateTime>> baselineTimeranges = new ArrayList<>();
    List<Range<DateTime>> currentTimeranges = new ArrayList<>();
    TimeGranularity aggregationTimeGranularity = comparisonRequest.getAggregationTimeGranularity();
    // baseline time ranges
    baselineTimeranges = TimeRangeUtils.computeTimeRanges(aggregationTimeGranularity,
        comparisonRequest.getBaselineStart(), comparisonRequest.getBaselineEnd());
    // current time ranges
    currentTimeranges = TimeRangeUtils.computeTimeRanges(aggregationTimeGranularity,
        comparisonRequest.getCurrentStart(), comparisonRequest.getCurrentEnd());
    // create baseline request
    ThirdEyeRequest baselineRequest = createThirdEyeRequest("baseline", comparisonRequest,
        comparisonRequest.getBaselineStart(), comparisonRequest.getBaselineEnd());
    // create current request
    ThirdEyeRequest currentRequest = createThirdEyeRequest("current", comparisonRequest,
        comparisonRequest.getCurrentStart(), comparisonRequest.getCurrentEnd());

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
      ThirdEyeResponse response = responseFuture.get(60, TimeUnit.SECONDS);
      if ("baseline".equals(request.getRequestReference())) {
        baselineResponse = response;
      } else if ("current".equals(request.getRequestReference())) {
        currentResponse = response;
      }
    }
    TimeOnTimeResponseParser timeOnTimeResponseParser =
        new TimeOnTimeResponseParser(baselineResponse, currentResponse, baselineTimeranges,
            currentTimeranges, comparisonRequest.getAggregationTimeGranularity(),
            comparisonRequest.getGroupByDimensions());
    List<Row> rows = timeOnTimeResponseParser.parseResponse();
    //compute the derived metrics
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
          if (Double.isInfinite(derivedMetricBaselineValue) || Double.isNaN(derivedMetricBaselineValue)) {
            derivedMetricBaselineValue = 0;
          }
          double currentMetricBaselineValue =
              MetricExpression.evaluateExpression(derivedMetricExpression, currentValueContext);
          if (Double.isInfinite(currentMetricBaselineValue) || Double.isNaN(currentMetricBaselineValue)) {
            currentMetricBaselineValue = 0;
          }

          row.getMetrics().add(new Metric(expression.getExpressionName(),
              derivedMetricBaselineValue, currentMetricBaselineValue));
        }
      }
    }
  }

  private static ThirdEyeRequest createThirdEyeRequest(String requestReference,
      TimeOnTimeComparisonRequest comparisonRequest, DateTime start, DateTime end) {
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setCollection(comparisonRequest.getCollectionName());
    requestBuilder.setStartTimeInclusive(start);
    requestBuilder.setEndTimeExclusive(end);
    requestBuilder.setFilterSet(comparisonRequest.getFilterSet());
    requestBuilder.addGroupBy(comparisonRequest.getGroupByDimensions());
    requestBuilder.setGroupByTimeGranularity(comparisonRequest.getAggregationTimeGranularity());
    List<MetricExpression> metricExpressions = comparisonRequest.getMetricExpressions();
    List<MetricFunction> metricFunctionsFromExpressions =
        Utils.computeMetricFunctionsFromExpressions(metricExpressions);
    requestBuilder.setMetricFunctions(metricFunctionsFromExpressions);
    return requestBuilder.build(requestReference);
  }

  public ThirdEyeClient getClient() {
    return queryCache.getClient();
  }

  public static void main(String[] args) {
    long x = (long) (1 << 48 - 1);
    System.out.println(x);
    System.out.println(Double.parseDouble("" + x));
  }
}
