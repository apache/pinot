package com.linkedin.thirdeye.client.comparison;

import static com.linkedin.thirdeye.client.comparison.TimeOnTimeConstants.ALL_BASELINE;
import static com.linkedin.thirdeye.client.comparison.TimeOnTimeConstants.ALL_CURRENT;
import static com.linkedin.thirdeye.client.comparison.TimeOnTimeConstants.BASELINE_VALUE;
import static com.linkedin.thirdeye.client.comparison.TimeOnTimeConstants.CURRENT_VALUE;
import static com.linkedin.thirdeye.client.comparison.TimeOnTimeConstants.DIMENSION_BASELINE;
import static com.linkedin.thirdeye.client.comparison.TimeOnTimeConstants.DIMENSION_CURRENT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;

import java.util.TreeMap;

import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.comparison.Row.Builder;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeResponse;

public class TimeOnTimeResponseParser {

  private static final double MIN_CONTRIBUTION_PERCENT = 0.0001;

  public static Map<MetricFunction, Map<String, Map<String, Map<String, Double>>>> processGroupByDimensionResponse(
      Map<ThirdEyeRequest, PinotThirdEyeResponse> queryResultMap) {
    Map<MetricFunction, Map<String, Map<String, Map<String, Double>>>> metricGroupByDimensionData =
        new HashMap<>();

    for (Entry<ThirdEyeRequest, PinotThirdEyeResponse> entry : queryResultMap.entrySet()) {
      ThirdEyeRequest thirdEyeRequest = entry.getKey();
      ThirdEyeResponse thirdEyeResponse = entry.getValue();
      String requestReference = thirdEyeRequest.getRequestReference();
      if (requestReference.startsWith(DIMENSION_BASELINE)) {
        parseMetricGroupByDimensionResponse(metricGroupByDimensionData, thirdEyeRequest,
            thirdEyeResponse, BASELINE_VALUE);
      }
      if (requestReference.startsWith(DIMENSION_CURRENT)) {
        parseMetricGroupByDimensionResponse(metricGroupByDimensionData, thirdEyeRequest,
            thirdEyeResponse, CURRENT_VALUE);
      }
    }
    return metricGroupByDimensionData;
  }

  public static Map<MetricFunction, Map<String, Double>> processAggregateResponse(
      Map<ThirdEyeRequest, PinotThirdEyeResponse> queryResultMap) {
    // Map<metricFunction,Map<(baseline|current), Double>>>
    Map<MetricFunction, Map<String, Double>> metricOnlyData = new HashMap<>();

    for (Entry<ThirdEyeRequest, PinotThirdEyeResponse> entry : queryResultMap.entrySet()) {
      ThirdEyeRequest thirdEyeRequest = entry.getKey();
      ThirdEyeResponse thirdEyeResponse = entry.getValue();
      String requestReference = thirdEyeRequest.getRequestReference();
      if (ALL_BASELINE.equals(requestReference)) {
        parseMetricOnlyResponse(metricOnlyData, thirdEyeResponse, BASELINE_VALUE);
      }
      if (ALL_CURRENT.equals(requestReference)) {
        parseMetricOnlyResponse(metricOnlyData, thirdEyeResponse, CURRENT_VALUE);
      }
    }
    return metricOnlyData;
  }

  public static void parseMetricOnlyResponse(Map<MetricFunction, Map<String, Double>> aggregateData,
      ThirdEyeResponse thirdEyeResponse, String baselineOrCurrent) {
    List<MetricFunction> metricFunctions = thirdEyeResponse.getMetricFunctions();
    for (MetricFunction metricFunction : metricFunctions) {
      Map<String, String> row = thirdEyeResponse.getRow(metricFunction, 0);
      // TODO THIS DEPENDS STRICTLY ON THE ORDER OF THE KEYS RETURNED?
      String baselineValue = row.values().iterator().next();
      if (!aggregateData.containsKey(metricFunction)) {
        aggregateData.put(metricFunction, new HashMap<String, Double>());
      }
      aggregateData.get(metricFunction).put(baselineOrCurrent, Double.parseDouble(baselineValue));
    }
  }

  public static void parseMetricGroupByDimensionResponse(
      Map<MetricFunction, Map<String, Map<String, Map<String, Double>>>> dimensionData,
      ThirdEyeRequest thirdEyeRequest, ThirdEyeResponse thirdEyeResponse,
      String BASELINE_OR_CURRENT) {
    List<MetricFunction> metricFunctions = thirdEyeResponse.getMetricFunctions();
    for (MetricFunction metricFunction : metricFunctions) {
      Map<String, Map<String, Map<String, Double>>> dimensionNameMap =
          dimensionData.get(metricFunction);
      if (dimensionNameMap == null) {
        dimensionNameMap = new TreeMap<>();
        dimensionData.put(metricFunction, dimensionNameMap);
      }
      // we only group by at most 1 dimension
      String dimensionName = thirdEyeRequest.getGroupBy().get(0);
      Map<String, Map<String, Double>> dimensionValueMap = dimensionNameMap.get(dimensionName);
      if (dimensionValueMap == null) {
        dimensionValueMap = new TreeMap<>();
        dimensionData.get(metricFunction).put(dimensionName, dimensionValueMap);
      }
      // one row for each group by
      int rows = thirdEyeResponse.getNumRowsFor(metricFunction);
      for (int i = 0; i < rows; i++) {
        Map<String, String> row = thirdEyeResponse.getRow(metricFunction, i);
        String dimensionValue = row.get(dimensionName);
        if (!dimensionValueMap.containsKey(dimensionValue)) {
          Map<String, Double> value = new TreeMap<>();
          dimensionValueMap.put(dimensionValue, value);
          dimensionValueMap.get(dimensionValue).put(BASELINE_VALUE, 0d);
          dimensionValueMap.get(dimensionValue).put(CURRENT_VALUE, 0d);
        }
        Double metricValue = Double.parseDouble(row.get(metricFunction.toString()));
        dimensionValueMap.get(dimensionValue).put(BASELINE_OR_CURRENT, metricValue);
      }
    }
  }

  public static Row parseAggregationOnlyResponse(TimeOnTimeComparisonRequest comparisonRequest,
      Map<ThirdEyeRequest, PinotThirdEyeResponse> queryResultMap) {
    Map<MetricFunction, Map<String, Double>> metricOnlyData =
        TimeOnTimeResponseParser.processAggregateResponse(queryResultMap);

    Row.Builder rowBuilder = new Row.Builder();
    rowBuilder.setBaselineStart(comparisonRequest.getBaselineStart());
    rowBuilder.setBaselineEnd(comparisonRequest.getBaselineEnd());

    rowBuilder.setCurrentStart(comparisonRequest.getCurrentStart());
    rowBuilder.setCurrentEnd(comparisonRequest.getCurrentEnd());

    for (Entry<MetricFunction, Map<String, Double>> entry : metricOnlyData.entrySet()) {
      MetricFunction key = entry.getKey();
      Map<String, Double> valueMap = entry.getValue();
      rowBuilder.addMetric(key.getMetricName(), valueMap.get(BASELINE_VALUE),
          valueMap.get(CURRENT_VALUE));
    }
    return rowBuilder.build();
  }

  public static List<Row> parseGroupByDimensionResponse(
      TimeOnTimeComparisonRequest comparisonRequest,
      Map<ThirdEyeRequest, PinotThirdEyeResponse> queryResultMap) {
    Map<MetricFunction, Map<String, Double>> metricOnlyData =
        TimeOnTimeResponseParser.processAggregateResponse(queryResultMap);
    // we retrieve only top 25 group by values for each dimension, so we compute the sum of
    // retrieved
    // values, in the end we add a explicit "OTHER" group in each dimension
    Map<MetricFunction, Map<String, Map<String, Double>>> metricSumFromGroupByResponse =
        new HashMap<>();
    // Map<MetricFunction,Map<dimensionName,Map<dimensionValue,Map<(baseline|current), Double>>>>
    Map<MetricFunction, Map<String, Map<String, Map<String, Double>>>> metricGroupByDimensionData =
        TimeOnTimeResponseParser.processGroupByDimensionResponse(queryResultMap);
    // we need a build for each dimensionName, dimensionValue combination
    Map<String, Map<String, Row.Builder>> rowBuildersMap = new LinkedHashMap<>();
    for (Entry<MetricFunction, Map<String, Map<String, Map<String, Double>>>> metricEntry : metricGroupByDimensionData
        .entrySet()) {
      MetricFunction metricFunction = metricEntry.getKey();
      Map<String, Map<String, Map<String, Double>>> dimensionDataMap = metricEntry.getValue();
      for (String dimensionName : dimensionDataMap.keySet()) {
        Map<String, Map<String, Double>> map = dimensionDataMap.get(dimensionName);
        for (String dimensionValue : map.keySet()) {
          Map<String, Double> metricValueMap = map.get(dimensionValue);
          // check if the dimension meets the MIN_CONTRIBUTION_PERCENT, if not this will get added
          // to the OTHER category
          double totalBaselineValue = metricOnlyData.get(metricFunction).get(BASELINE_VALUE);
          double totalCurrentValue = metricOnlyData.get(metricFunction).get(BASELINE_VALUE);
          Double baselineValue = metricValueMap.get(BASELINE_VALUE);
          Double currentValue = metricValueMap.get(CURRENT_VALUE);
          boolean meetsMinContributionThreshold =
              (baselineValue > (totalBaselineValue * MIN_CONTRIBUTION_PERCENT))
                  || (currentValue > totalCurrentValue * MIN_CONTRIBUTION_PERCENT);
          if (!meetsMinContributionThreshold) {
            continue;
          }
          Row.Builder rowBuilder = getRowBuilder(rowBuildersMap, dimensionName, dimensionValue);
          rowBuilder.addMetric(metricFunction.getMetricName(), baselineValue, currentValue);
          // compute for each dimension
          if (!metricSumFromGroupByResponse.containsKey(metricFunction)) {
            Map<String, Map<String, Double>> value = new HashMap<>();
            metricSumFromGroupByResponse.put(metricFunction, value);
          }
          if (!metricSumFromGroupByResponse.get(metricFunction).containsKey(dimensionName)) {
            Map<String, Double> value = new HashMap<>();
            value.put(BASELINE_VALUE, 0d);
            value.put(CURRENT_VALUE, 0d);
            metricSumFromGroupByResponse.get(metricFunction).put(dimensionName, value);
          }
          Map<String, Double> metricSum =
              metricSumFromGroupByResponse.get(metricFunction).get(dimensionName);
          metricSum.put(BASELINE_VALUE, metricSum.get(BASELINE_VALUE) + baselineValue);
          metricSum.put(CURRENT_VALUE, metricSum.get(CURRENT_VALUE) + currentValue);
        }
      }
    }

    for (MetricFunction metricFunction : metricSumFromGroupByResponse.keySet()) {
      Map<String, Map<String, Double>> map = metricSumFromGroupByResponse.get(metricFunction);
      for (String dimensionName : map.keySet()) {
        Map<String, Double> valueMap = map.get(dimensionName);

        double baselineValue =
            metricOnlyData.get(metricFunction).get(BASELINE_VALUE) - valueMap.get(BASELINE_VALUE);
        double currentValue =
            metricOnlyData.get(metricFunction).get(CURRENT_VALUE) - valueMap.get(CURRENT_VALUE);
        double totalBaselineValue = metricOnlyData.get(metricFunction).get(BASELINE_VALUE);
        double totalCurrentValue = metricOnlyData.get(metricFunction).get(BASELINE_VALUE);
        boolean meetsMinContributionThreshold =
            (baselineValue > (totalBaselineValue * MIN_CONTRIBUTION_PERCENT))
                || (currentValue > totalCurrentValue * MIN_CONTRIBUTION_PERCENT);

        // don't add OTHER group if top 25 covered all possible dimensions or if it does not meet
        // minimum threshold
        if (meetsMinContributionThreshold) {
          Builder rowBuilder = getRowBuilder(rowBuildersMap, dimensionName, "other");
          rowBuilder.addMetric(metricFunction.getMetricName(), baselineValue, currentValue);
        }
      }
    }
    List<Row> rows = new ArrayList<>();
    for (Map<String, Builder> dimensionValueMap : rowBuildersMap.values()) {
      for (Row.Builder builder : dimensionValueMap.values()) {
        builder.setBaselineStart(comparisonRequest.getBaselineStart());
        builder.setBaselineEnd(comparisonRequest.getBaselineEnd());

        builder.setCurrentStart(comparisonRequest.getCurrentStart());
        builder.setCurrentEnd(comparisonRequest.getCurrentEnd());

        Row row = builder.build();
        rows.add(row);
      }
    }
    return rows;
  }

  private static Builder getRowBuilder(Map<String, Map<String, Builder>> rowBuildersMap,
      String dimensionName, String dimensionValue) {
    Map<String, Builder> map = rowBuildersMap.get(dimensionName);
    if (map == null) {
      map = new LinkedHashMap<>();
      rowBuildersMap.put(dimensionName, map);
    }
    Builder builder = map.get(dimensionValue);
    if (builder == null) {
      builder = new Row.Builder();
      builder.setDimensionName(dimensionName);
      builder.setDimensionValue(dimensionValue);
      map.put(dimensionValue, builder);
    }
    return builder;
  }

}
