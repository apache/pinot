package com.linkedin.thirdeye.datasource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.datasource.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class ResponseParserUtils {
  public static ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  public static final Logger LOGGER = LoggerFactory.getLogger(ResponseParserUtils.class);

  public static String TIME_DIMENSION_JOINER_ESCAPED = "\\|";
  public static String TIME_DIMENSION_JOINER = "|";
  public static String OTHER = "OTHER";
  public static String UNKNOWN = "UNKNOWN";

  public static Map<String, ThirdEyeResponseRow> createResponseMapByTimeAndDimension(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      String key =
          computeTimeDimensionValues(thirdEyeResponseRow.getTimeBucketId(), thirdEyeResponseRow.getDimensions());
      responseMap.put(key, thirdEyeResponseRow);
    }
    return responseMap;
  }

  public static Map<String, ThirdEyeResponseRow> createResponseMapByTime(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap;
    responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      responseMap.put(String.valueOf(thirdEyeResponseRow.getTimeBucketId()), thirdEyeResponseRow);
    }
    return responseMap;
  }

  public static Map<String, ThirdEyeResponseRow> createResponseMapByDimension(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap;
    responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      responseMap.put(thirdEyeResponseRow.getDimensions().get(0), thirdEyeResponseRow);
    }
    return responseMap;
  }

  public static List<Double> getMetricSums(ThirdEyeResponse response) {

    ThirdEyeRequest request = response.getRequest();
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setStartTimeInclusive(request.getStartTimeInclusive());
    requestBuilder.setEndTimeExclusive(request.getEndTimeExclusive());
    requestBuilder.setFilterSet(request.getFilterSet());
    requestBuilder.setMetricFunctions(request.getMetricFunctions());
    requestBuilder.setDataSource(ThirdEyeUtils.getDataSourceFromMetricFunctions(request.getMetricFunctions()));
    ThirdEyeRequest metricSumsRequest = requestBuilder.build("metricSums");
    try {
      ThirdEyeResponse metricSumsResponse = CACHE_REGISTRY_INSTANCE.getQueryCache().getQueryResult(metricSumsRequest);
      return metricSumsResponse.getRow(0).getMetrics();
    } catch (Exception e) {
      LOGGER.error("Caught exception when executing metric sums request", e);
    }
    return Collections.emptyList();
  }

  public static Map<Integer, List<Double>> getMetricSumsByTime(ThirdEyeResponse response) {

    ThirdEyeRequest request = response.getRequest();
    Map<Integer, List<Double>> metricSums = new HashMap<>();
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setStartTimeInclusive(request.getStartTimeInclusive());
    requestBuilder.setEndTimeExclusive(request.getEndTimeExclusive());
    requestBuilder.setFilterSet(request.getFilterSet());
    requestBuilder.setGroupByTimeGranularity(request.getGroupByTimeGranularity());
    requestBuilder.setMetricFunctions(request.getMetricFunctions());
    requestBuilder.setDataSource(ThirdEyeUtils.getDataSourceFromMetricFunctions(request.getMetricFunctions()));
    ThirdEyeRequest metricSumsRequest = requestBuilder.build("metricSums");
    ThirdEyeResponse metricSumsResponse = null;
    try {
      metricSumsResponse =
          CACHE_REGISTRY_INSTANCE.getQueryCache().getQueryResult(metricSumsRequest);
    } catch (Exception e) {
      LOGGER.error("Caught exception when executing metric sums request", e);
    }

    for (int i = 0; i < metricSumsResponse.getNumRows(); i++) {
      ThirdEyeResponseRow row = metricSumsResponse.getRow(i);
      metricSums.put(row.getTimeBucketId(), row.getMetrics());
    }
    return metricSums;
  }

  public static String computeTimeDimensionValue(int timeBucketId, String dimensionValue) {
    return timeBucketId + TIME_DIMENSION_JOINER + dimensionValue;
  }

  public static String computeTimeDimensionValues(int timeBucketId, List<String> dimensionValues) {
    if (dimensionValues == null || dimensionValues.size() == 0) {
      return Integer.toString(timeBucketId);
    } else if (dimensionValues.size() == 1) {
      return computeTimeDimensionValue(timeBucketId, dimensionValues.get(0));
    } else {
      StringBuilder sb = new StringBuilder(Integer.toString(timeBucketId)).append(TIME_DIMENSION_JOINER);
      String separator = "";
      for (String dimensionValue : dimensionValues) {
        sb.append(separator).append(dimensionValue);
        separator = TIME_DIMENSION_JOINER;
      }
      return sb.toString();
    }
  }

  public static String extractFirstDimensionValue(String timeDimensionValue) {
    String[] tokens = timeDimensionValue.split(TIME_DIMENSION_JOINER_ESCAPED);
    String dimensionValue = tokens.length < 2 ? "" : tokens[1];
    return dimensionValue;
  }

  public static List<String> extractDimensionValues(String timeDimensionValues) {
    String[] tokens = timeDimensionValues.split(TIME_DIMENSION_JOINER_ESCAPED);
    if (tokens.length < 2) {
      return Collections.emptyList();
    } else {
      List<String> res = new ArrayList<>(tokens.length - 1);
      for (int i = 1; i < tokens.length; ++i) {
        res.add(tokens[i]);
      }
      return res;
    }
  }
}
