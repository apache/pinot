package com.linkedin.thirdeye.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;

public class ResponseParserUtils {
  public static ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  public static final Logger LOGGER = LoggerFactory.getLogger(ResponseParserUtils.class);

  public static String TIME_DIMENSION_JOINER_ESCAPED = "\\|";
  public static String TIME_DIMENSION_JOINER = "|";
  public static String OTHER = "OTHER";

  public static Map<String, ThirdEyeResponseRow> createResponseMapByTimeAndDimension(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap;
    responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      responseMap.put(thirdEyeResponseRow.getTimeBucketId() + TIME_DIMENSION_JOINER
          + thirdEyeResponseRow.getDimensions().get(0), thirdEyeResponseRow);
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
    requestBuilder.setCollection(request.getCollection());
    requestBuilder.setStartTimeInclusive(request.getStartTimeInclusive());
    requestBuilder.setEndTimeExclusive(request.getEndTimeExclusive());
    requestBuilder.setFilterSet(request.getFilterSet());
    requestBuilder.setMetricFunctions(request.getMetricFunctions());
    ThirdEyeRequest metricSumsRequest = requestBuilder.build("metricSums");
    ThirdEyeResponse metricSumsResponse = null;
    try {
      metricSumsResponse =
          CACHE_REGISTRY_INSTANCE.getQueryCache().getQueryResult(metricSumsRequest);
    } catch (Exception e) {
      LOGGER.error("Caught exception when executing metric sums request", e);
    }
    List<Double> metricSums = metricSumsResponse.getRow(0).getMetrics();
    return metricSums;
  }

  public static Map<Integer, List<Double>> getMetricSumsByTime(ThirdEyeResponse response) {

    ThirdEyeRequest request = response.getRequest();
    Map<Integer, List<Double>> metricSums = new HashMap<>();
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setCollection(request.getCollection());
    requestBuilder.setStartTimeInclusive(request.getStartTimeInclusive());
    requestBuilder.setEndTimeExclusive(request.getEndTimeExclusive());
    requestBuilder.setFilterSet(request.getFilterSet());
    requestBuilder.setGroupByTimeGranularity(request.getGroupByTimeGranularity());
    requestBuilder.setMetricFunctions(request.getMetricFunctions());
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

  public static String extractDimensionValue(String timeDimensionValue) {
    String[] tokens = timeDimensionValue.split(TIME_DIMENSION_JOINER_ESCAPED);
    String dimensionValue = tokens.length < 2 ? "" : tokens[1];
    return dimensionValue;
  }
}
