package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.constant.MetricAggFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfig;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;

public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  private static String DEFAULT_DASHBOARD = "Default_Dashboard";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry
      .getInstance();

  public static List<ThirdEyeRequest> generateRequests(String collection, String requestReference,
      MetricFunction metricFunction, List<String> dimensions, DateTime start, DateTime end) {

    List<ThirdEyeRequest> requests = new ArrayList<>();

    for (String dimension : dimensions) {
      ThirdEyeRequestBuilder requestBuilder = new ThirdEyeRequestBuilder();
      requestBuilder.setCollection(collection);
      List<MetricFunction> metricFunctions = Arrays.asList(metricFunction);
      requestBuilder.setMetricFunctions(metricFunctions);

      requestBuilder.setStartTimeInclusive(start);
      requestBuilder.setEndTimeExclusive(end);
      requestBuilder.setGroupBy(dimension);

      ThirdEyeRequest request = requestBuilder.build(requestReference);
      requests.add(request);
    }

    return requests;
  }

  public static Map<String, List<String>> getFilters(QueryCache queryCache, String collection,
      String requestReference, String metricName, List<String> dimensions, DateTime start,
      DateTime end) throws Exception {

    MetricFunction metricFunction = new MetricFunction(MetricAggFunction.COUNT, "*");

    List<ThirdEyeRequest> requests =
        generateRequests(collection, requestReference, metricFunction, dimensions, start, end);

    Map<ThirdEyeRequest, Future<ThirdEyeResponse>> queryResultMap =
        queryCache.getQueryResultsAsync(requests);

    Map<String, List<String>> result = new HashMap<>();
    for (Map.Entry<ThirdEyeRequest, Future<ThirdEyeResponse>> entry : queryResultMap.entrySet()) {
      ThirdEyeRequest request = entry.getKey();
      String dimension = request.getGroupBy().get(0);
      ThirdEyeResponse thirdEyeResponse = entry.getValue().get();
      int n = thirdEyeResponse.getNumRowsFor(metricFunction);

      List<String> values = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        Map<String, String> row = thirdEyeResponse.getRow(metricFunction, i);
        String dimensionValue = row.get(dimension);
        values.add(dimensionValue);
      }

      result.put(dimension, values);

    }

    return result;
  }

  public static List<String> getDimensions(QueryCache queryCache, String collection)
      throws Exception {
    CollectionSchema schema = queryCache.getClient().getCollectionSchema(collection);
    List<String> dimensions = schema.getDimensionNames();
    Collections.sort(dimensions);

    return dimensions;
  }

  public static List<String> getDimensionsToGroupBy(QueryCache queryCache, String collection,
      Multimap<String, String> filters) throws Exception {
    List<String> dimensions = Utils.getDimensions(queryCache, collection);

    List<String> dimensionsToGroupBy = new ArrayList<>();
    if (filters != null) {
      Set<String> filterDimenions = filters.keySet();
      for (String dimension : dimensions) {
        if (!filterDimenions.contains(dimension)) {
          // dimensions.remove(dimension);
          dimensionsToGroupBy.add(dimension);
        }
      }
    } else {
      return dimensions;
    }

    return dimensionsToGroupBy;
  }

  public static List<String> getDashboards(WebappConfigManager webappConfigDAO, String collection) throws Exception {
    List<WebappConfigDTO> webappConfigs = webappConfigDAO
        .findByCollectionAndType(collection, WebappConfigType.DASHBOARD_CONFIG);

    List<String> dashboards = new ArrayList<>();
    for (WebappConfigDTO webappConfig : webappConfigs) {
      DashboardConfig dashboardConfig = AbstractConfig.fromJSON(webappConfig.getConfig(), DashboardConfig.class);
      dashboards.add(dashboardConfig.getDashboardName());
    }

    dashboards.add(DEFAULT_DASHBOARD);
    return dashboards;
  }


  public static List<MetricExpression> convertToMetricExpressions(String metricsJson,
      String collection) {

    CollectionConfig collectionConfig = null;
    try {
      collectionConfig = CACHE_REGISTRY_INSTANCE.getCollectionConfigCache().get(collection);
    } catch (InvalidCacheLoadException | ExecutionException e) {
      LOG.debug("No collection configs for collection {}", collection);
    }
    List<MetricExpression> metricExpressions = new ArrayList<>();
    if (metricsJson == null) {
      return metricExpressions;
    }
    ArrayList<String> metricExpressionNames;
    try {
      TypeReference<ArrayList<String>> valueTypeRef = new TypeReference<ArrayList<String>>() {
      };

      metricExpressionNames = OBJECT_MAPPER.readValue(metricsJson, valueTypeRef);
    } catch (Exception e) {
      LOG.error("Error parsing metrics json: {} errorMessage:{}", metricsJson, e.getMessage());
      metricExpressionNames = new ArrayList<>();
      String[] metrics = metricsJson.split(",");
      for (String metric : metrics) {
        metricExpressionNames.add(metric);
      }
    }
    for (String metricExpressionName : metricExpressionNames) {
      if (collectionConfig != null && collectionConfig.getDerivedMetrics() != null
          && collectionConfig.getDerivedMetrics().containsKey(metricsJson)) {
        String metricExpressionString =
            collectionConfig.getDerivedMetrics().get(metricExpressionName);
        metricExpressions.add(new MetricExpression(metricExpressionName, metricExpressionString));
      } else {
        metricExpressions.add(new MetricExpression(metricExpressionName));
      }

    }

    return metricExpressions;
  }

  public static List<MetricFunction> computeMetricFunctionsFromExpressions(
      List<MetricExpression> metricExpressions) {
    Set<MetricFunction> metricFunctions = new HashSet<>();

    for (MetricExpression expression : metricExpressions) {
      metricFunctions.addAll(expression.computeMetricFunctions());
    }
    return Lists.newArrayList(metricFunctions);
  }

  public static TimeGranularity getAggregationTimeGranularity(String aggTimeGranularity) {

    TimeGranularity timeGranularity;
    if (aggTimeGranularity.indexOf("_") > -1) {
      String[] split = aggTimeGranularity.split("_");
      timeGranularity = new TimeGranularity(Integer.parseInt(split[0]), TimeUnit.valueOf(split[1]));
    } else {
      timeGranularity = new TimeGranularity(1, TimeUnit.valueOf(aggTimeGranularity));
    }
    return timeGranularity;
  }

  public static List<MetricExpression> convertToMetricExpressions(
      List<MetricFunction> metricFunctions) {
    List<MetricExpression> metricExpressions = new ArrayList<>();
    for (MetricFunction function : metricFunctions) {
      metricExpressions.add(new MetricExpression(function.getMetricName()));
    }
    return metricExpressions;
  }
}
