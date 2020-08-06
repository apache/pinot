/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dashboard.resources;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;


/**
 * Provide a table combining metrics data and anomaly feedback for UI representation
 */
@Api(tags = {Constants.ANOMALY_TAG})
@Singleton
public class AnomalyFlattenResource {
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final MetricConfigManager metricConfigDAO;
  private final DatasetConfigManager datasetConfgDAO;

  private final ExecutorService executor;

  public static final String ANOMALY_COMMENT = "comment";
  private static final String DATAFRAME_VALUE = "value";
  private static final String DEFAULT_COMMENT_FORMAT = "#%d is %s";
  private static final int DEFAULT_THREAD_POOLS_SIZE = 5;
  private static final long DEFAULT_TIME_OUT_IN_MINUTES = 1;

  @Inject
  public AnomalyFlattenResource(MergedAnomalyResultManager mergedAnomalyResultDAO,
      DatasetConfigManager datasetConfgDAO,
      MetricConfigManager metricConfigDAO) {
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.datasetConfgDAO = datasetConfgDAO;
    this.metricConfigDAO = metricConfigDAO;
    this.executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOLS_SIZE);
  }


  /**
   * Returns a list of formatted metric values and anomaly comments for UI to generate a table
   * @param metricIds a list of metric ids
   * @param start start time in epoc milliseconds
   * @param end end time in epoc milliseconds
   * @param dimensionKeys a list of keys in dimensions
   * @return a list of formatted metric info and anomaly comments
   */
  @GET
  @ApiOperation(value = "View a collection of metrics and anonalies feedback in a list of maps")
  @Produces("Application/json")
  public List<Map<String, Object>> listDimensionValues(
      @ApiParam("metric config id") @NotNull @QueryParam("metricIds") List<Long> metricIds,
      @ApiParam("start time for anomalies") @QueryParam("start") long start,
      @ApiParam("end time for anomalies") @QueryParam("end") long end,
      @ApiParam("dimension keys") @NotNull @QueryParam("dimensionKeys") List<String> dimensionKeys) throws Exception {
    Preconditions.checkArgument(!metricIds.isEmpty());
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    List<MetricConfigDTO> metrics = new ArrayList<>();
    for (long id : metricIds) {
      metrics.add(metricConfigDAO.findById(id));
      anomalies.addAll(mergedAnomalyResultDAO.findAnomaliesByMetricIdAndTimeRange(id, start, end));
    }

    Map<String, DataFrame> metricDataFrame = new HashMap<>();

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricConfigDAO, datasetConfgDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    Map<String, Future<DataFrame>> futureMap = new HashMap<String, Future<DataFrame>>() {{
      for (MetricConfigDTO metricDTO : metrics) {
        Future<DataFrame> future = fetchAggregatedMetric(aggregationLoader, metricDTO.getId(), start, end,
            dimensionKeys);
        put(metricDTO.getName(), future);
      }
    }};
    for (String metric : futureMap.keySet()) {
      Future<DataFrame> future = futureMap.get(metric);
      metricDataFrame.put(metric, future.get(DEFAULT_TIME_OUT_IN_MINUTES, TimeUnit.MINUTES));
    }

    return reformatDataFrameAndAnomalies(metricDataFrame, anomalies, dimensionKeys);
  }

  /**
   * Return a Future thread with aggregated metric value as return
   * @param aggregationLoader an aggregation loader
   * @param metricId the metric id
   * @param start the start time in epoch time
   * @param end the end time in epoch time
   * @param dimensionKeys the list of dimension keys
   * @return a Future thread with aggregated metric value
   */
  private Future<DataFrame> fetchAggregatedMetric(AggregationLoader aggregationLoader, long metricId, long start,
      long end, List<String> dimensionKeys) {
    return executor.submit(() -> {
      MetricSlice metricSlice = MetricSlice.from(metricId, start, end);
      DataFrame resultDataFrame = null;
      try {
        resultDataFrame = aggregationLoader.loadAggregate(metricSlice, dimensionKeys, -1);
      } catch (Exception e) {
        throw new ExecutionException(e);
      }
      return resultDataFrame;
    });
  }

  /**
   * Reformat the rows in data frame and anomaly information into a list of map
   * @param metricDataFrame a map from metric name to dataframe with aggregated time series
   * @param anomalies a list of anomalies within the same time interval as dataframe
   * @param dimensions a list of dimensions
   * @return a list of maps with dataframe and anomaly information
   */
  public static List<Map<String, Object>> reformatDataFrameAndAnomalies(Map<String, DataFrame> metricDataFrame,
      List<MergedAnomalyResultDTO> anomalies, List<String> dimensions) {
    List<Map<String, Object>> resultList = new ArrayList<>();
    Map<DimensionMap, Map<Long, String>> anomalyCommentMap = extractComments(anomalies);
    List<String> metrics = new ArrayList<>(metricDataFrame.keySet());
    Map<DimensionMap, Map<String, Double>> metricValues = new HashMap<>();
    for (String metric : metrics) {
      DataFrame dataFrame = metricDataFrame.get(metric);
      for (int i = 0; i < dataFrame.size(); i++) {
        DimensionMap dimensionMap = new DimensionMap();
        for (String dimensionKey : dimensions) {
          String dimensionValue = dataFrame.getString(dimensionKey, i);
          dimensionMap.put(dimensionKey, dimensionValue);
        }
        if (!metricValues.containsKey(dimensionMap)) {
          metricValues.put(dimensionMap, new HashMap<>());
        }
        metricValues.get(dimensionMap).put(metric, dataFrame.getDouble(DATAFRAME_VALUE, i));
      }
    }

    for (DimensionMap dimensionMap : metricValues.keySet()) {
      Map<String, Object> resultMap = new ListOrderedMap<>();
      for (String key : dimensionMap.keySet()) {
        resultMap.put(key, dimensionMap.get(key));
      }
      for (String metric : metrics) {
        resultMap.put(metric, metricValues.getOrDefault(dimensionMap, Collections.emptyMap()).getOrDefault(metric, Double.NaN));
      }
      Map<Long, String> comment = Collections.emptyMap();
      if (anomalyCommentMap.containsKey(dimensionMap)) {
        comment = anomalyCommentMap.get(dimensionMap);
      }
      resultMap.put(ANOMALY_COMMENT, comment);

      resultList.add(resultMap);
    }

    return resultList;
  }

  /**
   * Extract comments from anomalies and format it as a map
   * @param anomalies a list of merged anomaly results
   * @return a map from dimension map to a map of anomaly id to its comment
   */
  public static Map<DimensionMap, Map<Long, String>> extractComments (List<MergedAnomalyResultDTO> anomalies) {
    Map<DimensionMap, Map<Long, String>> resultMap = new HashMap<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      DimensionMap dimensions = anomaly.getDimensions();
      if (!resultMap.containsKey(dimensions)) {
        resultMap.put(dimensions, new HashMap<>());
      }
      long anomalyId = anomaly.getId();
      String comment = String.format(DEFAULT_COMMENT_FORMAT, anomalyId, "Not Reviewed Yet");
      AnomalyFeedback feedback = anomaly.getFeedback();
      if (feedback != null) {
        if (StringUtils.isNoneBlank(feedback.getComment())) {
          comment = feedback.getComment();
        } else {
          comment = String.format(DEFAULT_COMMENT_FORMAT, anomalyId, feedback.getFeedbackType().toString());
        }
      }
      resultMap.get(dimensions).put(anomalyId, comment);
    }
    return resultMap;
  }
}
