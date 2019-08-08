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
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
 * Flatten the anomaly results for UI purpose.
 * Convert a list of anomalies to rows of columns so that UI can directly convert the anomalies to table
 */
@Path("thirdeye/table")
@Api(tags = {Constants.ANOMALY_TAG})
public class AnomalyFlattenResource {
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;
  private final MetricConfigManager metricConfigDAO;
  private final DatasetConfigManager datasetConfgDAO;
  public static final String ANOMALY_ID = "anomalyId";
  public static final String ANOMALY_COMMENT = "comment";
  private static final String DATAFRAME_VALUE = "value";
  private static final String DEFAULT_COMMENT_FORMAT = "#%d is %s";

  public AnomalyFlattenResource(MergedAnomalyResultManager mergedAnomalyResultDAO, DatasetConfigManager datasetConfgDAO,
      MetricConfigManager metricConfigDAO) {
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.datasetConfgDAO = datasetConfgDAO;
    this.metricConfigDAO = metricConfigDAO;
  }


  /**
   * Returns a list of formatted metric values and anomaly comments for UI to generate a table
   * @param metricIdStr a string of metric ids separated by comma
   * @param start start time in epoc milliseconds
   * @param end end time in epoc milliseconds
   * @param dimensionStrings a list of keys in dimensions joined by comma
   * @return a list of formatted metric info and anomaly comments
   */
  @GET
  @ApiOperation(value = "View a flatted merged anomalies for collection")
  @Produces("Application/json")
  public List<Map<String, Object>> listDimensionValues(
      @ApiParam("metric config id") @NotNull @QueryParam("metricIds") String metricIdStr,
      @ApiParam("start time for anomalies") @QueryParam("start") long start,
      @ApiParam("end time for anomalies") @QueryParam("end") long end,
      @ApiParam("dimension keys") @NotNull @QueryParam("dimensionKeys") String dimensionStrings) throws Exception {
    Preconditions.checkArgument(StringUtils.isNotBlank(metricIdStr));
    List<String> dimensionKeys = Arrays.asList(dimensionStrings.split(","));
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    List<MetricConfigDTO> metrics = new ArrayList<>();
    for (String metricId : metricIdStr.split(",")) {
      long id = Long.valueOf(metricId);
      metrics.add(metricConfigDAO.findById(id));
      anomalies.addAll(mergedAnomalyResultDAO.findAnomaliesByMetricIdAndTimeRange(id, start, end));
    }

    Map<String, DataFrame> metricDataFrame = new HashMap<>();

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricConfigDAO, datasetConfgDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    for (MetricConfigDTO metricDTO : metrics) {
      MetricSlice metricSlice = MetricSlice.from(metricDTO.getId(), start, end);
      DataFrame dataFrame = aggregationLoader.loadAggregate(metricSlice, dimensionKeys, -1);
      metricDataFrame.put(metricDTO.getName(), dataFrame);
    }

    return reformatDataFrameAndAnomalies(metricDataFrame, anomalies, dimensionKeys);
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
      Map<String, Object> resultMap = new HashMap<>();
      for (String key : dimensionMap.keySet()) {
        resultMap.put(key, dimensionMap.get(key));
      }
      for (String metric : metrics) {
        if (metricValues.containsKey(dimensionMap)) {
          resultMap.put(metric, metricValues.get(dimensionMap).getOrDefault(metric, Double.NaN));
        } else {
          resultMap.put(metric, Double.NaN);
        }
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
