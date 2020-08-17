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

package org.apache.pinot.thirdeye.cube.data.dbclient;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.dbrow.Row;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * This class generates query requests to the backend database and retrieve the data for summary algorithm.
 *
 * The generated requests are organized the following tree structure:
 *   Root level by GroupBy dimensions.
 *   Mid  level by "baseline" or "current"; The "baseline" request is ordered before the "current" request.
 *   Leaf level by metric functions; This level is handled by the request itself, i.e., a request can gather multiple
 *   metric functions at the same time.
 * The generated requests are store in a List. Because of the tree structure, the requests belong to the same
 * timeline (baseline or current) are located together. Then, the requests belong to the same GroupBy dimension are
 * located together.
 */
public abstract class BaseCubePinotClient<R extends Row> implements CubePinotClient<R> {
  protected static final Logger LOG = LoggerFactory.getLogger(BaseCubePinotClient.class);

  protected final static DateTime NULL_DATETIME = new DateTime();
  protected final static int TIME_OUT_VALUE = 1200;
  protected final static TimeUnit TIME_OUT_UNIT = TimeUnit.SECONDS;

  protected QueryCache queryCache;
  protected String dataset = "";
  protected DateTime baselineStartInclusive = NULL_DATETIME;
  protected DateTime baselineEndExclusive = NULL_DATETIME;
  protected DateTime currentStartInclusive = NULL_DATETIME;
  protected DateTime currentEndExclusive = NULL_DATETIME;

  /**
   * Constructs a Pinot client.
   *
   * @param queryCache the query cached to Pinot.
   */
  public BaseCubePinotClient(QueryCache queryCache) {
    this.queryCache = Preconditions.checkNotNull(queryCache);
  }

  public void setDataset(String dataset) {
    this.dataset = Preconditions.checkNotNull(dataset);
  }

  @Override
  public void setBaselineStartInclusive(DateTime dateTime) {
    baselineStartInclusive = Preconditions.checkNotNull(dateTime);
  }

  @Override
  public void setBaselineEndExclusive(DateTime dateTime) {
    baselineEndExclusive = Preconditions.checkNotNull(dateTime);
  }

  @Override
  public void setCurrentStartInclusive(DateTime dateTime) {
    currentStartInclusive = Preconditions.checkNotNull(dateTime);
  }

  @Override
  public void setCurrentEndExclusive(DateTime dateTime) {
    currentEndExclusive = Preconditions.checkNotNull(dateTime);
  }

  /**
   * Construct bulks ThirdEye requests.
   *
   * @param dataset the data set to be queries.
   * @param cubeSpecs the spec to retrieve the metrics.
   * @param groupBy groupBy for database.
   * @param filterSets the data filter.
   * @return a list of ThirdEye requests.
   */
  protected static Map<CubeTag, ThirdEyeRequestMetricExpressions> constructBulkRequests(String dataset,
      List<CubeSpec> cubeSpecs, List<String> groupBy, Multimap<String, String> filterSets) throws ExecutionException {

    Map<CubeTag, ThirdEyeRequestMetricExpressions> requests = new HashMap<>();
    MetricConfigDTO metricConfigDTO = null;
    if (filterSets.containsKey("metric")) {
      Collection<String> metric = filterSets.get("metric");
      MetricConfigManager metricConfigDAO = DAORegistry.getInstance().getMetricConfigDAO();
      metricConfigDTO = metricConfigDAO.findByMetricAndDataset((String) metric.toArray()[0], dataset);
      filterSets.removeAll("metric");
    }

    for (CubeSpec cubeSpec : cubeSpecs) {
      // Set dataset and metric
      List<MetricExpression> metricExpressions =
          Utils.convertToMetricExpressions(cubeSpec.getMetric(),
                  metricConfigDTO == null ? MetricAggFunction.AVG : metricConfigDTO.getDefaultAggFunction(), dataset);
      List<MetricFunction> metricFunctions = metricExpressions.get(0).computeMetricFunctions();

      ThirdEyeRequest.ThirdEyeRequestBuilder builder = ThirdEyeRequest.newBuilder();

      builder.setMetricFunctions(metricFunctions);
      builder.setDataSource(ThirdEyeUtils.getDataSourceFromMetricFunctions(metricFunctions));

      // Set start and end time
      builder.setStartTimeInclusive(cubeSpec.getStartInclusive());
      builder.setEndTimeExclusive(cubeSpec.getEndExclusive());

      // Set groupBy and filter
      builder.setGroupBy(groupBy);
      builder.setFilterSet(filterSets);

      requests.put(cubeSpec.getTag(),
          new ThirdEyeRequestMetricExpressions(builder.build(cubeSpec.getTag().toString()), metricExpressions));
    }

    return requests;
  }

  /**
   * The cube specs that specified which metric and dataset to be queried.
   *
   * @return a list of cube spec.
   */
  protected abstract List<CubeSpec> getCubeSpecs();

  /**
   * Fills in multiple Pinot results to one Cube row.
   *
   * @param rowTable the table from dimension values to cube row; the return of this method.
   * @param dimensions the dimension names of the row.
   * @param dimensionValues the dimension values of the row.
   * @param value the value to be filled in to the row.
   * @param tag The field of the row where the value is filled in.
   */
  protected abstract void fillValueToRowTable(Map<List<String>, R> rowTable, Dimensions dimensions,
      List<String> dimensionValues, double value, CubeTag tag);

  /**
   * Returns a list of rows. The value of each row is evaluated and no further processing is needed.
   * @param dimensions dimensions of the response
   * @param response the response from backend database
   * @param rowTable the storage for rows
   * @param tag true if the response is for baseline values
   */
  protected void buildMetricFunctionOrExpressionsRows(Dimensions dimensions, List<MetricExpression> metricExpressions,
      List<MetricFunction> metricFunctions, ThirdEyeResponse response, Map<List<String>, R> rowTable, CubeTag tag) {
    Map<String, Double> context = new HashMap<>();
    for (int rowIdx = 0; rowIdx < response.getNumRows(); ++rowIdx) {
      double value = 0d;
      // If the metric expression is a single metric function, then we get the value immediately
      if (metricFunctions.size() <= 1) {
        value = response.getRow(rowIdx).getMetrics().get(0);
      } else { // Otherwise, we need to evaluate the expression
        for (int metricFuncIdx = 0; metricFuncIdx < metricFunctions.size(); ++metricFuncIdx) {
          double contextValue = response.getRow(rowIdx).getMetrics().get(metricFuncIdx);
          context.put(metricFunctions.get(metricFuncIdx).getMetricName(), contextValue);
        }
        try {
          value = MetricExpression.evaluateExpression(metricExpressions.get(0), context);
        } catch (Exception e) {
          LOG.warn(e.getMessage());
        }
      }
      List<String> dimensionValues = response.getRow(rowIdx).getDimensions();
      fillValueToRowTable(rowTable, dimensions, dimensionValues, value, tag);
    }
  }

  /**
   * Converts Pinot results to Cube Rows.
   *
   * @param dimensions the dimension of the Pinot results.
   * @param bulkRequests the original requests of those results.
   * @return Cube rows.
   */
  protected List<List<R>> constructAggregatedValues(Dimensions dimensions,
      List<Map<CubeTag, ThirdEyeRequestMetricExpressions>> bulkRequests) throws Exception {

    List<ThirdEyeRequest> allRequests = new ArrayList<>();
    for (Map<CubeTag, ThirdEyeRequestMetricExpressions> bulkRequest : bulkRequests) {
      for (Map.Entry<CubeTag, ThirdEyeRequestMetricExpressions> entry : bulkRequest.entrySet()) {
        ThirdEyeRequest thirdEyeRequest = entry.getValue().getThirdEyeRequest();
        allRequests.add(thirdEyeRequest);
      }
    }

    Map<ThirdEyeRequest, Future<ThirdEyeResponse>> queryResponses = queryCache.getQueryResultsAsync(allRequests);

    List<List<R>> res = new ArrayList<>();
    int level = 0;
    for (Map<CubeTag, ThirdEyeRequestMetricExpressions> bulkRequest : bulkRequests) {
      Map<List<String>, R> rowOfSameLevel = new HashMap<>();

      for (Map.Entry<CubeTag, ThirdEyeRequestMetricExpressions> entry : bulkRequest.entrySet()) {
        CubeTag tag = entry.getKey();
        ThirdEyeRequest thirdEyeRequest = entry.getValue().getThirdEyeRequest();
        ThirdEyeResponse thirdEyeResponse = queryResponses.get(thirdEyeRequest).get(TIME_OUT_VALUE, TIME_OUT_UNIT);
        if (thirdEyeResponse.getNumRows() == 0) {
          LOG.warn("Get 0 rows from the request(s): {}", thirdEyeRequest);
        }
        List<MetricExpression> metricExpressions = entry.getValue().getMetricExpressions();
        buildMetricFunctionOrExpressionsRows(dimensions, metricExpressions, thirdEyeRequest.getMetricFunctions(),
            thirdEyeResponse, rowOfSameLevel, tag);
      }
      if (rowOfSameLevel.size() == 0) {
        LOG.warn("Failed to retrieve non-zero results for requests of level {}.", level);
      }
      List<R> rows = new ArrayList<>(rowOfSameLevel.values());
      res.add(rows);
      ++level;
    }

    return res;
  }

  @Override
  public R getTopAggregatedValues(Multimap<String, String> filterSets) throws Exception {
    List<String> groupBy = Collections.emptyList();
    List<Map<CubeTag, ThirdEyeRequestMetricExpressions>> bulkRequests = Collections.singletonList(
        BaseCubePinotClient.constructBulkRequests(dataset, getCubeSpecs(), groupBy, filterSets));
    return constructAggregatedValues(new Dimensions(), bulkRequests).get(0).get(0);
  }

  @Override
  public List<List<R>> getAggregatedValuesOfDimension(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception {
    List<Map<CubeTag, ThirdEyeRequestMetricExpressions>> bulkRequests = new ArrayList<>();
    for (int level = 0; level < dimensions.size(); ++level) {
      List<String> groupBy = Lists.newArrayList(dimensions.get(level));
      bulkRequests.add(BaseCubePinotClient.constructBulkRequests(dataset, getCubeSpecs(), groupBy, filterSets));
    }
    return constructAggregatedValues(dimensions, bulkRequests);
  }

  @Override
  public List<List<R>> getAggregatedValuesOfLevels(Dimensions dimensions, Multimap<String, String> filterSets)
      throws Exception {
    List<Map<CubeTag, ThirdEyeRequestMetricExpressions>> bulkRequests = new ArrayList<>();
    for (int level = 0; level < dimensions.size() + 1; ++level) {
      List<String> groupBy = Lists.newArrayList(dimensions.namesToDepth(level));
      bulkRequests.add(BaseCubePinotClient.constructBulkRequests(dataset, getCubeSpecs(), groupBy, filterSets));
    }
    return constructAggregatedValues(dimensions, bulkRequests);
  }
}
