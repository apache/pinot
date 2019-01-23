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

package org.apache.pinot.thirdeye.dashboard.views.heatmap;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.dashboard.views.GenericResponse;
import org.apache.pinot.thirdeye.dashboard.views.GenericResponse.Info;
import org.apache.pinot.thirdeye.dashboard.views.GenericResponse.ResponseSchema;
import org.apache.pinot.thirdeye.dashboard.views.ViewHandler;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.comparison.Row;
import org.apache.pinot.thirdeye.datasource.comparison.TimeOnTimeComparisonHandler;
import org.apache.pinot.thirdeye.datasource.comparison.TimeOnTimeComparisonRequest;
import org.apache.pinot.thirdeye.datasource.comparison.TimeOnTimeComparisonResponse;
import org.apache.pinot.thirdeye.datasource.comparison.Row.Metric;


public class HeatMapViewHandler implements ViewHandler<HeatMapViewRequest, HeatMapViewResponse> {

  private final QueryCache queryCache;
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();
  private static final String RATIO_SEPARATOR = "/";
  private static final String TOPK = "_topk";

  public HeatMapViewHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  private TimeOnTimeComparisonRequest generateTimeOnTimeComparisonRequest(HeatMapViewRequest request)
      throws Exception {

    TimeOnTimeComparisonRequest comparisonRequest = new TimeOnTimeComparisonRequest();
    String collection = request.getCollection();
    DateTime baselineStart = request.getBaselineStart();
    DateTime baselineEnd = request.getBaselineEnd();
    DateTime currentStart = request.getCurrentStart();
    DateTime currentEnd = request.getCurrentEnd();
    comparisonRequest.setEndDateInclusive(false);

    Multimap<String, String> filters = request.getFilters();
    List<String> dimensionsToGroupBy = Utils.getDimensionsToGroupBy(collection, filters);

    List<MetricExpression> metricExpressions = request.getMetricExpressions();
    comparisonRequest.setCollectionName(collection);
    comparisonRequest.setBaselineStart(baselineStart);
    comparisonRequest.setBaselineEnd(baselineEnd);
    comparisonRequest.setCurrentStart(currentStart);
    comparisonRequest.setCurrentEnd(currentEnd);
    comparisonRequest.setGroupByDimensions(dimensionsToGroupBy);
    comparisonRequest.setFilterSet(filters);
    comparisonRequest.setMetricExpressions(metricExpressions);
    comparisonRequest.setAggregationTimeGranularity(null);
    return comparisonRequest;
  }

  @Override
  public HeatMapViewResponse process(HeatMapViewRequest request) throws Exception {

    // query 1 for everything from baseline start to baseline end
    // query 2 for everything from current start to current end
    // for each dimension group by top 100
    // query 1 for everything from baseline start to baseline end
    // query for everything from current start to current end

    List<String> expressionNames = new ArrayList<>();
    Map<String, String> metricExpressions = new HashMap<>();
    Set<String> metricOrExpressionNames = new HashSet<>();
    for (MetricExpression expression : request.getMetricExpressions()) {
      expressionNames.add(expression.getExpressionName());
      metricExpressions.put(expression.getExpressionName(), expression.getExpression());
      metricOrExpressionNames.add(expression.getExpressionName());
      List<MetricFunction> metricFunctions = expression.computeMetricFunctions();
      for (MetricFunction function : metricFunctions) {
        metricOrExpressionNames.add(function.getMetricName());
      }
    }

    Map<String, HeatMap.Builder> data = new HashMap<>();

    TimeOnTimeComparisonRequest comparisonRequest = generateTimeOnTimeComparisonRequest(request);
    List<String> groupByDimensions = comparisonRequest.getGroupByDimensions();
    List<String> groupByDimensionsFiltered = new ArrayList<>();
    // remove group by dimensions which have topk as well
    for (String groupByDimension : groupByDimensions) {
      if (!groupByDimensions.contains(groupByDimension + TOPK)) {
        groupByDimensionsFiltered.add(groupByDimension);
      }
    }
    final TimeOnTimeComparisonHandler handler = new TimeOnTimeComparisonHandler(queryCache);

    // we are tracking per dimension, to validate that its the same for each dimension
    Map<String, Map<String, Double>> baselineTotalPerMetricAndDimension = new HashMap<>();
    Map<String, Map<String, Double>> currentTotalPerMetricAndDimension = new HashMap<>();

    for (String metricOrExpressionName : metricOrExpressionNames) {
      Map<String, Double> baselineTotalMap = new HashMap<>();
      Map<String, Double> currentTotalMap = new HashMap<>();
      baselineTotalPerMetricAndDimension.put(metricOrExpressionName, baselineTotalMap);
      currentTotalPerMetricAndDimension.put(metricOrExpressionName, currentTotalMap);
      for (String dimension : groupByDimensionsFiltered) {
        baselineTotalMap.put(dimension, 0d);
        currentTotalMap.put(dimension, 0d);
      }
    }

    List<Future<TimeOnTimeComparisonResponse>> timeOnTimeComparisonResponsesFutures =
        getTimeOnTimeComparisonResponses(groupByDimensionsFiltered, comparisonRequest, handler);

    for (int groupByDimensionId = 0; groupByDimensionId < groupByDimensionsFiltered.size(); groupByDimensionId++) {
      String groupByDimension = groupByDimensionsFiltered.get(groupByDimensionId);

      TimeOnTimeComparisonResponse response =
          timeOnTimeComparisonResponsesFutures.get(groupByDimensionId).get();

      int numRows = response.getNumRows();
      for (int i = 0; i < numRows; i++) {

        Row row = response.getRow(i);
        String dimensionValue = row.getDimensionValue();
        Map<String, Metric> metricMap = new HashMap<>();
        for (Metric metric : row.getMetrics()) {
          metricMap.put(metric.getMetricName(), metric);
        }
        for (Metric metric : row.getMetrics()) {
          String metricName = metric.getMetricName();
          // update the baselineTotal and current total
          Map<String, Double> baselineTotalMap = baselineTotalPerMetricAndDimension.get(metricName);
          Map<String, Double> currentTotalMap = currentTotalPerMetricAndDimension.get(metricName);

          baselineTotalMap.put(groupByDimension,
              baselineTotalMap.get(groupByDimension) + metric.getBaselineValue());
          currentTotalMap.put(groupByDimension,
              currentTotalMap.get(groupByDimension) + metric.getCurrentValue());

          if (!expressionNames.contains(metricName)) {
            continue;
          }
          String dataKey = metricName + "." + groupByDimension;
          HeatMap.Builder heatMapBuilder = data.get(dataKey);
          if (heatMapBuilder == null) {
            heatMapBuilder = new HeatMap.Builder(groupByDimension);
            data.put(dataKey, heatMapBuilder);
          }
          MetricDataset metricDataset = new MetricDataset(metricName, comparisonRequest.getCollectionName());
          MetricConfigDTO metricConfig = CACHE_REGISTRY.getMetricConfigCache().get(metricDataset);
          if (StringUtils.isNotBlank(metricConfig.getCellSizeExpression())) {

            String metricExpression = metricExpressions.get(metricName);

            String[] tokens = metricExpression.split(RATIO_SEPARATOR);
            String numerator = tokens[0];
            String denominator = tokens[1];
            Metric numeratorMetric = metricMap.get(numerator);
            Metric denominatorMetric = metricMap.get(denominator);
            Double numeratorBaseline =
                numeratorMetric == null ? 0 : numeratorMetric.getBaselineValue();
            Double numeratorCurrent =
                numeratorMetric == null ? 0 : numeratorMetric.getCurrentValue();
            Double denominatorBaseline =
                denominatorMetric == null ? 0 : denominatorMetric.getBaselineValue();
            Double denominatorCurrent =
                denominatorMetric == null ? 0 : denominatorMetric.getCurrentValue();

            Map<String, Double> context = new HashMap<>();
            context.put(numerator, numeratorCurrent);
            context.put(denominator, denominatorCurrent);
            String cellSizeExpression = metricConfig.getCellSizeExpression();
            Double cellSize = MetricExpression.evaluateExpression(cellSizeExpression, context);

            heatMapBuilder.addCell(dimensionValue, metric.getBaselineValue(),
                metric.getCurrentValue(), cellSize, cellSizeExpression, numeratorBaseline,
                denominatorBaseline, numeratorCurrent, denominatorCurrent);
          } else {
            heatMapBuilder.addCell(dimensionValue, metric.getBaselineValue(),
                metric.getCurrentValue());
          }
        }
      }
    }

    ResponseSchema schema = new ResponseSchema();

    String[] columns = HeatMapCell.columns();
    for (int i = 0; i < columns.length; i++) {
      String column = columns[i];
      schema.add(column, i);
    }
    Info summary = new Info();

    Map<String, GenericResponse> heatMapViewResponseData = new HashMap<>();
    for (MetricExpression expression : request.getMetricExpressions()) {
      List<MetricFunction> metricFunctions = expression.computeMetricFunctions();
      Double baselineTotal =
          baselineTotalPerMetricAndDimension.get(expression.getExpressionName()).values()
              .iterator().next();
      Double currentTotal =
          currentTotalPerMetricAndDimension.get(expression.getExpressionName()).values().iterator()
              .next();

      // check if its derived
      if (metricFunctions.size() > 1) {
        Map<String, Double> baselineContext = new HashMap<>();
        Map<String, Double> currentContext = new HashMap<>();
        for (String metricOrExpression : metricOrExpressionNames) {
          baselineContext
              .put(metricOrExpression, baselineTotalPerMetricAndDimension.get(metricOrExpression)
                  .values().iterator().next());
          currentContext.put(metricOrExpression,
              currentTotalPerMetricAndDimension.get(metricOrExpression).values().iterator().next());
        }
        baselineTotal = MetricExpression.evaluateExpression(expression, baselineContext);
        currentTotal = MetricExpression.evaluateExpression(expression, currentContext);
      } else {
        baselineTotal =
            baselineTotalPerMetricAndDimension.get(expression.getExpressionName()).values()
                .iterator().next();
        currentTotal =
            currentTotalPerMetricAndDimension.get(expression.getExpressionName()).values()
                .iterator().next();
      }
      summary.addSimpleField("baselineStart", Long.toString(comparisonRequest.getBaselineStart().getMillis()));
      summary.addSimpleField("baselineEnd", Long.toString(comparisonRequest.getBaselineEnd().getMillis()));
      summary.addSimpleField("currentStart", Long.toString(comparisonRequest.getCurrentStart().getMillis()));
      summary.addSimpleField("currentEnd", Long.toString(comparisonRequest.getCurrentEnd().getMillis()));

      summary.addSimpleField("baselineTotal", HeatMapCell.format(baselineTotal));
      summary.addSimpleField("currentTotal", HeatMapCell.format(currentTotal));
      summary.addSimpleField("deltaChange", HeatMapCell.format(currentTotal - baselineTotal));
      summary.addSimpleField("deltaPercentage",
          HeatMapCell.format((currentTotal - baselineTotal) * 100.0 / baselineTotal));
    }

    for (Entry<String, HeatMap.Builder> entry : data.entrySet()) {
      String dataKey = entry.getKey();
      GenericResponse heatMapResponse = new GenericResponse();
      List<String[]> heatMapResponseData = new ArrayList<>();
      HeatMap.Builder builder = entry.getValue();
      HeatMap heatMap = builder.build();
      for (HeatMapCell cell : heatMap.heatMapCells) {
        String[] newRowData = cell.toArray();
        heatMapResponseData.add(newRowData);
      }
      heatMapResponse.setSchema(schema);
      heatMapResponse.setResponseData(heatMapResponseData);

      heatMapViewResponseData.put(dataKey, heatMapResponse);

    }

    HeatMapViewResponse heatMapViewResponse = new HeatMapViewResponse();
    heatMapViewResponse.setMetrics(expressionNames);
    heatMapViewResponse.setDimensions(groupByDimensionsFiltered);
    heatMapViewResponse.setData(heatMapViewResponseData);
    heatMapViewResponse.setMetricExpression(metricExpressions);
    heatMapViewResponse.setSummary(summary);

    return heatMapViewResponse;
  }

  private TimeOnTimeComparisonRequest getComparisonRequestByDimension(
      TimeOnTimeComparisonRequest comparisonRequest, String groupByDimension) {
    TimeOnTimeComparisonRequest request = new TimeOnTimeComparisonRequest(comparisonRequest);
    request.setGroupByDimensions(Lists.newArrayList(groupByDimension));
    return request;
  }

  private List<Future<TimeOnTimeComparisonResponse>> getTimeOnTimeComparisonResponses(
      List<String> groupByDimensions, TimeOnTimeComparisonRequest comparisonRequest,
      final TimeOnTimeComparisonHandler handler) {

    ExecutorService service = Executors.newFixedThreadPool(10);

    List<Future<TimeOnTimeComparisonResponse>> timeOnTimeComparisonResponseFutures =
        new ArrayList<>();

    for (final String groupByDimension : groupByDimensions) {
      final TimeOnTimeComparisonRequest comparisonRequestByDimension =
          getComparisonRequestByDimension(comparisonRequest, groupByDimension);
      Callable<TimeOnTimeComparisonResponse> callable =
          new Callable<TimeOnTimeComparisonResponse>() {
            @Override
            public TimeOnTimeComparisonResponse call() throws Exception {
              return handler.handle(comparisonRequestByDimension);
            }
          };
      timeOnTimeComparisonResponseFutures.add(service.submit(callable));
    }
    service.shutdown();
    return timeOnTimeComparisonResponseFutures;
  }

  public static void main() {

  }

}
