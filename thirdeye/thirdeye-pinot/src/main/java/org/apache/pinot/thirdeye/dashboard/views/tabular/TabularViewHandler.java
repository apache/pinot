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

package org.apache.pinot.thirdeye.dashboard.views.tabular;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dashboard.views.GenericResponse;
import org.apache.pinot.thirdeye.dashboard.views.GenericResponse.ResponseSchema;
import org.apache.pinot.thirdeye.dashboard.views.TimeBucket;
import org.apache.pinot.thirdeye.dashboard.views.ViewHandler;
import org.apache.pinot.thirdeye.dashboard.views.heatmap.HeatMapCell;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.comparison.Row;
import org.apache.pinot.thirdeye.datasource.comparison.TimeOnTimeComparisonHandler;
import org.apache.pinot.thirdeye.datasource.comparison.TimeOnTimeComparisonRequest;
import org.apache.pinot.thirdeye.datasource.comparison.TimeOnTimeComparisonResponse;
import org.apache.pinot.thirdeye.datasource.comparison.Row.Metric;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;

public class TabularViewHandler implements ViewHandler<TabularViewRequest, TabularViewResponse> {
  private static ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

  private final Comparator<Row> rowComparator = new Comparator<Row>() {
    @Override
    public int compare(Row row1, Row row2) {
      long millisSinceEpoch1 = row1.getCurrentStart().getMillis();
      long millisSinceEpoch2 = row2.getCurrentStart().getMillis();
      return (millisSinceEpoch1 < millisSinceEpoch2) ? -1
          : (millisSinceEpoch1 == millisSinceEpoch2) ? 0 : 1;
    }
  };

  private final QueryCache queryCache;

  public TabularViewHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  private TimeOnTimeComparisonRequest generateTimeOnTimeComparisonRequest(TabularViewRequest request)
      throws Exception {

    TimeOnTimeComparisonRequest comparisonRequest = new TimeOnTimeComparisonRequest();
    String collection = request.getCollection();
    DateTime baselineStart = request.getBaselineStart();
    DateTime baselineEnd = request.getBaselineEnd();
    DateTime currentStart = request.getCurrentStart();
    DateTime currentEnd = request.getCurrentEnd();

    DatasetConfigDTO datasetConfig = CACHE_REGISTRY.getDatasetConfigCache().get(collection);
    TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    if (!request.getTimeGranularity().getUnit().equals(TimeUnit.DAYS) ||
        !StringUtils.isBlank(timespec.getFormat())) {
      comparisonRequest.setEndDateInclusive(true);
    }

    Multimap<String, String> filters = request.getFilters();

    List<MetricExpression> metricExpressions = request.getMetricExpressions();
    comparisonRequest.setCollectionName(collection);
    comparisonRequest.setBaselineStart(baselineStart);
    comparisonRequest.setBaselineEnd(baselineEnd);
    comparisonRequest.setCurrentStart(currentStart);
    comparisonRequest.setCurrentEnd(currentEnd);
    comparisonRequest.setFilterSet(filters);
    comparisonRequest.setMetricExpressions(metricExpressions);
    comparisonRequest.setAggregationTimeGranularity(request.getTimeGranularity());
    return comparisonRequest;
  }

  private List<TimeBucket> getTimeBuckets(TimeOnTimeComparisonResponse response) {
    List<TimeBucket> timeBuckets = new ArrayList<>();

    int numRows = response.getNumRows();
    for (int i = 0; i < numRows; i++) {
      Row row = response.getRow(i);
      TimeBucket bucket = TimeBucket.fromRow(row);
      timeBuckets.add(bucket);
    }

    Collections.sort(timeBuckets);
    return timeBuckets;
  }

  private List<Row> getRowsSortedByTime(TimeOnTimeComparisonResponse response) {
    SortedSet<Row> rows = new TreeSet<>(rowComparator);

    int numRows = response.getNumRows();
    for (int i = 0; i < numRows; i++) {
      Row row = response.getRow(i);
      rows.add(row);
    }

    return new ArrayList<Row>(rows);
  }

  @Override
  public TabularViewResponse process(TabularViewRequest request) throws Exception {

    // query 1 for everything from baseline start to baseline end
    // query 2 for everything from current start to current end
    // for each dimension group by top 100
    // query 1 for everything from baseline start to baseline end
    // query for everything from current start to current end

    TimeOnTimeComparisonRequest comparisonRequest = generateTimeOnTimeComparisonRequest(request);
    TimeOnTimeComparisonHandler handler = new TimeOnTimeComparisonHandler(queryCache);

    TimeOnTimeComparisonResponse response = handler.handle(comparisonRequest);
    List<TimeBucket> timeBuckets = getTimeBuckets(response);
    List<Row> rows = getRowsSortedByTime(response);

    TabularViewResponse tabularViewResponse = new TabularViewResponse();

    Map<String, String> summary = new HashMap<>();
    summary.put("baselineStart", Long.toString(comparisonRequest.getBaselineStart().getMillis()));
    summary.put("baselineEnd", Long.toString(comparisonRequest.getBaselineEnd().getMillis()));
    summary.put("currentStart", Long.toString(comparisonRequest.getCurrentStart().getMillis()));
    summary.put("currentEnd", Long.toString(comparisonRequest.getCurrentEnd().getMillis()));

    tabularViewResponse.setSummary(summary);
    List<String> expressionNames = new ArrayList<>();
    for (MetricExpression expression : request.getMetricExpressions()) {
      expressionNames.add(expression.getExpressionName());
    }
    tabularViewResponse.setMetrics(expressionNames);
    tabularViewResponse.setTimeBuckets(timeBuckets);

    String[] columns =
        new String[] {
            "baselineValue", "currentValue", "ratio", "cumulativeBaselineValue",
            "cumulativeCurrentValue", "cumulativeRatio"
        };
    // maintain same order in response
    Map<String, GenericResponse> data = new LinkedHashMap<>();
    for (String metric : expressionNames) {
      ResponseSchema schema = new ResponseSchema();
      for (int i = 0; i < columns.length; i++) {
        String column = columns[i];
        schema.add(column, i);
      }
      GenericResponse rowData = new GenericResponse();
      rowData.setSchema(schema);
      List<String[]> genericResponseData = new ArrayList<>();
      rowData.setResponseData(genericResponseData);
      data.put(metric, rowData);
    }
    tabularViewResponse.setData(data);

    Map<String, Double[]> runningTotalMap = new HashMap<>();

    for (Row row : rows) {
      for (Metric metric : row.getMetrics()) {
        String metricName = metric.getMetricName();
        if (!expressionNames.contains(metricName)) {
          continue;
        }
        Double baselineValue = metric.getBaselineValue();
        Double currentValue = metric.getCurrentValue();
        String baselineValueStr = HeatMapCell.format(baselineValue);
        String currentValueStr = HeatMapCell.format(currentValue);
        double ratio = ((currentValue - baselineValue) * 100) / baselineValue;
        if (Double.isNaN(ratio)) {
          ratio = 0;
        } else if (Double.isInfinite(ratio)) {
          ratio = 100;
        }
        String ratioStr = HeatMapCell.format(ratio);
        Double cumulativeBaselineValue;
        Double cumulativeCurrentValue;

        if (runningTotalMap.containsKey(metricName)) {
          Double[] totalValues = runningTotalMap.get(metricName);
          cumulativeBaselineValue = totalValues[0] + baselineValue;
          cumulativeCurrentValue = totalValues[1] + currentValue;
        } else {
          cumulativeBaselineValue = baselineValue;
          cumulativeCurrentValue = currentValue;
        }

        Double[] runningTotalPerMetric = new Double[] {
            cumulativeBaselineValue, cumulativeCurrentValue
        };

        runningTotalMap.put(metricName, runningTotalPerMetric);

        String cumulativeBaselineValueStr = HeatMapCell.format(cumulativeBaselineValue);
        String cumulativeCurrentValueStr = HeatMapCell.format(cumulativeCurrentValue);
        double cumulativeRatio =
            ((cumulativeCurrentValue - cumulativeBaselineValue) * 100) / cumulativeBaselineValue;
        if (Double.isNaN(ratio)) {
          cumulativeRatio = 0;
        } else if (Double.isInfinite(cumulativeRatio)) {
          cumulativeRatio = 100;
        }
        String cumulativeRatioStr = HeatMapCell.format(cumulativeRatio);

        String[] columnData =
            {
                baselineValueStr, currentValueStr, ratioStr, cumulativeBaselineValueStr,
                cumulativeCurrentValueStr, cumulativeRatioStr
            };
        GenericResponse rowData = data.get(metric.getMetricName());
        rowData.getResponseData().add(columnData);

      }
    }

    return tabularViewResponse;
  }

  public void addColumnData(Map<String, GenericResponse> data, String metric, String[] columnData,
      String[] columns) {
    GenericResponse rowData;
    if (data.containsKey(metric)) {
      rowData = data.get(metric);
      rowData.getResponseData().add(columnData);
    } else {

      ResponseSchema schema = new ResponseSchema();
      for (int i = 0; i < columns.length; i++) {
        String column = columns[i];
        schema.add(column, i);
      }

      rowData = new GenericResponse();
      List<String[]> genericResponseData = new ArrayList<>();
      genericResponseData.add(columnData);
      rowData.setResponseData(genericResponseData);
      rowData.setSchema(schema);
      data.put(metric, rowData);
    }

  }
}
