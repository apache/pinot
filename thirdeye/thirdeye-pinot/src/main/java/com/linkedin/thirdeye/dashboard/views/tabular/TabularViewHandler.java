package com.linkedin.thirdeye.dashboard.views.tabular;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.joda.time.DateTime;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.comparison.Row;
import com.linkedin.thirdeye.client.comparison.Row.Metric;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonRequest;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.dashboard.resources.ViewRequestParams;
import com.linkedin.thirdeye.dashboard.views.GenericResponse;
import com.linkedin.thirdeye.dashboard.views.GenericResponse.ResponseSchema;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.dashboard.views.ViewHandler;
import com.linkedin.thirdeye.dashboard.views.ViewRequest;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapCell;

public class TabularViewHandler implements ViewHandler<TabularViewRequest, TabularViewResponse> {

  private final Comparator<Row> rowComparator = new Comparator<Row>() {
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

  private TimeOnTimeComparisonRequest generateTimeOnTimeComparisonRequest(
      TabularViewRequest request) throws Exception {

    TimeOnTimeComparisonRequest comparisonRequest = new TimeOnTimeComparisonRequest();
    String collection = request.getCollection();
    DateTime baselineStart = request.getBaselineStart();
    DateTime baselineEnd = request.getBaselineEnd();
    DateTime currentStart = request.getCurrentStart();
    DateTime currentEnd = request.getCurrentEnd();

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
    summary.put("baselineStart", comparisonRequest.getBaselineStart().toString());
    summary.put("baselineEnd", comparisonRequest.getBaselineEnd().toString());
    summary.put("currentStart", comparisonRequest.getCurrentStart().toString());
    summary.put("currentEnd", comparisonRequest.getCurrentEnd().toString());

    tabularViewResponse.setSummary(summary);
    List<String> expressionNames = new ArrayList<>();
    for (MetricExpression expression : request.getMetricExpressions()) {
      expressionNames.add(expression.getExpressionName());
    }
    tabularViewResponse.setMetrics(expressionNames);
    tabularViewResponse.setTimeBuckets(timeBuckets);

    String[] columns = new String[] {
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

        String[] columnData = {
            baselineValueStr, currentValueStr, ratioStr, cumulativeBaselineValueStr,
            cumulativeCurrentValueStr, cumulativeRatioStr
        };
        GenericResponse rowData = data.get(metric.getMetricName());
        rowData.getResponseData().add(columnData);

      }
    }

    return tabularViewResponse;
  }

  @Override
  public ViewRequest createRequest(ViewRequestParams ViewRequesParams) {
    // TODO Auto-generated method stub
    return null;
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
