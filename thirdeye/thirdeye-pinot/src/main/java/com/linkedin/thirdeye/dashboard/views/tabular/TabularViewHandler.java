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
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.QueryCache;
import com.linkedin.thirdeye.client.comparison.Row;
import com.linkedin.thirdeye.client.comparison.Row.Metric;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonRequest;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.resources.ViewRequestParams;
import com.linkedin.thirdeye.dashboard.views.GenericResponse;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.dashboard.views.ViewHandler;
import com.linkedin.thirdeye.dashboard.views.ViewRequest;
import com.linkedin.thirdeye.dashboard.views.GenericResponse.ResponseSchema;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapCell;

public class TabularViewHandler implements ViewHandler<TabularViewRequest, TabularViewResponse> {

  private final Comparator<DateTime> dateTimeComparator = new Comparator<DateTime>() {
    public int compare(DateTime dateTime1, DateTime dateTime2) {
      long millisSinceEpoch1 = dateTime1.getMillis();
      long millisSinceEpoch2 = dateTime2.getMillis();
      return (millisSinceEpoch1 < millisSinceEpoch2) ? -1
          : (millisSinceEpoch1 == millisSinceEpoch2) ? 0 : 1;
    }
  };

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
    List<MetricFunction> metricFunctions =
        Utils.computeMetricFunctionsFromExpressions(metricExpressions);
    comparisonRequest.setCollectionName(collection);
    comparisonRequest.setBaselineStart(baselineStart);
    comparisonRequest.setBaselineEnd(baselineEnd);
    comparisonRequest.setCurrentStart(currentStart);
    comparisonRequest.setCurrentEnd(currentEnd);
    comparisonRequest.setFilterSet(filters);
    comparisonRequest.setMetricFunctions(metricFunctions);
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
    List<String> metricNames = new ArrayList<>(response.getMetrics());
    List<TimeBucket> timeBuckets = getTimeBuckets(response);
    List<Row> rows = getRowsSortedByTime(response);

    TabularViewResponse tabularViewResponse = new TabularViewResponse();

    Map<String, String> summary = new HashMap<>();
    summary.put("baselineStart", comparisonRequest.getBaselineStart().toString());
    summary.put("baselineEnd", comparisonRequest.getBaselineEnd().toString());
    summary.put("currentStart", comparisonRequest.getCurrentStart().toString());
    summary.put("currentEnd", comparisonRequest.getCurrentEnd().toString());

    tabularViewResponse.setSummary(summary);
    tabularViewResponse.setMetrics(metricNames);
    tabularViewResponse.setTimeBuckets(timeBuckets);

    String[] columns = new String[] {
        "baselineValue", "currentValue", "ratio", "cumulativeBaselineValue",
        "cumulativeCurrentValue", "cumulativeRatio"
    };
    //maintain same order in response
    Map<String, GenericResponse> data = new LinkedHashMap<>();
    for(String metric:metricNames){
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

        Double baselineValue = metric.getBaselineValue();
        Double currentValue = metric.getCurrentValue();
        String baselineValueStr = HeatMapCell.format(baselineValue);
        String currentValueStr = HeatMapCell.format(currentValue);
        String ratioStr = HeatMapCell.format((currentValue - baselineValue) / baselineValue);

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
        String cumulativeRatioStr = HeatMapCell
            .format((cumulativeCurrentValue - cumulativeBaselineValue) / cumulativeBaselineValue);

        String[] columnData = {
            baselineValueStr, currentValueStr, ratioStr, cumulativeBaselineValueStr,
            cumulativeCurrentValueStr, cumulativeRatioStr
        };
        GenericResponse rowData = data.get(metric.getMetricName());
        if(rowData == null){
          System.out.println("NULL for metric:"+ metric + " metricNames:"+ metricNames);
        }
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
  public void addColumnData(Map<String, GenericResponse> data, String metric, String[] columnData, String[] columns) {
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
