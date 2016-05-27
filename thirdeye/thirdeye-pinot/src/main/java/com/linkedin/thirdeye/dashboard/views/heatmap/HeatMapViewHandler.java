package com.linkedin.thirdeye.dashboard.views.heatmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.joda.time.DateTime;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.comparison.Row;
import com.linkedin.thirdeye.client.comparison.Row.Metric;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonRequest;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.resources.ViewRequestParams;
import com.linkedin.thirdeye.dashboard.views.GenericResponse;
import com.linkedin.thirdeye.dashboard.views.GenericResponse.Info;
import com.linkedin.thirdeye.dashboard.views.GenericResponse.ResponseSchema;
import com.linkedin.thirdeye.dashboard.views.ViewHandler;
import com.linkedin.thirdeye.dashboard.views.ViewRequest;

public class HeatMapViewHandler implements ViewHandler<HeatMapViewRequest, HeatMapViewResponse> {

  private final QueryCache queryCache;

  public HeatMapViewHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  private TimeOnTimeComparisonRequest generateTimeOnTimeComparisonRequest(
      HeatMapViewRequest request) throws Exception {

    TimeOnTimeComparisonRequest comparisonRequest = new TimeOnTimeComparisonRequest();
    String collection = request.getCollection();
    DateTime baselineStart = request.getBaselineStart();
    DateTime baselineEnd = request.getBaselineEnd();
    DateTime currentStart = request.getCurrentStart();
    DateTime currentEnd = request.getCurrentEnd();

    Multimap<String, String> filters = request.getFilters();
    List<String> dimensionsToGroupBy =
        Utils.getDimensionsToGroupBy(queryCache, collection, filters);

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

    TimeOnTimeComparisonRequest comparisonRequest = generateTimeOnTimeComparisonRequest(request);
    TimeOnTimeComparisonHandler handler = new TimeOnTimeComparisonHandler(queryCache);

    TimeOnTimeComparisonResponse response = handler.handle(comparisonRequest);

    Set<String> dimensions = response.getDimensions();
    List<String> expressionNames = new ArrayList<>();
    for (MetricExpression expression : request.getMetricExpressions()) {
      expressionNames.add(expression.getExpressionName());
    }

    int numRows = response.getNumRows();

    Map<String, HeatMap.Builder> data = new HashMap<>();
    // we are tracking per dimension, to validate that its the same for each dimension
    Map<String, Double> baselineTotalPerDimension = new HashMap<>();
    Map<String, Double> currentTotalPerDimension = new HashMap<>();
    for (String dimension : dimensions) {
      baselineTotalPerDimension.put(dimension, 0d);
      currentTotalPerDimension.put(dimension, 0d);
    }

    for (int i = 0; i < numRows; i++) {
      Row row = response.getRow(i);
      String dimension = row.getDimensionName();
      String dimensionValue = row.getDimensionValue();
      for (Metric metric : row.getMetrics()) {
        String metricName = metric.getMetricName();
        if(!expressionNames.contains(metricName)){
          continue;
        }
        String dataKey = metricName + "." + dimension;
        HeatMap.Builder heatMapBuilder = data.get(dataKey);
        if (heatMapBuilder == null) {
          heatMapBuilder = new HeatMap.Builder(dimension);
          data.put(dataKey, heatMapBuilder);
        }
        heatMapBuilder.addCell(dimensionValue, metric.getBaselineValue(), metric.getCurrentValue());
        baselineTotalPerDimension.put(dimension,
            baselineTotalPerDimension.get(dimension) + metric.getBaselineValue());
        currentTotalPerDimension.put(dimension,
            currentTotalPerDimension.get(dimension) + metric.getCurrentValue());
      }
    }

    ResponseSchema schema = new ResponseSchema();

    String[] columns = HeatMapCell.columns();
    for (int i = 0; i < columns.length; i++) {
      String column = columns[i];
      schema.add(column, i);
    }

    Map<String, GenericResponse> heatMapViewResponseData = new HashMap<>();
    Info summary = new Info();
    summary.addSimpleField("baselineStart", comparisonRequest.getBaselineStart().toString());
    summary.addSimpleField("baselineEnd", comparisonRequest.getBaselineEnd().toString());
    summary.addSimpleField("currentStart", comparisonRequest.getCurrentStart().toString());
    summary.addSimpleField("currentEnd", comparisonRequest.getCurrentEnd().toString());
    Double baselineTotal = baselineTotalPerDimension.values().iterator().next();
    Double currentTotal = currentTotalPerDimension.values().iterator().next();
    summary.addSimpleField("baselineTotal", HeatMapCell.format(baselineTotal));
    summary.addSimpleField("currentTotal", HeatMapCell.format(currentTotal));
    summary.addSimpleField("deltaChange", HeatMapCell.format(currentTotal-baselineTotal));
    summary.addSimpleField("deltaPercentage", HeatMapCell.format((currentTotal-baselineTotal)*100.0/baselineTotal));
    
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
    heatMapViewResponse.setDimensions(new ArrayList<String>(dimensions));

    heatMapViewResponse.setSummary(summary);
    heatMapViewResponse.setData(heatMapViewResponseData);
    return heatMapViewResponse;
  }

  public static void main() {

  }

  @Override
  public ViewRequest createRequest(ViewRequestParams ViewRequesParams) {
    // TODO Auto-generated method stub
    return null;
  }

}
