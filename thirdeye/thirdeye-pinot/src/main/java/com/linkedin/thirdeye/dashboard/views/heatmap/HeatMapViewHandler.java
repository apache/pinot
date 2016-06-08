package com.linkedin.thirdeye.dashboard.views.heatmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.comparison.Row;
import com.linkedin.thirdeye.client.comparison.Row.Metric;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonRequest;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.resources.ViewRequestParams;
import com.linkedin.thirdeye.dashboard.views.GenericResponse;
import com.linkedin.thirdeye.dashboard.views.GenericResponse.Info;
import com.linkedin.thirdeye.dashboard.views.GenericResponse.ResponseSchema;
import com.linkedin.thirdeye.dashboard.views.ViewHandler;
import com.linkedin.thirdeye.dashboard.views.ViewRequest;

import jersey.repackaged.com.google.common.collect.Lists;

public class HeatMapViewHandler implements ViewHandler<HeatMapViewRequest, HeatMapViewResponse> {

  private final QueryCache queryCache;
  private CollectionConfig collectionConfig = null;
  private static final Logger LOGGER = LoggerFactory.getLogger(HeatMapViewHandler.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE =
      ThirdEyeCacheRegistry.getInstance();

  private static final String RATIO_SEPARATOR = "/";

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

    try {
      collectionConfig =
          CACHE_REGISTRY_INSTANCE.getCollectionConfigCache().get(request.getCollection());
    } catch (InvalidCacheLoadException e) {
      LOGGER.debug("No collection configs for collection {}", request.getCollection());
    }

    List<String> expressionNames = new ArrayList<>();
    Map<String, String> cellSizeExpressions = new HashMap<>();
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
    TimeOnTimeComparisonHandler handler = new TimeOnTimeComparisonHandler(queryCache);

    // we are tracking per dimension, to validate that its the same for each dimension
    Map<String, Map<String, Double>> baselineTotalPerMetricAndDimension = new HashMap<>();
    Map<String, Map<String, Double>> currentTotalPerMetricAndDimension = new HashMap<>();

    for (String metricOrExpressionName : metricOrExpressionNames) {
      Map<String, Double> baselineTotalMap = new HashMap<>();
      Map<String, Double> currentTotalMap = new HashMap<>();
      baselineTotalPerMetricAndDimension.put(metricOrExpressionName, baselineTotalMap);
      currentTotalPerMetricAndDimension.put(metricOrExpressionName, currentTotalMap);
      for (String dimension : groupByDimensions) {
        baselineTotalMap.put(dimension, 0d);
        currentTotalMap.put(dimension, 0d);
      }
    }

    for (String groupByDimension : groupByDimensions) {

      comparisonRequest.setGroupByDimensions(Lists.newArrayList(groupByDimension));
      TimeOnTimeComparisonResponse response = handler.handle(comparisonRequest);

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
          if (collectionConfig != null && collectionConfig.getCellSizeExpression() != null
              && collectionConfig.getCellSizeExpression().get(metricName) != null) {
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
            String cellSizeExpression =
                collectionConfig.getCellSizeExpression().get(metricName).getExpression();
            cellSizeExpressions.put(metricName, cellSizeExpression);
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
      Double baselineTotal = baselineTotalPerMetricAndDimension.get(expression.getExpressionName())
          .values().iterator().next();
      Double currentTotal = currentTotalPerMetricAndDimension.get(expression.getExpressionName())
          .values().iterator().next();

      // check if its derived
      if (metricFunctions.size() > 1) {
        Map<String, Double> baselineContext = new HashMap<>();
        Map<String, Double> currentContext = new HashMap<>();
        for (String metricOrExpression : metricOrExpressionNames) {
          baselineContext.put(metricOrExpression, baselineTotalPerMetricAndDimension
              .get(metricOrExpression).values().iterator().next());
          currentContext.put(metricOrExpression,
              currentTotalPerMetricAndDimension.get(metricOrExpression).values().iterator().next());
        }
        baselineTotal = MetricExpression.evaluateExpression(expression, baselineContext);
        currentTotal = MetricExpression.evaluateExpression(expression, currentContext);
      } else {
        baselineTotal = baselineTotalPerMetricAndDimension.get(expression.getExpressionName())
            .values().iterator().next();
        currentTotal = currentTotalPerMetricAndDimension.get(expression.getExpressionName())
            .values().iterator().next();
      }
      summary.addSimpleField("baselineStart", comparisonRequest.getBaselineStart().toString());
      summary.addSimpleField("baselineEnd", comparisonRequest.getBaselineEnd().toString());
      summary.addSimpleField("currentStart", comparisonRequest.getCurrentStart().toString());
      summary.addSimpleField("currentEnd", comparisonRequest.getCurrentEnd().toString());

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
    heatMapViewResponse.setDimensions(groupByDimensions);
    heatMapViewResponse.setData(heatMapViewResponseData);
    heatMapViewResponse.setCellSizeExpression(cellSizeExpressions);
    heatMapViewResponse.setSummary(summary);

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
