package com.linkedin.thirdeye.dashboard.views.contributor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.GenericResponse;
import com.linkedin.thirdeye.dashboard.views.GenericResponse.Info;
import com.linkedin.thirdeye.dashboard.views.GenericResponse.ResponseSchema;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.dashboard.views.ViewHandler;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.datasource.comparison.Row;
import com.linkedin.thirdeye.datasource.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.datasource.comparison.TimeOnTimeComparisonRequest;
import com.linkedin.thirdeye.datasource.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.datasource.comparison.Row.Metric;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class ContributorViewHandler implements
    ViewHandler<ContributorViewRequest, ContributorViewResponse> {

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();

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

  public ContributorViewHandler(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  private TimeOnTimeComparisonRequest generateTimeOnTimeComparisonRequest(
      ContributorViewRequest request) throws Exception {

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
    List<String> dimensionsToGroupBy = request.getGroupByDimensions();
    if (dimensionsToGroupBy == null || dimensionsToGroupBy.isEmpty()) {
      List<String> allDimensions = Utils.getDimensionsToGroupBy(collection, filters);
      dimensionsToGroupBy = Lists.newArrayList(allDimensions.get(0));
    }
    List<MetricExpression> metricExpressions = request.getMetricExpressions();
    comparisonRequest.setCollectionName(collection);
    comparisonRequest.setBaselineStart(baselineStart);
    comparisonRequest.setBaselineEnd(baselineEnd);
    comparisonRequest.setCurrentStart(currentStart);
    comparisonRequest.setCurrentEnd(currentEnd);
    comparisonRequest.setFilterSet(filters);
    comparisonRequest.setMetricExpressions(metricExpressions);
    comparisonRequest.setAggregationTimeGranularity(request.getTimeGranularity());
    comparisonRequest.setGroupByDimensions(dimensionsToGroupBy);
    return comparisonRequest;
  }

  private List<TimeBucket> getTimeBuckets(TimeOnTimeComparisonResponse response) {
    Set<TimeBucket> timeBuckets = new TreeSet<>();

    int numRows = response.getNumRows();
    for (int i = 0; i < numRows; i++) {
      Row row = response.getRow(i);
      TimeBucket bucket = TimeBucket.fromRow(row);
      timeBuckets.add(bucket);
    }
    return new ArrayList<TimeBucket>(timeBuckets);
  }

  private Map<String, SortedSet<Row>> getRowsSortedByTime(TimeOnTimeComparisonResponse response) {
    Map<String, SortedSet<Row>> result = new HashMap<>();

    int numRows = response.getNumRows();
    for (int i = 0; i < numRows; i++) {
      Row row = response.getRow(i);
      String dimensionName = row.getDimensionName();
      String dimensionValue = row.getDimensionValue();
      String rowGroupKey = dimensionName + "." + dimensionValue;
      if (result.containsKey(rowGroupKey)) {
        result.get(rowGroupKey).add(row);
      } else {
        SortedSet<Row> rows = new TreeSet<>(rowComparator);
        rows.add(row);
        result.put(rowGroupKey, rows);
      }
    }

    return result;
  }

  @Override
  public ContributorViewResponse process(ContributorViewRequest request) throws Exception {

    TimeOnTimeComparisonRequest comparisonRequest = generateTimeOnTimeComparisonRequest(request);
    TimeOnTimeComparisonHandler handler = new TimeOnTimeComparisonHandler(queryCache);

    TimeOnTimeComparisonResponse response = handler.handle(comparisonRequest);
    List<String> metricNames = new ArrayList<>(response.getMetrics());
    List<String> expressionNames = new ArrayList<>();
    for (MetricExpression expression : request.getMetricExpressions()) {
      expressionNames.add(expression.getExpressionName());
    }
    List<String> dimensions = new ArrayList<>(response.getDimensions());
    List<TimeBucket> timeBuckets = getTimeBuckets(response);
    Map<String, SortedSet<Row>> rows = getRowsSortedByTime(response);

    ContributorViewResponse contributorViewResponse = new ContributorViewResponse();
    contributorViewResponse.setMetrics(expressionNames);
    contributorViewResponse.setDimensions(dimensions);
    contributorViewResponse.setTimeBuckets(timeBuckets);
    GenericResponse genericResponse = new GenericResponse();

    Map<String, Double[]> runningTotalMap = new HashMap<>();
    // one view per <metric,dimensionName> combination
    Map<String, ContributionViewTableBuilder> contributionViewTableMap = new LinkedHashMap<>();
    Map<String, List<String>> dimensionValuesMap = new HashMap<>();
    for (Map.Entry<String, SortedSet<Row>> entry : rows.entrySet()) {
      for (Row row : entry.getValue()) {
        String dimensionName = row.getDimensionName();
        String dimensionValue = row.getDimensionValue();
        for (Metric metric : row.getMetrics()) {
          String metricName = metric.getMetricName();
          if (!expressionNames.contains(metricName)) {
            continue;
          }
          Double baselineValue = metric.getBaselineValue();
          Double currentValue = metric.getCurrentValue();

          Double cumulativeBaselineValue;
          Double cumulativeCurrentValue;

          String metricDimensionNameString = metricName + "." + dimensionName;
          ContributionViewTableBuilder contributionViewTable =
              contributionViewTableMap.get(metricDimensionNameString);
          if (contributionViewTable == null) {
            contributionViewTable = new ContributionViewTableBuilder(metricName, dimensionName);
            contributionViewTableMap.put(metricDimensionNameString, contributionViewTable);
          }

          String rowKey = metricName + "." + dimensionName + "." + dimensionValue;
          if (runningTotalMap.containsKey(rowKey)) {
            Double[] totalValues = runningTotalMap.get(rowKey);
            cumulativeBaselineValue = totalValues[0] + baselineValue;
            cumulativeCurrentValue = totalValues[1] + currentValue;
          } else {
            cumulativeBaselineValue = baselineValue;
            cumulativeCurrentValue = currentValue;
          }
          TimeBucket timeBucket = TimeBucket.fromRow(row);
          contributionViewTable.addEntry(dimensionValue, timeBucket, baselineValue, currentValue,
              cumulativeBaselineValue, cumulativeCurrentValue);
          List<String> dimensionValues = dimensionValuesMap.get(dimensionName);
          if (dimensionValues == null) {
            dimensionValues = new ArrayList<>();
            dimensionValuesMap.put(dimensionName, dimensionValues);
          }
          if (!dimensionValues.contains(dimensionValue)) {
            dimensionValues.add(dimensionValue);
          }
          Double[] runningTotalPerMetric = new Double[] {
              cumulativeBaselineValue, cumulativeCurrentValue
          };
          runningTotalMap.put(rowKey, runningTotalPerMetric);
        }
      }
    }
    Map<String, List<Integer>> keyToRowIdListMapping = new TreeMap<>();
    List<String[]> rowData = new ArrayList<>();
    // for each metric, dimension pair compute the total value for each dimension. This will be used
    // to sort the dimension values
    Map<String, Map<String, Map<String, Double>>> baselineTotalMapPerDimensionValue =
        new HashMap<>();
    Map<String, Map<String, Map<String, Double>>> currentTotalMapPerDimensionValue =
        new HashMap<>();

    for (String metricDimensionNameString : contributionViewTableMap.keySet()) {
      ContributionViewTableBuilder contributionViewTable =
          contributionViewTableMap.get(metricDimensionNameString);
      ContributionViewTable table = contributionViewTable.build();
      List<ContributionCell> cells = table.getCells();
      for (ContributionCell cell : cells) {
        String metricName = table.getMetricName();
        String dimName = table.getDimensionName();
        String dimValue = cell.getDimensionValue();
        String key = metricName + "|" + dimName + "|" + dimValue;
        List<Integer> rowIdList = keyToRowIdListMapping.get(key);
        if (rowIdList == null) {
          rowIdList = new ArrayList<>();
          keyToRowIdListMapping.put(key, rowIdList);
        }
        rowIdList.add(rowData.size());
        rowData.add(cell.toArray());
        // update baseline
        updateTotalForDimensionValue(baselineTotalMapPerDimensionValue, metricName, dimName,
            dimValue, cell.getBaselineValue());
        // update current
        updateTotalForDimensionValue(currentTotalMapPerDimensionValue, metricName, dimName,
            dimValue, cell.getCurrentValue());
      }
    }
    genericResponse.setResponseData(rowData);
    genericResponse.setSchema(new ResponseSchema(ContributionCell.columns()));
    genericResponse.setKeyToRowIdMapping(keyToRowIdListMapping);
    Info summary = new Info();
    genericResponse.setSummary(summary);
    for (String dimensionName : dimensionValuesMap.keySet()) {
      List<String> dimensionValues = dimensionValuesMap.get(dimensionName);
      sort(expressionNames, dimensionName, dimensionValues, baselineTotalMapPerDimensionValue,
          currentTotalMapPerDimensionValue);
    }
    contributorViewResponse.setDimensionValuesMap(dimensionValuesMap);
    contributorViewResponse.setResponseData(genericResponse);
    contributorViewResponse.setCurrentTotalMapPerDimensionValue(currentTotalMapPerDimensionValue);
    contributorViewResponse.setBaselineTotalMapPerDimensionValue(baselineTotalMapPerDimensionValue);
    return contributorViewResponse;
  }

  /**
   * sort the values based on their values
   * @param dimensionValues
   * @param baselineTotalMapPerDimensionValue
   * @param currentTotalMapPerDimensionValue
   */
  private void sort(List<String> metricNames, String dimensionName, List<String> dimensionValues,
      final Map<String, Map<String, Map<String, Double>>> baselineTotalMapPerDimensionValue,
      final Map<String, Map<String, Map<String, Double>>> currentTotalMapPerDimensionValue) {
    final String metricName = metricNames.get(0);
    // sort using current values for now
    final Map<String, Double> baselineValuesMap =
        baselineTotalMapPerDimensionValue.get(metricName).get(dimensionName);
    final Map<String, Double> currentValuesMap =
        currentTotalMapPerDimensionValue.get(metricName).get(dimensionName);
    Comparator<? super String> comparator = new Comparator<String>() {

      @Override
      public int compare(String o1, String o2) {
        double d1 = 0;
        if (currentValuesMap.get(o1) != null) {
          d1 += currentValuesMap.get(o1);
        }
        if (baselineValuesMap.get(o1) != null) {
          d1 += baselineValuesMap.get(o1);
        }
        double d2 = 0;
        if (currentValuesMap.get(o2) != null) {
          d2 += currentValuesMap.get(o2);
        }
        if (baselineValuesMap.get(o2) != null) {
          d2 += baselineValuesMap.get(o2);
        }

        return Double.compare(d1, d2) * -1;
      }

    };
    Collections.sort(dimensionValues, comparator);
  }

  private void updateTotalForDimensionValue(Map<String, Map<String, Map<String, Double>>> map,
      String metricName, String dimName, String dimValue, double value) {
    if (!map.containsKey(metricName)) {
      map.put(metricName, new HashMap<String, Map<String, Double>>());
    }
    if (!map.get(metricName).containsKey(dimName)) {
      map.get(metricName).put(dimName, new HashMap<String, Double>());
    }
    if (!map.get(metricName).get(dimName).containsKey(dimValue)) {
      map.get(metricName).get(dimName).put(dimValue, 0d);
      ;
    }
    double currentSum = map.get(metricName).get(dimName).get(dimValue);
    map.get(metricName).get(dimName).put(dimValue, currentSum + value);
  }

}
