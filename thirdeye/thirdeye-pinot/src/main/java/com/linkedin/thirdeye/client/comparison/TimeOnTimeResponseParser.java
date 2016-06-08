package com.linkedin.thirdeye.client.comparison;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.ThirdEyeResponseRow;
import com.linkedin.thirdeye.client.comparison.Row.Builder;
import com.linkedin.thirdeye.client.comparison.Row.Metric;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;

public class TimeOnTimeResponseParser {

  private ThirdEyeResponse baselineResponse;
  private ThirdEyeResponse currentResponse;
  private List<Range<DateTime>> baselineRanges;
  private List<Range<DateTime>> currentRanges;
  private TimeGranularity aggTimeGranularity;
  private List<String> groupByDimensions;

  private CollectionConfig collectionConfig = null;
  private static ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeOnTimeResponseParser.class);


  private static String TIME_DIMENSION_JOINER_ESCAPED = "\\|";
  private static String TIME_DIMENSION_JOINER = "|";
  private double metricThreshold = CollectionConfig.DEFAULT_THRESHOLD;
  private static String OTHER = "OTHER";

  public TimeOnTimeResponseParser(ThirdEyeResponse baselineResponse,
      ThirdEyeResponse currentResponse, List<Range<DateTime>> baselineRanges,
      List<Range<DateTime>> currentRanges, TimeGranularity timeGranularity,
      List<String> groupByDimensions) {
    this.baselineResponse = baselineResponse;
    this.currentResponse = currentResponse;
    this.baselineRanges = baselineRanges;
    this.currentRanges = currentRanges;
    this.aggTimeGranularity = timeGranularity;
    this.groupByDimensions = groupByDimensions;

    String collection = baselineResponse.getRequest().getCollection();
    try {
      collectionConfig = CACHE_REGISTRY_INSTANCE.getCollectionConfigCache().get(collection);
    } catch (Exception e) {
      LOGGER.debug("No collection configs for collection {}", collection);
    }

    if (collectionConfig != null) {
      metricThreshold = collectionConfig.getMetricThreshold();
    }
  }

  List<Row> parseResponse() {
    if (baselineResponse == null || currentResponse == null) {
      return Collections.emptyList();
    }
    boolean hasGroupByDimensions = false;
    if (groupByDimensions != null && groupByDimensions.size() > 0) {
      hasGroupByDimensions = true;
    }
    boolean hasGroupByTime = false;
    if (aggTimeGranularity != null) {
      hasGroupByTime = true;
    }

    Map<String, ThirdEyeResponseRow> baselineResponseMap;
    Map<String, ThirdEyeResponseRow> currentResponseMap;
    List<MetricFunction> metricFunctions = baselineResponse.getMetricFunctions();
    int numMetrics = metricFunctions.size();

    List<Row> rows = new ArrayList<>();
    if (hasGroupByTime) {

      int numTimeBuckets = baselineRanges.size();

      if (hasGroupByDimensions) {
        // group by time and dimension  //contributor
        baselineResponseMap = createResponseMapByTimeAndDimension(baselineResponse);
        currentResponseMap = createResponseMapByTimeAndDimension(currentResponse);

        Map<Integer, List<Double>> baselineMetricSums = getMetricSumsByTime(baselineResponse);
        Map<Integer, List<Double>> currentMetricSums = getMetricSumsByTime(currentResponse);

        Set<String> timeDimensionValues = new HashSet<>();
        timeDimensionValues.addAll(baselineResponseMap.keySet());
        timeDimensionValues.addAll(currentResponseMap.keySet());
        Set<String> dimensionValues = new HashSet<>();
        for (String timeDimensionValue : timeDimensionValues) {
          String[] tokens = timeDimensionValue.split(TIME_DIMENSION_JOINER_ESCAPED);
          String dimensionValue = tokens.length < 2 ? "" : tokens[1];
          dimensionValues.add(dimensionValue);
        }

        String dimensionName = baselineResponse.getGroupKeyColumns().get(1);

        // other row
        List<Row.Builder> otherBuilders = new ArrayList<>();
        List<double[]> otherBaselineMetrics = new ArrayList<>();
        List<double[]> otherCurrentMetrics = new ArrayList<>();
        List<Row> otherRows = new ArrayList<>();
        boolean includeOther = false;
        for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
          Range<DateTime> baselineTimeRange = baselineRanges.get(timeBucketId);
          Range<DateTime> currentTimeRange = currentRanges.get(timeBucketId);

          Row.Builder builder = new Row.Builder();
          builder.setBaselineStart(baselineTimeRange.lowerEndpoint());
          builder.setBaselineEnd(baselineTimeRange.upperEndpoint());
          builder.setCurrentStart(currentTimeRange.lowerEndpoint());
          builder.setCurrentEnd(currentTimeRange.upperEndpoint());
          builder.setDimensionName(dimensionName);
          builder.setDimensionValue(OTHER);
          otherBuilders.add(builder);
          double[] otherBaseline = new double[numMetrics];
          Arrays.fill(otherBaseline, 0);
          double[] otherCurrent = new double[numMetrics];
          Arrays.fill(otherCurrent, 0);
          otherBaselineMetrics.add(otherBaseline);
          otherCurrentMetrics.add(otherCurrent);
        }

        for (String dimensionValue : dimensionValues) {
          List<Row> thresholdRows = new ArrayList<>();
          for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
            Range<DateTime> baselineTimeRange = baselineRanges.get(timeBucketId);
            Range<DateTime> currentTimeRange = currentRanges.get(timeBucketId);

            //compute the time|dimension key
            String baselineTimeDimensionValue = timeBucketId + TIME_DIMENSION_JOINER + dimensionValue;
            String currentTimeDimensionValue = timeBucketId + TIME_DIMENSION_JOINER + dimensionValue;

            ThirdEyeResponseRow baselineRow = baselineResponseMap.get(baselineTimeDimensionValue);
            ThirdEyeResponseRow currentRow = currentResponseMap.get(currentTimeDimensionValue);

            Row.Builder builder = new Row.Builder();
            builder.setBaselineStart(baselineTimeRange.lowerEndpoint());
            builder.setBaselineEnd(baselineTimeRange.upperEndpoint());
            builder.setCurrentStart(currentTimeRange.lowerEndpoint());
            builder.setCurrentEnd(currentTimeRange.upperEndpoint());
            builder.setDimensionName(dimensionName);
            builder.setDimensionValue(dimensionValue);
            addMetric(baselineRow, currentRow, builder);

            Row row = builder.build();
            thresholdRows.add(row);
          }
          boolean passedThreshold = false;

          for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
            if (checkMetricSums(thresholdRows.get(timeBucketId), baselineMetricSums.get(timeBucketId), currentMetricSums.get(timeBucketId))) {
              passedThreshold = true;
              break;
            }
          }

          if (passedThreshold) { // if any of the cells of a contributor row passes threshold, add all those cells
            rows.addAll(thresholdRows);
          } else { // else that row of cells goes into OTHER
            otherRows.addAll(thresholdRows);
            includeOther = true;
            for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
              Row row = thresholdRows.get(timeBucketId);
              List<Metric> metrics = row.getMetrics();
              for (int i = 0; i < metrics.size(); i++) {
                Metric metricToAdd = metrics.get(i);
                otherBaselineMetrics.get(timeBucketId)[i] += metricToAdd.getBaselineValue();
                otherCurrentMetrics.get(timeBucketId)[i] += metricToAdd.getCurrentValue();
              }
            }
          }
        }

        // include OTHER row
        if (includeOther) {
          for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
            Builder otherBuilder = otherBuilders.get(timeBucketId);
            double[] otherBaseline = otherBaselineMetrics.get(timeBucketId);
            double[] otherCurrent = otherCurrentMetrics.get(timeBucketId);
            for (int i = 0; i < numMetrics; i++) {
              otherBuilder.addMetric(metricFunctions.get(i).getMetricName(), otherBaseline[i], otherCurrent[i]);
            }
            rows.add(otherBuilder.build());
          }
        }
      } else {
        // group by time only //tabular
        baselineResponseMap = createResponseMapByTime(baselineResponse);
        currentResponseMap = createResponseMapByTime(currentResponse);

        for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
          Range<DateTime> baselineTimeRange = baselineRanges.get(timeBucketId);
          ThirdEyeResponseRow baselineRow =
              baselineResponseMap.get(String.valueOf(timeBucketId));

          Range<DateTime> currentTimeRange = currentRanges.get(timeBucketId);
          ThirdEyeResponseRow currentRow = currentResponseMap.get(String.valueOf(timeBucketId));

          Row.Builder builder = new Row.Builder();
          builder.setBaselineStart(baselineTimeRange.lowerEndpoint());
          builder.setBaselineEnd(baselineTimeRange.upperEndpoint());
          builder.setCurrentStart(currentTimeRange.lowerEndpoint());
          builder.setCurrentEnd(currentTimeRange.upperEndpoint());

          addMetric(baselineRow, currentRow, builder);
          Row row = builder.build();
          rows.add(row);
        }
      }
    } else {
      // no break down by time
      if (hasGroupByDimensions) {
        // group by dimensions only //heatmap
        baselineResponseMap = createResponseMapByDimension(baselineResponse);
        currentResponseMap = createResponseMapByDimension(currentResponse);

        String dimensionName = baselineResponse.getGroupKeyColumns().get(0);

        Set<String> dimensionValues = new HashSet<>();
        dimensionValues.addAll(baselineResponseMap.keySet());
        dimensionValues.addAll(currentResponseMap.keySet());


        for (String dimensionValue : dimensionValues) {
          Row.Builder builder = new Row.Builder();
          builder.setBaselineStart(baselineRanges.get(0).lowerEndpoint());
          builder.setBaselineEnd(baselineRanges.get(0).upperEndpoint());
          builder.setCurrentStart(currentRanges.get(0).lowerEndpoint());
          builder.setCurrentEnd(currentRanges.get(0).upperEndpoint());

          builder.setDimensionName(dimensionName);
          builder.setDimensionValue(dimensionValue);

          ThirdEyeResponseRow baselineRow = baselineResponseMap.get(dimensionValue);
          ThirdEyeResponseRow currentRow = currentResponseMap.get(dimensionValue);

          addMetric(baselineRow, currentRow, builder);

          Row row = builder.build();
          rows.add(row);
        }

        double[] baselineMetricSums = getMetricSums(baselineResponse);
        double[] currentMetricSums = getMetricSums(currentResponse);
        rows = getRowsWithThresholdFiltering(rows, dimensionName, metricFunctions, baselineMetricSums, currentMetricSums);

      } else {
        // no group by
      }
    }

    return rows;
  }

  private List<Row> getRowsWithThresholdFiltering(List<Row> rows, String dimensionName,
      List<MetricFunction> metricFunctions, double[] baselineMetricSums, double[] currentMetricSums) {

    int numMetrics = currentResponse.getMetricFunctions().size();

    // Collect rows which pass threshold
    List<Row> thresholdPassedRows = new ArrayList<>();

    // Construct OTHER row
    Row.Builder otherBuilder = new Row.Builder();
    otherBuilder.setBaselineStart(baselineRanges.get(0).lowerEndpoint());
    otherBuilder.setBaselineEnd(baselineRanges.get(0).upperEndpoint());
    otherBuilder.setCurrentStart(currentRanges.get(0).lowerEndpoint());
    otherBuilder.setCurrentEnd(currentRanges.get(0).upperEndpoint());
    otherBuilder.setDimensionName(dimensionName);
    otherBuilder.setDimensionValue(OTHER);
    double[] otherBaseline = new double[numMetrics];
    Arrays.fill(otherBaseline, 0);
    double[] otherCurrent = new double[numMetrics];
    Arrays.fill(otherCurrent, 0);
    boolean includeOther = false;

    for (Row row : rows) {
      boolean passedThreshold = false;
      List<Metric> metrics = row.getMetrics();
      for (int i = 0; i < numMetrics; i ++) {
        double baselineValue = metrics.get(i).getBaselineValue();
        double currentValue = metrics.get(i).getCurrentValue();
        if ((baselineValue >= baselineMetricSums[i] * metricThreshold) ||
            (currentValue >= currentMetricSums[i] * metricThreshold)) {
          passedThreshold = true;
          break;
        }
      }

      if (passedThreshold) {  // if any metric passes threshold, include it
        thresholdPassedRows.add(row);
      } else { // else add it to OTHER
        includeOther = true;
        for (int i = 0; i < numMetrics; i ++) {
          Metric metric = metrics.get(i);
          otherBaseline[i] += metric.getBaselineValue();
          otherCurrent[i] += metric.getCurrentValue();
        }
      }
    }
    if (includeOther) {
      for (int i = 0; i < numMetrics; i ++) {
        otherBuilder.addMetric(metricFunctions.get(i).getMetricName(), otherBaseline[i], otherCurrent[i]);
      }
      thresholdPassedRows.add(otherBuilder.build());
    }

    return thresholdPassedRows;
  }

  private static Map<String, ThirdEyeResponseRow> createResponseMapByTimeAndDimension(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap;
    responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      responseMap.put(
          thirdEyeResponseRow.getTimeBucketId() + "|" + thirdEyeResponseRow.getDimensions().get(0),
          thirdEyeResponseRow);
    }
    return responseMap;
  }

  private static Map<String, ThirdEyeResponseRow> createResponseMapByTime(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap;
    responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      responseMap.put(String.valueOf(thirdEyeResponseRow.getTimeBucketId()), thirdEyeResponseRow);
    }
    return responseMap;
  }

  private static Map<String, ThirdEyeResponseRow> createResponseMapByDimension(
      ThirdEyeResponse thirdEyeResponse) {
    Map<String, ThirdEyeResponseRow> responseMap;
    responseMap = new HashMap<>();
    int numRows = thirdEyeResponse.getNumRows();
    for (int i = 0; i < numRows; i++) {
      ThirdEyeResponseRow thirdEyeResponseRow = thirdEyeResponse.getRow(i);
      responseMap.put(thirdEyeResponseRow.getDimensions().get(0), thirdEyeResponseRow);
    }
    return responseMap;
  }

  private void addMetric(ThirdEyeResponseRow baselineRow, ThirdEyeResponseRow currentRow, Builder builder) {

    List<MetricFunction> metricFunctions = baselineResponse.getMetricFunctions();

    for (int i = 0; i < metricFunctions.size(); i++) {
      MetricFunction metricFunction = metricFunctions.get(i);
      double baselineValue = 0;
      if (baselineRow != null) {
        baselineValue = baselineRow.getMetrics().get(i);
      }
      double currentValue = 0;
      if (currentRow != null) {
        currentValue = currentRow.getMetrics().get(i);
      }
      builder.addMetric(metricFunction.getMetricName(), baselineValue, currentValue);
    }
  }

  private double[] getMetricSums(ThirdEyeResponse response) {

    ThirdEyeRequest request = response.getRequest();
    double[] metricSums = new double[request.getMetricNames().size()];
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setCollection(request.getCollection());
    requestBuilder.setStartTimeInclusive(request.getStartTimeInclusive());
    requestBuilder.setEndTimeExclusive(request.getEndTimeExclusive());
    requestBuilder.setFilterSet(request.getFilterSet());
    requestBuilder.setMetricFunctions(request.getMetricFunctions());
    ThirdEyeRequest metricSumsRequest = requestBuilder.build("metricSums");
    ThirdEyeResponse metricSumsResponse = null;
    try {
      metricSumsResponse = CACHE_REGISTRY_INSTANCE.getQueryCache().getClient().execute(metricSumsRequest);
    } catch (Exception e) {
      LOGGER.error("Caught exception when executing metric sums request", e);
    }
    List<Double> metrics = metricSumsResponse.getRow(0).getMetrics();
    for (int i = 0; i < metrics.size(); i++) {
      metricSums[i] = metrics.get(i);
    }
    return metricSums;
  }

  private Map<Integer, List<Double>> getMetricSumsByTime(ThirdEyeResponse response) {

    ThirdEyeRequest request = response.getRequest();
    Map<Integer, List<Double>> metricSums = new HashMap<>();
    ThirdEyeRequestBuilder requestBuilder = ThirdEyeRequest.newBuilder();
    requestBuilder.setCollection(request.getCollection());
    requestBuilder.setStartTimeInclusive(request.getStartTimeInclusive());
    requestBuilder.setEndTimeExclusive(request.getEndTimeExclusive());
    requestBuilder.setFilterSet(request.getFilterSet());
    requestBuilder.setGroupByTimeGranularity(request.getGroupByTimeGranularity());
    requestBuilder.setMetricFunctions(request.getMetricFunctions());
    ThirdEyeRequest metricSumsRequest = requestBuilder.build("metricSums");
    ThirdEyeResponse metricSumsResponse = null;
    try {
      metricSumsResponse = CACHE_REGISTRY_INSTANCE.getQueryCache().getClient().execute(metricSumsRequest);
    } catch (Exception e) {
      LOGGER.error("Caught exception when executing metric sums request", e);
    }

    for (int i = 0; i < metricSumsResponse.getNumRows(); i++) {
      ThirdEyeResponseRow row = metricSumsResponse.getRow(i);
      metricSums.put(row.getTimeBucketId(), row.getMetrics());
    }
    return metricSums;
  }

  private boolean checkMetricSums(Row row, List<Double> baselineMetricSums, List<Double> currentMetricSums) {
    List<Metric> metrics = row.getMetrics();
    for (int i = 0; i < metrics.size(); i ++) {
      Metric metric = metrics.get(i);
      if (metric.getBaselineValue() >= metricThreshold * baselineMetricSums.get(i) ||
          metric.getCurrentValue() >= metricThreshold * currentMetricSums.get(i)) {
        return true;
      }
    }
    return false;
  }

}
