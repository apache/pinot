package com.linkedin.thirdeye.datasource.comparison;

import static com.linkedin.thirdeye.datasource.ResponseParserUtils.OTHER;

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
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ResponseParserUtils;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.ThirdEyeResponseRow;
import com.linkedin.thirdeye.datasource.comparison.Row.Builder;
import com.linkedin.thirdeye.datasource.comparison.Row.Metric;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class TimeOnTimeResponseParser {

  private final ThirdEyeResponse baselineResponse;
  private final ThirdEyeResponse currentResponse;
  private final List<Range<DateTime>> baselineRanges;
  private final List<Range<DateTime>> currentRanges;
  private final TimeGranularity aggTimeGranularity;
  private final List<String> groupByDimensions;

  public static final Logger LOGGER = LoggerFactory.getLogger(TimeOnTimeResponseParser.class);

  private Map<String, ThirdEyeResponseRow> baselineResponseMap;
  private Map<String, ThirdEyeResponseRow> currentResponseMap;
  private List<MetricFunction> metricFunctions;
  private int numMetrics;
  int numTimeBuckets;
  private List<Row> rows;
  Map<String, Double> metricThresholds = new HashMap<>();

  public TimeOnTimeResponseParser(ThirdEyeResponse baselineResponse, ThirdEyeResponse currentResponse, List<Range<DateTime>> baselineRanges,
      List<Range<DateTime>> currentRanges, TimeGranularity timeGranularity, List<String> groupByDimensions) {
    this.baselineResponse = baselineResponse;
    this.currentResponse = currentResponse;
    this.baselineRanges = baselineRanges;
    this.currentRanges = currentRanges;
    this.aggTimeGranularity = timeGranularity;
    this.groupByDimensions = groupByDimensions;

    metricFunctions = baselineResponse.getMetricFunctions();
    metricThresholds = ThirdEyeUtils.getMetricThresholdsMap(metricFunctions);
  }

  public List<Row> parseResponse() {
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

    metricFunctions = baselineResponse.getMetricFunctions();
    numMetrics = metricFunctions.size();
    numTimeBuckets = Math.min(currentRanges.size(), baselineRanges.size());
    if (currentRanges.size() != baselineRanges.size()) {
      LOGGER.info("Current and baseline time series have different length, which could be induced by DST.");
    }
    rows = new ArrayList<>();

    if (hasGroupByTime) {
      if (hasGroupByDimensions) { // contributor view
        parseGroupByTimeDimensionResponse();
      } else { // tabular view
        parseGroupByTimeResponse();
      }
    } else {
      if (hasGroupByDimensions) { // heatmap
        parseGroupByDimensionResponse();
      } else {
        throw new UnsupportedOperationException("Response cannot have neither group by time nor group by dimension");
      }
    }
    return rows;
  }

  private void parseGroupByDimensionResponse() {
    baselineResponseMap = ResponseParserUtils.createResponseMapByDimension(baselineResponse);
    currentResponseMap = ResponseParserUtils.createResponseMapByDimension(currentResponse);

    List<Double> baselineMetricSums = ResponseParserUtils.getMetricSums(baselineResponse);
    List<Double> currentMetricSums = ResponseParserUtils.getMetricSums(currentResponse);

    // group by dimension name
    String dimensionName = baselineResponse.getGroupKeyColumns().get(0);

    // group by dimension values
    Set<String> dimensionValues = new HashSet<>();
    dimensionValues.addAll(baselineResponseMap.keySet());
    dimensionValues.addAll(currentResponseMap.keySet());

    // Construct OTHER row
    Row.Builder otherBuilder = new Row.Builder();
    otherBuilder.setBaselineStart(baselineRanges.get(0).lowerEndpoint());
    otherBuilder.setBaselineEnd(baselineRanges.get(0).upperEndpoint());
    otherBuilder.setCurrentStart(currentRanges.get(0).lowerEndpoint());
    otherBuilder.setCurrentEnd(currentRanges.get(0).upperEndpoint());
    otherBuilder.setDimensionName(dimensionName);
    otherBuilder.setDimensionValue(OTHER);
    Double[] otherBaseline = new Double[numMetrics];
    Arrays.fill(otherBaseline, 0.0);
    Double[] otherCurrent = new Double[numMetrics];
    Arrays.fill(otherCurrent, 0.0);
    boolean includeOther = false;

    // for every dimension value, we check if the row we constructed passes metric threshold
    // if it does, we add it to the rows
    // else, include it in the OTHER row
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

      boolean passedThreshold = checkMetricSums(row, baselineMetricSums, currentMetricSums);
      // if any non-OTHER metric passes threshold, include it
      if (passedThreshold && !dimensionValue.equalsIgnoreCase(OTHER)) {
        rows.add(row);
      } else { // else add it to OTHER
        includeOther = true;
        List<Metric> metrics = row.getMetrics();
        for (int i = 0; i < numMetrics; i++) {
          Metric metric = metrics.get(i);
          otherBaseline[i] += metric.getBaselineValue();
          otherCurrent[i] += metric.getCurrentValue();
        }

      }
    }
    if (includeOther) {
      for (int i = 0; i < numMetrics; i++) {
        otherBuilder.addMetric(metricFunctions.get(i).getMetricName(), otherBaseline[i], otherCurrent[i]);
      }
      Row row = otherBuilder.build();
      if (isValidMetric(row, Arrays.asList(otherBaseline), Arrays.asList(otherCurrent))) {
        rows.add(row);
      }
    }
  }

  private void parseGroupByTimeResponse() {
    baselineResponseMap = ResponseParserUtils.createResponseMapByTime(baselineResponse);
    currentResponseMap = ResponseParserUtils.createResponseMapByTime(currentResponse);

    for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
      Range<DateTime> baselineTimeRange = baselineRanges.get(timeBucketId);
      ThirdEyeResponseRow baselineRow = baselineResponseMap.get(String.valueOf(timeBucketId));
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

  private void parseGroupByTimeDimensionResponse() {

    baselineResponseMap = ResponseParserUtils.createResponseMapByTimeAndDimension(baselineResponse);
    currentResponseMap = ResponseParserUtils.createResponseMapByTimeAndDimension(currentResponse);

    Map<Integer, List<Double>> baselineMetricSums = ResponseParserUtils.getMetricSumsByTime(baselineResponse);
    Map<Integer, List<Double>> currentMetricSums = ResponseParserUtils.getMetricSumsByTime(currentResponse);

    // group by time and dimension values
    Set<String> timeDimensionValues = new HashSet<>();
    timeDimensionValues.addAll(baselineResponseMap.keySet());
    timeDimensionValues.addAll(currentResponseMap.keySet());
    Set<String> dimensionValues = new HashSet<>();
    for (String timeDimensionValue : timeDimensionValues) {
      String dimensionValue = ResponseParserUtils.extractFirstDimensionValue(timeDimensionValue);
      dimensionValues.add(dimensionValue);
    }

    // group by dimension name
    String dimensionName = baselineResponse.getGroupKeyColumns().get(1);

    // other row
    List<Row.Builder> otherBuilders = new ArrayList<>();
    List<Double[]> otherBaselineMetrics = new ArrayList<>();
    List<Double[]> otherCurrentMetrics = new ArrayList<>();
    boolean includeOther = false;
    // constructing an OTHER rows, 1 for each time bucket
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
      Double[] otherBaseline = new Double[numMetrics];
      Arrays.fill(otherBaseline, 0.0);
      Double[] otherCurrent = new Double[numMetrics];
      Arrays.fill(otherCurrent, 0.0);
      otherBaselineMetrics.add(otherBaseline);
      otherCurrentMetrics.add(otherCurrent);
    }

    // for every comparison row we construct, we check if any of its time buckets passes metric
    // threshold
    // if it does, we add it to the rows as is
    // else, we add the metric values to the OTHER row
    for (String dimensionValue : dimensionValues) {
      List<Row> thresholdRows = new ArrayList<>();
      for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
        Range<DateTime> baselineTimeRange = baselineRanges.get(timeBucketId);
        Range<DateTime> currentTimeRange = currentRanges.get(timeBucketId);

        // compute the time|dimension key
        String baselineTimeDimensionValue = ResponseParserUtils.computeTimeDimensionValue(timeBucketId, dimensionValue);
        String currentTimeDimensionValue = baselineTimeDimensionValue;

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

      // check if rows pass threshold
      boolean passedThreshold = false;
      for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
        if (checkMetricSums(thresholdRows.get(timeBucketId), baselineMetricSums.get(timeBucketId), currentMetricSums.get(timeBucketId))) {
          passedThreshold = true;
          break;
        }
      }

      if (passedThreshold) { // if any of the cells of a contributor row passes threshold, add all
                             // those cells
        rows.addAll(thresholdRows);
      } else { // else that row of cells goes into OTHER
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

    // create other row using the other baseline and current sums
    if (includeOther) {
      for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
        Builder otherBuilder = otherBuilders.get(timeBucketId);
        Double[] otherBaseline = otherBaselineMetrics.get(timeBucketId);
        Double[] otherCurrent = otherCurrentMetrics.get(timeBucketId);
        for (int i = 0; i < numMetrics; i++) {
          otherBuilder.addMetric(metricFunctions.get(i).getMetricName(), otherBaseline[i], otherCurrent[i]);
        }
        Row row = otherBuilder.build();
        if (isValidMetric(row, Arrays.asList(otherBaseline), Arrays.asList(otherCurrent))) {
          rows.add(row);
        }
      }
    }
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

  private boolean checkMetricSums(Row row, List<Double> baselineMetricSums, List<Double> currentMetricSums) {
    return isValidMetric(row, baselineMetricSums, currentMetricSums);
  }

  boolean isValidMetric(Row row, List<Double> baselineMetricSums, List<Double> currentMetricSums) {
    List<Metric> metrics = row.getMetrics();

    for (int i = 0; i < metrics.size(); i++) {
      Metric metric = metrics.get(i);

      double baselineSum = 0;
      if (baselineMetricSums != null) {
        baselineSum = baselineMetricSums.get(i);
      }
      double currentSum = 0;
      if (currentMetricSums != null) {
        currentSum = currentMetricSums.get(i);
      }
      Double thresholdFraction = metricThresholds.get(metric.getMetricName());
      if (metric.getBaselineValue() > thresholdFraction * baselineSum || metric.getCurrentValue() > thresholdFraction * currentSum) {
        return true;
      }
    }
    return false;
  }
}
