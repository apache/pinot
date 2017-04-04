package com.linkedin.thirdeye.client.timeseries;

import static com.linkedin.thirdeye.client.ResponseParserUtils.OTHER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ResponseParserUtils;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.ThirdEyeResponseRow;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.Builder;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.TimeSeriesMetric;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

//Heavily based off TimeOnTime equivalent
public class TimeSeriesResponseParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesResponseParser.class);
  private final ThirdEyeResponse response;
  private final List<Range<DateTime>> ranges;
  private final TimeGranularity aggTimeGranularity;
  private final List<String> groupByDimensions;

  private Map<String, ThirdEyeResponseRow> responseMap;
  private List<MetricFunction> metricFunctions;
  private int numMetrics;
  int numTimeBuckets;
  private List<TimeSeriesRow> rows;
  private Map<String, Double> metricThresholds = new HashMap<>();
  private boolean doRollUp = true; // roll up small metric to OTHER dimensions

  public TimeSeriesResponseParser(ThirdEyeResponse response, List<Range<DateTime>> ranges,
      TimeGranularity timeGranularity, List<String> groupByDimensions) {
    this.response = response;
    this.ranges = ranges;
    this.aggTimeGranularity = timeGranularity;
    this.groupByDimensions = groupByDimensions;

    metricFunctions = response.getMetricFunctions();
    metricThresholds = ThirdEyeUtils.getMetricThresholdsMap(metricFunctions);
  }

  public TimeSeriesResponseParser(ThirdEyeResponse response, List<Range<DateTime>> ranges,
      TimeGranularity timeGranularity, List<String> groupByDimensions, boolean doRollUp) {
    this(response, ranges, timeGranularity, groupByDimensions);
    this.doRollUp = doRollUp;
  }

  public List<TimeSeriesRow> parseResponse() {
    if (response == null) {
      return Collections.emptyList();
    }
    if (aggTimeGranularity == null) {
      throw new UnsupportedOperationException(
          "TimeSeriesResponse should always have group by time");
    }

    boolean hasGroupByDimensions = CollectionUtils.isNotEmpty(groupByDimensions);

    numMetrics = metricFunctions.size();
    numTimeBuckets = ranges.size();
    rows = new ArrayList<>();

    if (hasGroupByDimensions) {
      parseGroupByTimeDimensionResponse();
    } else {
      parseGroupByTimeResponse();
    }

    return rows;
  }

  private void parseGroupByTimeResponse() {
    responseMap = ResponseParserUtils.createResponseMapByTime(response);

    for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
      Range<DateTime> timeRange = ranges.get(timeBucketId);
      ThirdEyeResponseRow responseRow = responseMap.get(String.valueOf(timeBucketId));

      TimeSeriesRow.Builder builder = new TimeSeriesRow.Builder();
      builder.setStart(timeRange.lowerEndpoint());
      builder.setEnd(timeRange.upperEndpoint());

      addMetric(responseRow, builder);
      TimeSeriesRow row = builder.build();
      rows.add(row);
    }
  }

  private void parseGroupByTimeDimensionResponse() {
    responseMap = ResponseParserUtils.createResponseMapByTimeAndDimension(response);

    Map<Integer, List<Double>> metricSums = Collections.emptyMap();
    if (doRollUp) {
      metricSums = ResponseParserUtils.getMetricSumsByTime(response);
    }

    // group by time and dimension values
    Set<String> timeDimensionValues = new HashSet<>();
    timeDimensionValues.addAll(responseMap.keySet());
    Set<List<String>> dimensionValuesList = new HashSet<>();
    for (String timeDimensionValue : timeDimensionValues) {
      List<String> dimensionValues = ResponseParserUtils.extractDimensionValues(timeDimensionValue);
      dimensionValuesList.add(dimensionValues);
    }

    // group by dimension names (the 0th dimension, which is the time bucket, is skipped).
    List<String> groupKeyColumns = response.getGroupKeyColumns();
    List<String> dimensionNameList = new ArrayList<>(groupKeyColumns.size() - 1);
    for (int i = 1; i < groupKeyColumns.size(); ++i) {
      dimensionNameList.add(groupKeyColumns.get(i));
    }

    // other row
    List<TimeSeriesRow.Builder> otherBuilders = new ArrayList<>();
    List<double[]> otherMetrics = new ArrayList<>();
    boolean includeOther = false;
    // constructing an OTHER rows, 1 for each time bucket
    for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
      Range<DateTime> timeRange = ranges.get(timeBucketId);

      TimeSeriesRow.Builder builder = new TimeSeriesRow.Builder();
      builder.setStart(timeRange.lowerEndpoint());
      builder.setEnd(timeRange.upperEndpoint());
      builder.setDimensionNames(dimensionNameList);
      List<String> dimensionValues = new ArrayList(dimensionNameList.size());
      for (int i = 0; i < dimensionNameList.size(); ++i) {
        dimensionValues.add(OTHER);
      }
      builder.setDimensionValues(dimensionValues);
      otherBuilders.add(builder);
      double[] other = new double[numMetrics];
      Arrays.fill(other, 0);
      otherMetrics.add(other);
    }

    // for every row we construct, we check if any of its time buckets passes metric
    // threshold
    // if it does, we add it to the rows as is
    // else, we add the metric values to the OTHER row
    for (List<String> dimensionValues : dimensionValuesList) {
      List<TimeSeriesRow> thresholdRows = new ArrayList<>();
      for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
        Range<DateTime> timeRange = ranges.get(timeBucketId);

        // compute the time|dimension key
        String timeDimensionValue =
            ResponseParserUtils.computeTimeDimensionValues(timeBucketId, dimensionValues);

        ThirdEyeResponseRow responseRow = responseMap.get(timeDimensionValue);

        TimeSeriesRow.Builder builder = new TimeSeriesRow.Builder();
        builder.setStart(timeRange.lowerEndpoint());
        builder.setEnd(timeRange.upperEndpoint());
        builder.setDimensionNames(dimensionNameList);
        builder.setDimensionValues(dimensionValues);
        addMetric(responseRow, builder);

        TimeSeriesRow row = builder.build();
        thresholdRows.add(row);
      }

      boolean passedThreshold = false;
      if (doRollUp) {
        // check if rows pass threshold
        for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
          if (checkMetricSums(thresholdRows.get(timeBucketId), metricSums.get(timeBucketId))) {
            passedThreshold = true;
            break;
          }
        }
      } else {
        passedThreshold = true;
      }

      // if any of the cells of a contributor row passes threshold, add all those cells
      if (passedThreshold && !dimensionValues.contains(OTHER)) {
        rows.addAll(thresholdRows);
      } else { // else that row of cells goes into OTHER
        includeOther = true;
        for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
          TimeSeriesRow row = thresholdRows.get(timeBucketId);
          List<TimeSeriesMetric> metrics = row.getMetrics();
          for (int i = 0; i < metrics.size(); i++) {
            TimeSeriesMetric metricToAdd = metrics.get(i);
            otherMetrics.get(timeBucketId)[i] += metricToAdd.getValue();
          }
        }
      }
    }

    // create other row using the other sums
    if (includeOther) {
      for (int timeBucketId = 0; timeBucketId < numTimeBuckets; timeBucketId++) {
        Builder otherBuilder = otherBuilders.get(timeBucketId);
        double[] other = otherMetrics.get(timeBucketId);
        for (int i = 0; i < numMetrics; i++) {
          otherBuilder.addMetric(metricFunctions.get(i).getMetricName(), other[i]);
        }
        rows.add(otherBuilder.build());
      }
    }
  }

  /* Helper functions */

  private void addMetric(ThirdEyeResponseRow row, TimeSeriesRow.Builder builder) {
    List<MetricFunction> metricFunctions = response.getMetricFunctions();
    for (int i = 0; i < metricFunctions.size(); i++) {
      MetricFunction metricFunction = metricFunctions.get(i);
      double value = 0;
      if (row != null) {
        value = row.getMetrics().get(i);
      }
      builder.addMetric(metricFunction.getMetricName(), value);
    }
  }

  private boolean checkMetricSums(TimeSeriesRow row, List<Double> metricSums) {
    List<TimeSeriesMetric> metrics = row.getMetrics();
    for (int i = 0; i < metrics.size(); i++) {
      TimeSeriesMetric metric = metrics.get(i);
      double sum = 0;
      if (metricSums != null) {
        sum = metricSums.get(i);
      }
      if (metric.getValue() > metricThresholds.get(metric.getMetricName()) * sum) {
        return true;
      }
    }
    return false;
  }
}
