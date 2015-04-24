package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.impl.NumberUtils;

import java.util.*;

public class ThirdEyeMovingAverageFunction implements ThirdEyeFunction {
  private final List<String> metricNames;
  private final TimeGranularity window;

  public ThirdEyeMovingAverageFunction(List<String> metricNames, TimeGranularity window) {
    this.metricNames = metricNames;
    this.window = window;

    if (window.getSize() < 1) {
      throw new IllegalArgumentException("Must provide non-zero positive integer for window: " + window);
    }
  }

  public TimeGranularity getWindow() {
    return window;
  }

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, MetricTimeSeries timeSeries) {
    MetricSchema schema = timeSeries.getSchema();
    MetricTimeSeries movingAverage = ThirdEyeFunctionUtils.copyBlankSeriesDouble(metricNames, schema);

    List<Long> sortedTimes = new ArrayList<Long>(timeSeries.getTimeWindowSet());
    Collections.sort(sortedTimes);

    // Convert window to collection time
    long collectionWindow = config.getTime().getBucket().getUnit().convert(window.getSize(), window.getUnit())
        / config.getTime().getBucket().getSize();

    // Initialize
    Number[] currentSum = new Number[timeSeries.getSchema().getNumMetrics()];
    Arrays.fill(currentSum, 0);
    for (int i = 0; i < collectionWindow; i++) {
      long time = sortedTimes.get(i);
      for (int j = 0; j < schema.getNumMetrics(); j++) {
        currentSum[j] = NumberUtils.sum(
            currentSum[j],
            timeSeries.get(time, schema.getMetricName(j)),
            schema.getMetricType(j));
      }
    }

    // Compute rest
    for (int i = (int) collectionWindow; i < sortedTimes.size(); i++) {
      long time = sortedTimes.get(i);

      // Subtract one that fell out of window
      for (int j = 0; j < schema.getNumMetrics(); j++) {
        currentSum[j] = NumberUtils.difference(
            currentSum[j],
            timeSeries.get(time - collectionWindow, schema.getMetricName(j)),
            schema.getMetricType(j));
      }

      // Add current position to current sum
      for (int j = 0; j < schema.getNumMetrics(); j++) {
        currentSum[j] = NumberUtils.sum(
            currentSum[j],
            timeSeries.get(time, schema.getMetricName(j)),
            schema.getMetricType(j));
      }

      // Update new series
      for (int j = 0; j < schema.getNumMetrics(); j++) {
        String metricName = schema.getMetricName(j);
        if (metricNames.contains(metricName)) {
          movingAverage.increment(time, metricName, currentSum[j]);
        }
      }
    }

    // Take average of each cell
    for (Long time : movingAverage.getTimeWindowSet()) {
      for (String metricName : schema.getNames()) {
        if (metricNames.contains(metricName)) {
          Number sum = movingAverage.get(time, metricName);
          Number average = NumberUtils.divide(sum, collectionWindow, schema.getMetricType(metricName));
          movingAverage.set(time, metricName, average);
        }
      }
    }

    return movingAverage;
  }
}
