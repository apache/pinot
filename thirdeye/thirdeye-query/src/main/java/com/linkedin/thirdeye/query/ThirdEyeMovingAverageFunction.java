package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.impl.NumberUtils;

import java.util.*;

public class ThirdEyeMovingAverageFunction implements ThirdEyeFunction {
  private final TimeGranularity window;

  public ThirdEyeMovingAverageFunction(TimeGranularity window) {
    this.window = window;

    if (window.getSize() < 1) {
      throw new IllegalArgumentException("Must provide non-zero positive integer for window: " + window);
    }
  }

  public TimeGranularity getWindow() {
    return window;
  }

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, ThirdEyeQuery query, MetricTimeSeries timeSeries) {
    List<MetricType> metricTypes = new ArrayList<>();
    for (String metricName : timeSeries.getSchema().getNames()) {
      metricTypes.add(MetricType.DOUBLE);
    }
    List<String> metricNames = timeSeries.getSchema().getNames();
    MetricTimeSeries movingAverage = new MetricTimeSeries(new MetricSchema(metricNames, metricTypes));

    if (timeSeries.getTimeWindowSet().isEmpty()) {
      return movingAverage;
    }

    Long minTime = Collections.min(timeSeries.getTimeWindowSet());
    Long maxTime = Collections.max(timeSeries.getTimeWindowSet());

    // Convert window to collection time
    long collectionWindow = config.getTime().getBucket().getUnit().convert(window.getSize(), window.getUnit())
        / config.getTime().getBucket().getSize();
    Long startTime = minTime + collectionWindow;

    // Initialize
    Number[] currentSum = new Number[timeSeries.getSchema().getNumMetrics()];
    Arrays.fill(currentSum, 0);

    // Compute rest
    for (long time = minTime; time <= maxTime; time++) {
      for (int j = 0; j < movingAverage.getSchema().getNumMetrics(); j++) {
        String metricName = movingAverage.getSchema().getMetricName(j);
        MetricType metricType = movingAverage.getSchema().getMetricType(j);

        // Add current position to current sum
        Number newValue = timeSeries.get(time, metricName);
        currentSum[j] = NumberUtils.sum(
            currentSum[j],
            newValue.doubleValue() / collectionWindow,
            metricType);

        if (time >= startTime) {
          // Subtract one that fell out of window
          Number oldValue = timeSeries.get(time - collectionWindow, metricName);
          currentSum[j] = NumberUtils.difference(
              currentSum[j],
              oldValue.doubleValue() / collectionWindow,
              metricType);

          // Update new series
          movingAverage.increment(time, metricName, currentSum[j]);
        }
      }
    }

    return movingAverage;
  }
}
