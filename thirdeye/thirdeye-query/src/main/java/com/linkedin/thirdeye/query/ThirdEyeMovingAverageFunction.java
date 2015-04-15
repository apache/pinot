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

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, MetricTimeSeries timeSeries) {
    MetricSchema schema = timeSeries.getSchema();
    MetricTimeSeries movingAverage = ThirdEyeFunctionUtils.copyBlankSeriesDouble(metricNames, schema);

    List<Long> sortedTimes = new ArrayList<Long>(timeSeries.getTimeWindowSet());
    Collections.sort(sortedTimes);

    // Convert window to collection time
    long collectionWindow = config.getTime().getBucket().getUnit().convert(window.getSize(), window.getUnit())
        / config.getTime().getBucket().getSize();

    // Compute cumulative sum
    for (int i = (int) collectionWindow; i < sortedTimes.size(); i++) {
      long time = sortedTimes.get(i);
      for (int j = (int) (i - collectionWindow + 1); j <= i; j++) {
        for (int k = 0; k < schema.getNumMetrics(); k++) {
          long includedTime = sortedTimes.get(j);
          String metricName = schema.getMetricName(k);
          Number metricValue = timeSeries.get(includedTime, metricName);

          if (metricNames.contains(metricName)) {
            movingAverage.increment(time, metricName, metricValue);
          }
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
