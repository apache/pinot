package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ThirdEyeAggregateFunction implements ThirdEyeFunction{
  private final List<String> metricNames;
  private final TimeGranularity window;

  public ThirdEyeAggregateFunction(List<String> metricNames, TimeGranularity window) {
    this.metricNames = metricNames;
    this.window = window;
  }

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, MetricTimeSeries timeSeries) {
    MetricTimeSeries aggregate = ThirdEyeFunctionUtils.copyBlankSeriesSame(metricNames, timeSeries.getSchema());

    // Convert window to collection time
    long collectionWindow = config.getTime().getBucket().getUnit().convert(window.getSize(), window.getUnit())
        / config.getTime().getBucket().getSize();

//    int numTimes = timeSeries.getTimeWindowSet().size();
//    if (numTimes % collectionWindow != 0) {
//      throw new IllegalArgumentException("time range % collectionWindow must be zero: "
//          + numTimes + " % " + collectionWindow + " == " + (numTimes % collectionWindow));
//    }

    List<Long> sortedTimes = new ArrayList<>(timeSeries.getTimeWindowSet());
    Collections.sort(sortedTimes);

    long timeRange = sortedTimes.get(sortedTimes.size() - 1) - sortedTimes.get(0) + 1; // inclusive
    if (timeRange % collectionWindow != 0) {
      throw new IllegalArgumentException("timeRange % collectionWindow != 0! " +
          "timeRange=" + timeRange + ", collectionWindow=" + collectionWindow);
    }

    for (int i = 0; i < sortedTimes.size(); i += collectionWindow) {
      long time = sortedTimes.get(i);
      for (int j = i; j < i + collectionWindow; j++) {
        for (String metricName : metricNames) {
          Number metricValue = timeSeries.get(sortedTimes.get(j), metricName);
          aggregate.increment(time, metricName, metricValue);
        }
      }
    }

    return aggregate;
  }
}
