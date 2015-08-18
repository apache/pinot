package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;

import java.util.*;

public class ThirdEyeAggregateFunction implements ThirdEyeFunction{
  private final TimeGranularity window;

  public ThirdEyeAggregateFunction(TimeGranularity window) {
    this.window = window;
  }

  public TimeGranularity getWindow() {
    return window;
  }

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, ThirdEyeQuery query, MetricTimeSeries timeSeries) {
    MetricTimeSeries aggregate = ThirdEyeFunctionUtils.copyBlankSeriesSame(timeSeries.getSchema().getNames(), timeSeries.getSchema());

    if (timeSeries.getTimeWindowSet().isEmpty()) {
      return aggregate;
    }

    // Convert window to collection time
    long collectionWindow = config.getTime().getBucket().getUnit().convert(window.getSize(), window.getUnit())
        / config.getTime().getBucket().getSize();

    if (collectionWindow == 0) {
      throw new IllegalArgumentException("Minimum aggregation granularity is "
          + config.getTime().getBucket().getSize() + " " + config.getTime().getBucket().getUnit());
    }

    Long minTime = Collections.min(timeSeries.getTimeWindowSet());
    Long maxTime = Collections.max(timeSeries.getTimeWindowSet());

    for (long i = minTime; i < maxTime; i += collectionWindow) {
      long alignedTime = (i / collectionWindow) * collectionWindow;
      for (long j = i; j < i + collectionWindow; j++) {
        for (String metricName : timeSeries.getSchema().getNames()) {
          Number metricValue = timeSeries.get(j, metricName);
          if (metricValue != null) {
            aggregate.increment(alignedTime, metricName, metricValue);
          }
        }
      }
    }

    return aggregate;
  }
}
