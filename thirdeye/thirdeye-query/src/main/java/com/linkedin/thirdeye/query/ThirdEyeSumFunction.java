package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.List;

public class ThirdEyeSumFunction implements ThirdEyeFunction {
  private final List<String> metricNames;

  public ThirdEyeSumFunction(List<String> metricNames) {
    this.metricNames = metricNames;
  }

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, MetricTimeSeries timeSeries) {
    MetricTimeSeries sum = ThirdEyeFunctionUtils.copyBlankSeriesSame(metricNames, timeSeries.getSchema());

    for (Long time : timeSeries.getTimeWindowSet()) {
      for (String metricName : metricNames) {
        sum.increment(0, metricName, timeSeries.get(time, metricName));
      }
    }

    return sum;
  }
}
