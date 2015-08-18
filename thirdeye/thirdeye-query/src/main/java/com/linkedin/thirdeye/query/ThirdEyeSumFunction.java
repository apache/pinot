package com.linkedin.thirdeye.query;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.List;

public class ThirdEyeSumFunction implements ThirdEyeFunction {
  private static final Joiner JOINER = Joiner.on(",");
  private final List<String> metricNames;

  public ThirdEyeSumFunction(List<String> metricNames) {
    this.metricNames = metricNames;
  }

  @Override
  public String toString() {
    return String.format("SUM(%s)", JOINER.join(metricNames));
  }

  @Override
  public MetricTimeSeries apply(StarTreeConfig config, ThirdEyeQuery query, MetricTimeSeries timeSeries) {
    MetricTimeSeries sum = ThirdEyeFunctionUtils.copyBlankSeriesSame(metricNames, timeSeries.getSchema());

    for (Long time : timeSeries.getTimeWindowSet()) {
      for (String metricName : metricNames) {
        sum.increment(0, metricName, timeSeries.get(time, metricName));
      }
    }

    return sum;
  }
}
