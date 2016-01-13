package com.linkedin.thirdeye.rollup;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.RollupThresholdFunction;
import com.linkedin.thirdeye.api.TimeGranularity;

import java.util.Map;
import java.util.Set;

/**
 * @author kgopalak
 */

public class AverageBasedRollupFunction implements RollupThresholdFunction {

  private String metricName;
  private int averageThreshold;
  private int timeWindowSize;

  public AverageBasedRollupFunction(Map<String, String> params) {
    this.metricName = params.get("metricName");
    this.averageThreshold = Integer.parseInt(params.get("threshold"));
    this.timeWindowSize = Integer.parseInt(params.get("timeWindowSize"));

  }

  @Override
  public boolean isAboveThreshold(MetricTimeSeries timeSeries) {
    Set<Long> timeWindowSet = timeSeries.getTimeWindowSet();
    long sum = 0;
    for (Long timeWindow : timeWindowSet) {
      sum += timeSeries.get(timeWindow, metricName).longValue();
    }
    return sum / timeWindowSize >= averageThreshold;
  }

  @Override
  public TimeGranularity getRollupAggregationGranularity() {
    return null;
  }
}
