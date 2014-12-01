package com.linkedin.thirdeye.bootstrap.rollup;

import java.util.Set;

import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;
/**
 * 
 * @author kgopalak
 *
 */
public class AverageBasedRollupFunction implements RollupThresholdFunc {

  private String metricName;
  private int averageThreshold;

  public AverageBasedRollupFunction(String metricName, int averageThreshold) {
    this.metricName = metricName;
    this.averageThreshold = averageThreshold;
  }

  @Override
  public boolean isAboveThreshold(MetricTimeSeries timeSeries) {
    Set<Long> timeWindowSet = timeSeries.getTimeWindowSet();
    long sum = 0;
    for (Long timeWindow : timeWindowSet) {
      sum += timeSeries.get(timeWindow, metricName);
    }
    return sum / timeWindowSet.size() >= averageThreshold;
  }
}
