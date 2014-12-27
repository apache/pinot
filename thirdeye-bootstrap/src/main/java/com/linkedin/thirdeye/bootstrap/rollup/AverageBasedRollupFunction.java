package com.linkedin.thirdeye.bootstrap.rollup;

import java.util.Map;
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
}
