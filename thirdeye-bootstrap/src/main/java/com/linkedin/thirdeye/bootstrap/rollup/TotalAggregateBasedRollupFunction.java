package com.linkedin.thirdeye.bootstrap.rollup;

import java.util.Set;

import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;
/**
 * 
 * @author kgopalak
 *
 */
public class TotalAggregateBasedRollupFunction implements RollupThresholdFunc{

  private String metricName;
  private int totalAggregateThreshold;
  public TotalAggregateBasedRollupFunction(String metricName, int totalAggregateThreshold){
    this.metricName = metricName;
    this.totalAggregateThreshold = totalAggregateThreshold;
    
  }
  /**
   * 
   */
  @Override
  public boolean isAboveThreshold(MetricTimeSeries timeSeries) {
    Set<Long> timeWindowSet = timeSeries.getTimeWindowSet();
    long sum = 0;
    for (Long timeWindow : timeWindowSet) {
      sum += timeSeries.get(timeWindow, metricName);
    }
    return sum  >= totalAggregateThreshold; 
  }

}
