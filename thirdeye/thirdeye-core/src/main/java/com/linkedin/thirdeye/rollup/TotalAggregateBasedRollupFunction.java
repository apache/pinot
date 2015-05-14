package com.linkedin.thirdeye.rollup;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.RollupThresholdFunction;
import com.linkedin.thirdeye.api.TimeGranularity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author kgopalak
 *
 */
public class TotalAggregateBasedRollupFunction implements RollupThresholdFunction
{
  private static final Logger LOGGER = LoggerFactory
      .getLogger(TotalAggregateBasedRollupFunction.class);
  private String metricName;
  private int totalAggregateThreshold;
  public TotalAggregateBasedRollupFunction(Map<String, String> params){
    this.metricName = params.get("metricName");
    this.totalAggregateThreshold = Integer.parseInt(params.get("threshold"));
  }
  /**
   *
   */
  @Override
  public boolean isAboveThreshold(MetricTimeSeries timeSeries) {
    Set<Long> timeWindowSet = timeSeries.getTimeWindowSet();
    long sum = 0;
    for (Long timeWindow : timeWindowSet) {
      sum += timeSeries.get(timeWindow, metricName).longValue();
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("sum = " + sum);
    }
    return sum  >= totalAggregateThreshold;
  }

  @Override
  public TimeGranularity getRollupAggregationGranularity(){
    return null;
  }

}
