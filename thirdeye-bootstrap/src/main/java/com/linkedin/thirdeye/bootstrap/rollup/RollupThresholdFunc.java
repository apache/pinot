package com.linkedin.thirdeye.bootstrap.rollup;

import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;

/**
 * Interface that will be invoked by the roll up phase to check if a row passes
 * the threshold.
 * 
 * @author kgopalak
 * 
 */
public interface RollupThresholdFunc {

  public boolean isAboveThreshold(MetricTimeSeries timeSeries);

}
