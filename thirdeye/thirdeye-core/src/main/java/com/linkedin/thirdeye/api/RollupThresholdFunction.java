package com.linkedin.thirdeye.api;

import com.linkedin.thirdeye.api.MetricTimeSeries;

/**
 * Interface that will be invoked by the roll up phase to check if a row passes
 * the threshold.
 * 
 * @author kgopalak
 * 
 */
public interface RollupThresholdFunction
{
  /**
   * check if the timeseries clears the threshold. <br/>
   * possible implementations <br/>
   * Based on total aggregate <br/>
   * Based on average metric <br/>
   * Based on the consistency in timeseries <br/>
   * @param timeSeries
   * @return
   */
  public boolean isAboveThreshold(MetricTimeSeries timeSeries);

}
