package com.linkedin.thirdeye.api;

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

  /**
   *
   * @return null if the aggregation is across the complete time dimension
   * else an object of type TimeGranularity
   */
  public TimeGranularity getRollupAggregationGranularity();

}
