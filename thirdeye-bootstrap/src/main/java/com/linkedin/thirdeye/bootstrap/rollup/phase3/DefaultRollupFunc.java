package com.linkedin.thirdeye.bootstrap.rollup.phase3;

import java.util.Map;

import com.linkedin.thirdeye.bootstrap.DimensionKey;
import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;
import com.linkedin.thirdeye.bootstrap.rollup.RollupThresholdFunc;

/**
 * Default implementation that selects the one rolls up minimum number of
 * dimensions and clears the threshold
 * 
 * @author kgopalak
 * 
 */
public class DefaultRollupFunc implements RollupFunction {

  @Override
  public DimensionKey rollup(DimensionKey rawDimensionKey,
      Map<DimensionKey, MetricTimeSeries> possibleRollups,
      RollupThresholdFunc func) {
    return null;
  }

}
