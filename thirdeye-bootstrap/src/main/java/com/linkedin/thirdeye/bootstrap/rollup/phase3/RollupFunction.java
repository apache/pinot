package com.linkedin.thirdeye.bootstrap.rollup.phase3;

import java.util.Map;

import com.linkedin.thirdeye.bootstrap.DimensionKey;
import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;
import com.linkedin.thirdeye.bootstrap.rollup.RollupThresholdFunc;

public interface RollupFunction {
  /**
   * performs the roll up, returns one of the possible roll up that clear the
   * threshold
   * 
   * @param rawDimensionKey
   * @param possibleRollups
   * @return
   */
  DimensionKey rollup(DimensionKey rawDimensionKey,
      Map<DimensionKey, MetricTimeSeries> possibleRollups,
      RollupThresholdFunc func);
}
