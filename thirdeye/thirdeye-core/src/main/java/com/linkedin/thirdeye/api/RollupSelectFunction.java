package com.linkedin.thirdeye.api;

import java.util.Map;

public interface RollupSelectFunction {
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
      RollupThresholdFunction func);
}
