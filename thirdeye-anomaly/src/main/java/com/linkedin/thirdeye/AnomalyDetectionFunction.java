package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;

public interface AnomalyDetectionFunction
{
  /**
   * Computes whether or not this dimension combination + metric time series is anomalous.
   */
  AnomalyResult analyze(DimensionKey dimensionKey, MetricTimeSeries metricTimeSeries);
}
