package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.StarTreeRecord;

public interface AnomalyDetectionFunction
{
  /**
   * Computes whether or not this dimension combination + metric time series is anomalous.
   */
  AnomalyResult compute(StarTreeRecord record);
}
