package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.StarTreeRecord;

public interface AnomalyDetectionFunction
{
  AnomalyResult compute(Iterable<StarTreeRecord> records);
}
