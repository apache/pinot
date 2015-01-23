package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.StarTreeRecord;

public abstract class UnivariateAnomalyDetectionFunction implements AnomalyDetectionFunction
{
  @Override
  public AnomalyResult compute(Iterable<StarTreeRecord> records)
  {
    AnomalyResult result = null;

    for (StarTreeRecord record : records)
    {
      result = AnomalyResult.merge(result, compute(record));
    }

    return result;
  }

  protected abstract AnomalyResult compute(StarTreeRecord record);
}
