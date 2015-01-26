package com.linkedin.thirdeye;

public class AnomalyResult
{
  private final boolean isAnomaly;

  public AnomalyResult(boolean isAnomaly)
  {
    this.isAnomaly = isAnomaly;
  }

  public boolean isAnomaly()
  {
    return isAnomaly;
  }
}
