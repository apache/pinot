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

  public static AnomalyResult merge(AnomalyResult first, AnomalyResult second)
  {
    if (first == null)
    {
      return second;
    }
    else if (second == null)
    {
      return first;
    }

    boolean isAnomaly = first.isAnomaly() || second.isAnomaly();

    return new AnomalyResult(isAnomaly);
  }
}
