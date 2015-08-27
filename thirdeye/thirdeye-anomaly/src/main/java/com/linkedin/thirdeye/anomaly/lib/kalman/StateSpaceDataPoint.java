package com.linkedin.thirdeye.anomaly.lib.kalman;

public class StateSpaceDataPoint {

  public double predictedValue;
  public double actualValue;
  public double pValue;
  public long predictedDate;
  public double  stdError;
  public int index;

  public StateSpaceDataPoint(
      double predictedValue,
      double actualValue,
      double pValue,
      double stdError,
      long predictedDate,
      int index)
  {
    this.predictedValue = predictedValue;
    this.actualValue = actualValue;
    this.pValue = pValue;
    this.predictedDate = predictedDate;
    this.stdError = stdError;
    this.index = index;
  }
}
