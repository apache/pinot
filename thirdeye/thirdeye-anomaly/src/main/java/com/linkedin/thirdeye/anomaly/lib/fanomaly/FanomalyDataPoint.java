package com.linkedin.thirdeye.anomaly.lib.fanomaly;

public class FanomalyDataPoint {
  public double predictedValue;
  public double actualValue;
  public double pValue;
  public long predictedDate;
  public double  stdError;
  public int index;
  public FanomalyDataPoint(double predicted_value,
      double actual_value, double p_value, double std_error,
      long predicted_date, int index) {
    predictedValue=predicted_value;
    actualValue=actual_value;
    pValue=p_value;
    stdError=std_error;
    predictedDate=predicted_date;
    this.index=index;
  }
}
