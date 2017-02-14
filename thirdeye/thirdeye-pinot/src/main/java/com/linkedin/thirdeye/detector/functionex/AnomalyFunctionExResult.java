package com.linkedin.thirdeye.detector.functionex;

public class AnomalyFunctionExResult {
  boolean anomaly;
  String message;
  AnomalyFunctionExContext context;

  public boolean isAnomaly() {
    return anomaly;
  }

  public void setAnomaly(boolean anomaly) {
    this.anomaly = anomaly;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public AnomalyFunctionExContext getContext() {
    return context;
  }

  public void setContext(AnomalyFunctionExContext context) {
    this.context = context;
  }
}
