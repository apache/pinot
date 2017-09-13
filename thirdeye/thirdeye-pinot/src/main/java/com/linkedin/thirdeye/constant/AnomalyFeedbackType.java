package com.linkedin.thirdeye.constant;

public enum AnomalyFeedbackType {
  ANOMALY("Confirmed Anomaly"),
  NOT_ANOMALY("False Alarm"),
  ANOMALY_NEW_TREND("New Trend"),
  NO_FEEDBACK("Not Resolved");

  String userReadableName = null;

  AnomalyFeedbackType(String userReadableName) {
    this.userReadableName = userReadableName;
  }

  public String getUserReadableName() {
    return this.userReadableName;
  }
}
