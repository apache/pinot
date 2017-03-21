package com.linkedin.thirdeye.anomaly.task;

public class TaskConstants {

  public enum TaskType {
    ANOMALY_DETECTION,
    ANOMALY_DETECTION_EX,
    MERGE,

    // todo : mark ALERT deprecated
    ALERT,
    ALERT2,
    MONITOR,
    DATA_COMPLETENESS
  }

  public enum TaskStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED
  }
}
