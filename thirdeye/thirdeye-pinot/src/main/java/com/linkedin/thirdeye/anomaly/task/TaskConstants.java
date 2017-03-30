package com.linkedin.thirdeye.anomaly.task;

public class TaskConstants {

  public enum TaskType {
    ANOMALY_DETECTION,
    MERGE,

    // todo : mark ALERT deprecated
    ALERT,
    ALERT2,
    MONITOR,
    DATA_COMPLETENESS,
    CLASSIFICATION
  }

  public enum TaskStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED
  }
}
