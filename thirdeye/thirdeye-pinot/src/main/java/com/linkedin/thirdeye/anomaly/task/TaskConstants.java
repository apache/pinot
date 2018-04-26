package com.linkedin.thirdeye.anomaly.task;

public class TaskConstants {

  public enum TaskType {
    DETECTION,
    ANOMALY_DETECTION,
    MERGE,
    // TODO: deprecate ALERT task type
    ALERT,
    ALERT2,
    MONITOR,
    DATA_COMPLETENESS,
    CLASSIFICATION,
    REPLAY
  }

  public enum TaskStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED,
    TIMEOUT
  }
}
