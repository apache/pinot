package com.linkedin.thirdeye.anomaly.task;

public class TaskConstants {

  public enum TaskType {
    ANOMALY_DETECTION,
    MERGE,
    ALERT,
    MONITOR
  }

  public enum TaskStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED
  }
}
