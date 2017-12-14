package com.linkedin.thirdeye.anomaly.task;

public class TaskConstants {

  public enum TaskType {
    ANOMALY_DETECTION,
    MERGE,
    // TODO: deprecate ALERT task type
    ALERT,
    ALERT2,
    MONITOR,
    DATA_COMPLETENESS,
    CLASSIFICATION,
    // TODO: deprecate NULL task type after old task are deleted from current (as of 4/4/2017) database
    NULL // for backward compatibility
  }

  public enum TaskStatus {
    WAITING,
    RUNNING,
    COMPLETED,
    FAILED,
    TIMEOUT
  }
}
