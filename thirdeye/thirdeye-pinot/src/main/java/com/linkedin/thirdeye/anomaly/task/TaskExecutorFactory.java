package com.linkedin.thirdeye.anomaly.task;

import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;

public class TaskExecutorFactory {

  public static Class<?> getTaskExecutorClassFromTaskType(TaskType taskType) {
    Class<?> taskExecutorClass = null;
    switch(taskType) {
      case ANOMALY_DETECTION:
        taskExecutorClass = AnomalyDetectionTaskRunner.class;
        break;
      case MERGE:
        break;
      case MONITOR:
        break;
      case REPORTER:
        break;
      default:
        break;

    }
    return taskExecutorClass;
  }

}
