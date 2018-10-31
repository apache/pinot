package com.linkedin.thirdeye.anomaly.task;

import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.anomaly.detection.DetectionTaskRunner;
import com.linkedin.thirdeye.anomaly.classification.ClassificationTaskRunner;
import com.linkedin.thirdeye.anomaly.monitor.MonitorTaskRunner;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessTaskRunner;


/**
 * This class returns an instance of the task runner depending on the task type
 */
public class TaskRunnerFactory {

  public static TaskRunner getTaskRunnerFromTaskType(TaskType taskType) {
    TaskRunner taskRunner = null;
    switch (taskType) {
      case ANOMALY_DETECTION:
        taskRunner = new DetectionTaskRunner();
        break;
      case MERGE:
        break;
      case MONITOR:
        taskRunner = new MonitorTaskRunner();
        break;
      case DATA_COMPLETENESS:
        taskRunner = new DataCompletenessTaskRunner();
        break;
      case ALERT:
      case ALERT2:
        taskRunner = new AlertTaskRunnerV2();
        break;
      case CLASSIFICATION:
        taskRunner = new ClassificationTaskRunner();
        break;
      default:
    }
    return taskRunner;
  }
}
