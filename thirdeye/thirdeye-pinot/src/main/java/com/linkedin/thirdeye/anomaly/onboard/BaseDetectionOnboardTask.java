package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import org.apache.commons.lang.exception.ExceptionUtils;

public abstract class BaseDetectionOnboardTask implements DetectionOnboardTask {
  private final String taskName;
  protected DetectionOnboardTaskContext taskContext = new DetectionOnboardTaskContext();

  public BaseDetectionOnboardTask(String taskName) {
    Preconditions.checkNotNull(taskName);
    this.taskName = taskName;
  }

  @Override
  public String getTaskName() {
    return taskName;
  }

  @Override
  public void setTaskContext(DetectionOnboardTaskContext taskContext) {
    Preconditions.checkNotNull(taskContext);
    this.taskContext = taskContext;
  }

  @Override
  public DetectionOnboardTaskContext getTaskContext() {
    return taskContext;
  }

  @Override
  public DetectionOnboardTaskStatus call() throws Exception {
    DetectionOnboardTaskStatus taskStatus = new DetectionOnboardTaskStatus();
    taskStatus.setTaskStatus(TaskConstants.TaskStatus.RUNNING);

    try {
      this.run();
    } catch (Exception e) {
      taskStatus.setTaskStatus(TaskConstants.TaskStatus.FAILED);
      taskStatus.setMessage(ExceptionUtils.getStackTrace(e));
    }
    taskStatus.setTaskStatus(TaskConstants.TaskStatus.COMPLETED);

    return taskStatus;
  }
}
