package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;

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
}
