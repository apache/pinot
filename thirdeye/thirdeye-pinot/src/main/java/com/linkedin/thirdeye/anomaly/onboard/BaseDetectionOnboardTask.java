package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

public abstract class BaseDetectionOnboardTask implements DetectionOnboardTask {
  private final String taskName;
  protected DetectionOnboardTaskContext taskContext = new DetectionOnboardTaskContext();

  public BaseDetectionOnboardTask(String taskName) {
    Preconditions.checkNotNull(taskName);
    Preconditions.checkArgument(StringUtils.isNotBlank(taskName.trim()));
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
