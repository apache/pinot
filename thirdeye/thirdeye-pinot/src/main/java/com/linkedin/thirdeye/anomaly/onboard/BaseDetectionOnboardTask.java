package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseDetectionOnboardTask implements DetectionOnboardTask {
  private static final Logger LOG = LoggerFactory.getLogger(BaseDetectionOnboardTask.class);

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
      taskStatus.setTaskStatus(TaskConstants.TaskStatus.COMPLETED);
    } catch (Exception e) {
      taskStatus.setTaskStatus(TaskConstants.TaskStatus.FAILED);
      taskStatus.setMessage(ExceptionUtils.getStackTrace(e));
      LOG.error("Error encountered when running task: {}", taskName, e);
    }

    return taskStatus;
  }
}
