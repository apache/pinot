package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import java.util.concurrent.Callable;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetectionOnboardTaskRunner implements Callable<DetectionOnboardTaskStatus> {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionOnboardTaskRunner.class);

  private final DetectionOnboardTask task;

  public DetectionOnboardTaskRunner(DetectionOnboardTask task) {
    Preconditions.checkNotNull(task);
    this.task = task;
  }

  @Override
  public DetectionOnboardTaskStatus call() throws Exception {
    DetectionOnboardTaskStatus taskStatus = new DetectionOnboardTaskStatus(task.getTaskName());
    taskStatus.setTaskStatus(TaskConstants.TaskStatus.RUNNING);

    try {
      task.run();
      taskStatus.setTaskStatus(TaskConstants.TaskStatus.COMPLETED);
    } catch (Exception e) {
      taskStatus.setTaskStatus(TaskConstants.TaskStatus.FAILED);
      taskStatus.setMessage(ExceptionUtils.getStackTrace(e));
      LOG.error("Error encountered when running task: {}", task.getTaskName(), e);
    }

    return taskStatus;
  }
}
