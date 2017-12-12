package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;

public class DetectionOnboardTaskStatus {
  private TaskConstants.TaskStatus taskStatus = TaskConstants.TaskStatus.WAITING;
  private String message = "";

  public DetectionOnboardTaskStatus() {
  }

  public DetectionOnboardTaskStatus(TaskConstants.TaskStatus taskStatus, String message) {
    this.setTaskStatus(taskStatus);
    this.setMessage(message);
  }

  public TaskConstants.TaskStatus getTaskStatus() {
    return taskStatus;
  }

  public void setTaskStatus(TaskConstants.TaskStatus taskStatus) {
    Preconditions.checkNotNull(taskStatus);
    this.taskStatus = taskStatus;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    Preconditions.checkNotNull(message);
    this.message = message.trim();
  }
}
