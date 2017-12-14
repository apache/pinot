package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;

public class DetectionOnboardTaskStatus {
  private String taskName = "Unknown Task Name";
  private TaskConstants.TaskStatus taskStatus = TaskConstants.TaskStatus.WAITING;
  private String message = "";

  public DetectionOnboardTaskStatus() { }

  public DetectionOnboardTaskStatus(String taskName) {
    this.setTaskName(taskName);
  }

  public DetectionOnboardTaskStatus(String taskName, TaskConstants.TaskStatus taskStatus, String message) {
    this.setTaskName(taskName);
    this.setTaskStatus(taskStatus);
    this.setMessage(message);
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(taskName));
    this.taskName = taskName;
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
