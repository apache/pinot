/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.onboard.framework;

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
