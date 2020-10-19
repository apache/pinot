/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.pinot.thirdeye.datalayer.pojo;

import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskType;
import java.sql.Timestamp;
import java.util.Objects;


/**
 * This class corresponds to anomaly tasks. An execution of an anomaly function creates an anomaly
 * job, which in turn spawns into 1 or more anomaly tasks. The anomaly tasks are picked by the
 * workers
 */
public class TaskBean extends AbstractBean {

  private TaskType taskType;
  private Long workerId;
  private Long jobId;
  private String jobName;
  private TaskStatus status;
  private long startTime;
  private long endTime;
  // A JSON string of the task info such as anomaly function, monitoring windows, etc.
  private String taskInfo;
  // The task results, which could contain the error messages of tasks' execution.
  private String message;
  private Timestamp lastModified;

  public Long getWorkerId() {
    return workerId;
  }

  public void setWorkerId(Long workerId) {
    this.workerId = workerId;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getJobName() {
    return jobName;
  }

  public TaskStatus getStatus() {
    return status;
  }

  public void setStatus(TaskStatus status) {
    this.status = status;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getTaskInfo() {
    return taskInfo;
  }

  public void setTaskInfo(String taskInfo) {
    this.taskInfo = taskInfo;
  }

  public TaskType getTaskType() {
    return taskType;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public void setTaskType(TaskType taskType) {
    this.taskType = taskType;
  }

  public Timestamp getLastModified() {
    return lastModified;
  }

  public void setLastModified(Timestamp lastModified) {
    this.lastModified = lastModified;
  }

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(Long jobId) {
    this.jobId = jobId;
  }


  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TaskBean)) {
      return false;
    }
    TaskBean af = (TaskBean) o;
    return Objects.equals(getId(), af.getId()) && Objects.equals(status, af.getStatus())
        && Objects.equals(startTime, af.getStartTime()) && Objects.equals(endTime, af.getEndTime())
        && Objects.equals(taskInfo, af.getTaskInfo());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), status, startTime, endTime, taskInfo);
  }
}
