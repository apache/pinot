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

import org.apache.pinot.thirdeye.anomaly.job.JobConstants.JobStatus;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants.TaskType;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * This class corresponds to an anomaly job. An anomaly job is created for every execution of an
 * anomaly function spec An anomaly job consists of 1 or more anomaly tasks
 */
public class JobBean extends AbstractBean {

  private String jobName;
  private JobStatus status;
  private TaskType taskType;
  private long scheduleStartTime;
  private long scheduleEndTime;
  private long windowStartTime;
  private long windowEndTime;
  private Timestamp lastModified;
  // The id of the job configuration, which could be a detection, alert, or classification job, that triggers
  // this job. If this job is not triggered from a job config (e.g., monitor job), then it is 0.
  // For example, this config id is the function id when this job is an anomaly detection.
  private long configId;

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

  public TaskType getTaskType() {
    return taskType;
  }

  public void setTaskType(TaskType taskType) {
    this.taskType = taskType;
  }

  public long getScheduleStartTime() {
    return scheduleStartTime;
  }

  public void setScheduleStartTime(long scheduleStartTime) {
    this.scheduleStartTime = scheduleStartTime;
  }

  public long getScheduleEndTime() {
    return scheduleEndTime;
  }

  public void setScheduleEndTime(long scheduleEndTime) {
    this.scheduleEndTime = scheduleEndTime;
  }

  public long getWindowStartTime() {
    return windowStartTime;
  }

  public void setWindowStartTime(long windowStartTime) {
    this.windowStartTime = windowStartTime;
  }

  public long getWindowEndTime() {
    return windowEndTime;
  }

  public void setWindowEndTime(long windowEndTime) {
    this.windowEndTime = windowEndTime;
  }

  public Timestamp getLastModified() {
    return lastModified;
  }

  public void setLastModified(Timestamp lastModified) {
    this.lastModified = lastModified;
  }

  public long getConfigId() {
    return configId;
  }

  public void setConfigId(long configId) {
    this.configId = configId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JobBean)) {
      return false;
    }
    JobBean af = (JobBean) o;
    return Objects.equals(getId(), af.getId()) && Objects.equals(jobName, af.getJobName()) && Objects
        .equals(status, af.getStatus()) && Objects.equals(scheduleStartTime, af.getScheduleStartTime())
        && Objects.equals(taskType, af.getTaskType()) && Objects.equals(configId, af.getConfigId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), jobName, status, scheduleStartTime, taskType, configId);
  }
}
