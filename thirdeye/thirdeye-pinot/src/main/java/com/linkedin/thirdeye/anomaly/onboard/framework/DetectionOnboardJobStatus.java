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
import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import java.util.ArrayList;
import java.util.List;

public class DetectionOnboardJobStatus {
  private long jobId = -1;
  private String jobName = "Unknown Job Name";
  private JobConstants.JobStatus jobStatus = JobConstants.JobStatus.SCHEDULED;
  private String message = "";
  private List<DetectionOnboardTaskStatus> taskStatuses = new ArrayList<>();

  public DetectionOnboardJobStatus() { }

  public DetectionOnboardJobStatus(long jobId, String jobName) {
    this.setJobId(jobId);
    this.setJobName(jobName);
  }

  public DetectionOnboardJobStatus(long jobId, String jobName, JobConstants.JobStatus jobStatus, String message) {
    this.setJobId(jobId);
    this.setJobName(jobName);
    this.setJobStatus(jobStatus);
    this.setMessage(message);
  }

  public long getJobId() {
    return jobId;
  }

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(jobName));
    this.jobName = jobName;
  }

  public JobConstants.JobStatus getJobStatus() {
    return jobStatus;
  }

  public void setJobStatus(JobConstants.JobStatus jobStatus) {
    Preconditions.checkNotNull(jobStatus);
    this.jobStatus = jobStatus;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    Preconditions.checkNotNull(message);
    this.message = message;
  }

  public void addTaskStatus(DetectionOnboardTaskStatus taskStatus) {
    Preconditions.checkNotNull(taskStatus);
    taskStatuses.add(taskStatus);
  }

  public List<DetectionOnboardTaskStatus> getTaskStatuses() {
    return ImmutableList.copyOf(taskStatuses);
  }
}
