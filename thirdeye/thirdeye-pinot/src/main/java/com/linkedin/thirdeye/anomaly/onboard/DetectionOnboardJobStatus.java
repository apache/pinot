package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
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
