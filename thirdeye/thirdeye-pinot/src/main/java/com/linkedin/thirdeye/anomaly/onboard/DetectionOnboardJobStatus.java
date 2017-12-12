package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import java.util.ArrayList;
import java.util.List;

public class DetectionOnboardJobStatus {
  private JobConstants.JobStatus jobStatus;
  private List<DetectionOnboardTaskStatus> taskStatuses = new ArrayList<>();

  public JobConstants.JobStatus getJobStatus() {
    return jobStatus;
  }

  public void setJobStatus(JobConstants.JobStatus jobStatus) {
    this.jobStatus = jobStatus;
  }

  public void addTaskStatus(DetectionOnboardTaskStatus taskStatus) {
    Preconditions.checkNotNull(taskStatus);
    taskStatuses.add(taskStatus);
  }

  public List<DetectionOnboardTaskStatus> getTaskStatuses() {
    return ImmutableList.copyOf(taskStatuses);
  }
}
