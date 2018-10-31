package com.linkedin.thirdeye.anomaly.classification;

import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;

public class ClassificationJobContext {
  private ClassificationConfigDTO configDTO;
  private long windowStartTime;
  private long windowEndTime;
  private String jobName;
  private long jobExecutionId;

  public ClassificationConfigDTO getConfigDTO() {
    return configDTO;
  }

  public void setConfigDTO(ClassificationConfigDTO configDTO) {
    this.configDTO = configDTO;
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

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }
}
