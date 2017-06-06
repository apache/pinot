package com.linkedin.thirdeye.anomaly.classification;

import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;

public class ClassificationTaskInfo implements TaskInfo {
  private long jobexecutionId;
  private long windowStartTime;
  private long windowEndTime;
  private ClassificationConfigDTO classificationConfigDTO;

  public ClassificationTaskInfo() {
  }

  public ClassificationTaskInfo(long jobexecutionId, long windowStartTime, long windowEndTime,
      ClassificationConfigDTO classificationConfigDTO) {
    this.jobexecutionId = jobexecutionId;
    this.windowStartTime = windowStartTime;
    this.windowEndTime = windowEndTime;
    this.classificationConfigDTO = classificationConfigDTO;
  }

  public long getJobexecutionId() {
    return jobexecutionId;
  }

  public void setJobexecutionId(long jobexecutionId) {
    this.jobexecutionId = jobexecutionId;
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

  public ClassificationConfigDTO getClassificationConfigDTO() {
    return classificationConfigDTO;
  }

  public void setClassificationConfigDTO(ClassificationConfigDTO classificationConfigDTO) {
    this.classificationConfigDTO = classificationConfigDTO;
  }
}
