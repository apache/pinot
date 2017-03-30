package com.linkedin.thirdeye.anomaly.classification;

import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import org.joda.time.DateTime;

public class ClassificationTaskInfo {
  private long jobexecutionId;
  private DateTime windowStartTime;
  private DateTime windowEndTime;
  private ClassificationConfigDTO classificationConfigDTO;

  public ClassificationTaskInfo() {
  }

  public ClassificationTaskInfo(long jobexecutionId, DateTime windowStartTime, DateTime windowEndTime,
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

  public DateTime getWindowStartTime() {
    return windowStartTime;
  }

  public void setWindowStartTime(DateTime windowStartTime) {
    this.windowStartTime = windowStartTime;
  }

  public DateTime getWindowEndTime() {
    return windowEndTime;
  }

  public void setWindowEndTime(DateTime windowEndTime) {
    this.windowEndTime = windowEndTime;
  }

  public ClassificationConfigDTO getClassificationConfigDTO() {
    return classificationConfigDTO;
  }

  public void setClassificationConfigDTO(ClassificationConfigDTO classificationConfigDTO) {
    this.classificationConfigDTO = classificationConfigDTO;
  }
}
