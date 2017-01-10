package com.linkedin.thirdeye.anomaly.alert;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import java.util.Objects;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.util.CustomDateDeserializer;
import com.linkedin.thirdeye.util.CustomDateSerializer;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertTaskInfo implements TaskInfo {

  private long jobExecutionId;

  @JsonSerialize(using = CustomDateSerializer.class)
  @JsonDeserialize(using = CustomDateDeserializer.class)
  private DateTime windowStartTime;

  @JsonSerialize(using = CustomDateSerializer.class)
  @JsonDeserialize(using = CustomDateDeserializer.class)
  private DateTime windowEndTime;
  private EmailConfigurationDTO alertConfig;
  private AlertConfigDTO alertConfigDTO;

  public AlertTaskInfo(long jobExecutionId, DateTime windowStartTime,
      DateTime windowEndTime, EmailConfigurationDTO alertConfig, AlertConfigDTO alertConfigDTO) {
    this.jobExecutionId = jobExecutionId;
    this.windowStartTime = windowStartTime;
    this.windowEndTime = windowEndTime;
    this.alertConfig = alertConfig;
    this.alertConfigDTO = alertConfigDTO;
  }

  public AlertTaskInfo() {

  }

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
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

  public EmailConfigurationDTO getAlertConfig() {
    return alertConfig;
  }

  public void setAlertConfig(EmailConfigurationDTO alertConfig) {
    this.alertConfig = alertConfig;
  }

  public AlertConfigDTO getAlertConfigDTO() {
    return alertConfigDTO;
  }

  public void setAlertConfigDTO(AlertConfigDTO alertConfigDTO) {
    this.alertConfigDTO = alertConfigDTO;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AlertTaskInfo)) {
      return false;
    }
    AlertTaskInfo at = (AlertTaskInfo) o;
    return Objects.equals(jobExecutionId, at.getJobExecutionId())
        && Objects.equals(windowStartTime, at.getWindowStartTime())
        && Objects.equals(windowEndTime, at.getWindowEndTime())
        && Objects.equals(alertConfig, at.getAlertConfig());
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobExecutionId, windowStartTime, windowEndTime, alertConfig, alertConfigDTO);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("jobExecutionId", jobExecutionId)
        .add("windowStartTime", windowStartTime).add("windowEndTime", windowEndTime)
        .add("alertConfig", alertConfig).toString();
  }
}
