package com.linkedin.thirdeye.anomaly.alert;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

public class AlertJobContext extends JobContext {

  private Long alertConfigId;
  private DateTime windowStartTime;
  private DateTime windowEndTime;
  private EmailConfigurationDTO alertConfig;

  public Long getAlertConfigId() {
    return alertConfigId;
  }

  public void setAlertConfigId(Long alertConfigId) {
    this.alertConfigId = alertConfigId;
  }

  public EmailConfigurationDTO getAlertConfig() {
    return alertConfig;
  }

  public void setAlertConfig(EmailConfigurationDTO alertConfig) {
    this.alertConfig = alertConfig;
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


}
