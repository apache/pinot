package com.linkedin.thirdeye.anomaly.alert;


import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

public class AlertJobContext extends JobContext {

  private Long alertConfigId;
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
}
