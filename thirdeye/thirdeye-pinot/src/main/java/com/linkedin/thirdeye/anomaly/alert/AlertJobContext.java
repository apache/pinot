package com.linkedin.thirdeye.anomaly.alert;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertJobContext extends JobContext {

  private Long alertConfigId;
  private EmailConfigurationDTO alertConfig;
  private AlertConfigDTO alertConfigDTO;

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

  public AlertConfigDTO getAlertConfigDTO() {
    return alertConfigDTO;
  }

  public void setAlertConfigDTO(AlertConfigDTO alertConfigDTO) {
    this.alertConfigDTO = alertConfigDTO;
  }
}
