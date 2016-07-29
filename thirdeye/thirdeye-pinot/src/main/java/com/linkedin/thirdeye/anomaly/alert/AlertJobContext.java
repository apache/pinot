package com.linkedin.thirdeye.anomaly.alert;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;

public class AlertJobContext extends JobContext {

  private Long alertConfigId;
  private DateTime windowStart;
  private DateTime windowEnd;
  private EmailConfiguration alertConfig;

  public Long getAlertConfigId() {
    return alertConfigId;
  }

  public void setAlertConfigId(Long alertConfigId) {
    this.alertConfigId = alertConfigId;
  }

  public EmailConfiguration getAlertConfig() {
    return alertConfig;
  }

  public void setAlertConfig(EmailConfiguration alertConfig) {
    this.alertConfig = alertConfig;
  }

  public DateTime getWindowStart() {
    return windowStart;
  }

  public void setWindowStart(DateTime windowStart) {
    this.windowStart = windowStart;
  }

  public DateTime getWindowEnd() {
    return windowEnd;
  }

  public void setWindowEnd(DateTime windowEnd) {
    this.windowEnd = windowEnd;
  }


}
