package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertConfigBean extends AbstractBean {
  String name;
  String cronExpression;
  boolean active;
  EmailConfig emailConfig;
  ReportConfigCollection reportConfigCollection;
  String recipients;
  String fromAddress;

  public String getCronExpression() {
    return cronExpression;
  }

  public String getFromAddress() {
    return fromAddress;
  }

  public void setFromAddress(String fromAddress) {
    this.fromAddress = fromAddress;
  }

  public String getRecipients() {
    return recipients;
  }

  public void setRecipients(String recipients) {
    this.recipients = recipients;
  }

  public void setCronExpression(String cronExpression) {
    this.cronExpression = cronExpression;
  }

  public EmailConfig getEmailConfig() {
    return emailConfig;
  }

  public void setEmailConfig(EmailConfig emailConfig) {
    this.emailConfig = emailConfig;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ReportConfigCollection getReportConfigCollection() {
    return reportConfigCollection;
  }

  public void setReportConfigCollection(ReportConfigCollection reportConfigCollection) {
    this.reportConfigCollection = reportConfigCollection;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class EmailConfig {
    long anomalyWatermark = 0l;
    List<Long> functionIds = new ArrayList<>();

    public List<Long> getFunctionIds() {
      return functionIds;
    }

    public void setFunctionIds(List<Long> functionIds) {
      this.functionIds = functionIds;
    }

    public long getAnomalyWatermark() {
      return anomalyWatermark;
    }

    public void setAnomalyWatermark(long lastNotifiedAnomalyId) {
      this.anomalyWatermark = lastNotifiedAnomalyId;
    }

    @Override
    public String toString() {
      return "EmailConfig{" +
          "functionIds=" + functionIds +
          ", anomalyWatermark=" + anomalyWatermark +
          '}';
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ReportMetricConfig {
    COMPARE_MODE compareMode = COMPARE_MODE.Wo2W;
    Long metricId;
    List<String> dimensions = new ArrayList<>();

    public COMPARE_MODE getCompareMode() {
      return compareMode;
    }

    public void setCompareMode(COMPARE_MODE compareMode) {
      this.compareMode = compareMode;
    }

    public List<String> getDimensions() {
      return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
      this.dimensions = dimensions;
    }

    public Long getMetricId() {
      return metricId;
    }

    public void setMetricId(Long metricId) {
      this.metricId = metricId;
    }

  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ReportConfigCollection {
    boolean enabled;
    boolean intraDay;
    long delayOffsetMillis = 2 * 36_00_000; // 2 hours
    List<ReportMetricConfig> reportMetricConfigs = new ArrayList<>();
    String contactEmail;

    public boolean isIntraDay() {
      return intraDay;
    }

    public void setIntraDay(boolean intraDay) {
      this.intraDay = intraDay;
    }

    public long getDelayOffsetMillis() {
      return delayOffsetMillis;
    }

    public void setDelayOffsetMillis(long delayOffsetMillis) {
      this.delayOffsetMillis = delayOffsetMillis;
    }

    public boolean isEnabled() {
      return enabled;
    }
    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public String getContactEmail() {
      return contactEmail;
    }

    public List<ReportMetricConfig> getReportMetricConfigs() {
      return reportMetricConfigs;
    }

    public void setReportMetricConfigs(List<ReportMetricConfig> reportMetricConfigs) {
      this.reportMetricConfigs = reportMetricConfigs;
    }

    public void setContactEmail(String contactEmail) {
      this.contactEmail = contactEmail;
    }

    @Override public String toString() {
      return "ReportConfigCollection{" +
          "contactEmail='" + contactEmail + '\'' +
          ", enabled=" + enabled +
          ", reportMetricConfigs=" + reportMetricConfigs +
          '}';
    }
  }

  public enum COMPARE_MODE {
    WoW, Wo2W, Wo3W
  }

  @Override
  public String toString() {
    return "AlertConfigBean{" +
        "active=" + active +
        ", name='" + name + '\'' +
        ", cronExpression='" + cronExpression + '\'' +
        ", emailConfig=" + emailConfig +
        ", reportConfigCollection=" + reportConfigCollection +
        ", recipients='" + recipients + '\'' +
        ", fromAddress='" + fromAddress + '\'' +
        '}';
  }

}
