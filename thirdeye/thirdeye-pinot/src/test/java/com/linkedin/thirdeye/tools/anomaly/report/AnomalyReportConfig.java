package com.linkedin.thirdeye.tools.anomaly.report;

public class AnomalyReportConfig {
  private String startTimeIso;
  private String endTimeIso;
  private String thirdEyeConfigDirectoryPath;
  private String datasets;
  private String teBaseUrl;
  private String emailRecipients;
  private boolean includeNotifiedOnly = true;

  public String getEndTimeIso() {
    return endTimeIso;
  }

  public String getTeBaseUrl() {
    return teBaseUrl;
  }

  public void setTeBaseUrl(String teBaseUrl) {
    this.teBaseUrl = teBaseUrl;
  }

  public void setEndTimeIso(String endTimeIso) {
    this.endTimeIso = endTimeIso;
  }

  public String getStartTimeIso() {
    return startTimeIso;
  }

  public void setStartTimeIso(String startTimeIso) {
    this.startTimeIso = startTimeIso;
  }

  public String getDatasets() {
    return datasets;
  }

  public void setDatasets(String datasets) {
    this.datasets = datasets;
  }

  public String getThirdEyeConfigDirectoryPath() {
    return thirdEyeConfigDirectoryPath;
  }

  public void setThirdEyeConfigDirectoryPath(String thirdEyeConfigDirectoryPath) {
    this.thirdEyeConfigDirectoryPath = thirdEyeConfigDirectoryPath;
  }

  public String getEmailRecipients() {
    return emailRecipients;
  }

  public void setEmailRecipients(String emailRecipients) {
    this.emailRecipients = emailRecipients;
  }

  public boolean isIncludeNotifiedOnly() {
    return includeNotifiedOnly;
  }

  public void setIncludeNotifiedOnly(boolean includeNotifiedOnly) {
    this.includeNotifiedOnly = includeNotifiedOnly;
  }
}
