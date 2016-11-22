package com.linkedin.thirdeye.tools.anomaly.report;

public class AnomalyReportConfig {
  private String startTimeIso;
  private String endTimeIso;
  private String persistenceConfigPath;
  private String datasets;
  private String teBaseUrl;

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

  public String getPersistenceConfigPath() {
    return persistenceConfigPath;
  }

  public void setPersistenceConfigPath(String persistenceConfigPath) {
    this.persistenceConfigPath = persistenceConfigPath;
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
}
