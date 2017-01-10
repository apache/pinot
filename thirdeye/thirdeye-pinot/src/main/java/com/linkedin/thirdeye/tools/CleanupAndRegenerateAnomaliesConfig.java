package com.linkedin.thirdeye.tools;


public class CleanupAndRegenerateAnomaliesConfig {

  // File containing db details
  private String persistenceFile;
  // Anomaly detector host who will run adhoc function
  private String detectorHost;
  private int detectorPort;
  // Start time and end time in ISO format for adhoc run
  private String startTimeIso;
  private String endTimeIso;

  // function ids to cleanup/regenerate
  private String functionIds;
  // datasets to cleanup/regenerate if functionIds not provided.
  // will be ignored if functionIds provided
  private String datasets;
  // forceBackfill previous backfill job if there exists any
  private String forceBackfill;

  public String getPersistenceFile() {
    return persistenceFile;
  }
  public void setPersistenceFile(String persistenceFile) {
    this.persistenceFile = persistenceFile;
  }
  public String getDetectorHost() {
    return detectorHost;
  }
  public void setDetectorHost(String detectorHost) {
    this.detectorHost = detectorHost;
  }
  public int getDetectorPort() {
    return detectorPort;
  }
  public void setDetectorPort(int detectorPort) {
    this.detectorPort = detectorPort;
  }
  public String getStartTimeIso() {
    return startTimeIso;
  }
  public void setStartTimeIso(String startTimeIso) {
    this.startTimeIso = startTimeIso;
  }
  public String getEndTimeIso() {
    return endTimeIso;
  }
  public void setEndTimeIso(String endTimeIso) {
    this.endTimeIso = endTimeIso;
  }
  public String getDatasets() {
    return datasets;
  }
  public void setDatasets(String datasets) {
    this.datasets = datasets;
  }
  public String getFunctionIds() {
    return functionIds;
  }
  public void setFunctionIds(String functionIds) {
    this.functionIds = functionIds;
  }
  public String getForceBackfill() {
    return forceBackfill;
  }
  public void setForceBackfill(String forceBackfill) {
    this.forceBackfill = forceBackfill;
  }

}
