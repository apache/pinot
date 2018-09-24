package com.linkedin.thirdeye.tools;

public class AnomalyFunctionMigrationConfigs {

  // File containing db details
  private String persistenceFile;
  // Anomaly detector host who will run adhoc function
  private String detectorHost;
  private int detectorPort;
  // Start time and end time in ISO format for replay run
  private String startTimeIso;
  private String endTimeIso;

  // function ids to clone/migrate/replay
  private String functionIds;

  private String exportPath;

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

  public String getFunctionIds() {
    return functionIds;
  }

  public void setFunctionIds(String functionIds) {
    this.functionIds = functionIds;
  }

  public String getExportPath() {
    return exportPath;
  }

  public void setExportPath(String exportPath) {
    this.exportPath = exportPath;
  }
}
