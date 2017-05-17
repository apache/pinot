package com.linkedin.thirdeye.tools;

public class DrawTimeSeriesViewConfig {

  // File containing db details
  private String persistenceFile;
  // Anomaly detector host who will run adhoc function
  private String dashboardHost;
  private int dashboardPort;

  private String startTimeISO;
  private String endTimeISO;
  private boolean weekOverWeek;

  // function ids to cleanup/regenerate
  private String functionIds;
  private String outputPath;

  public String getPersistenceFile() {
    return persistenceFile;
  }
  public void setPersistenceFile(String persistenceFile) {
    this.persistenceFile = persistenceFile;
  }
  public String getDashboardHost() {
    return dashboardHost;
  }
  public void setDashboardHost(String detectorHost) {
    this.dashboardHost = detectorHost;
  }
  public int getDashboardPort() {
    return dashboardPort;
  }
  public void setDashboardPort(int detectorPort) {
    this.dashboardPort = detectorPort;
  }
  public String getFunctionIds() {
    return functionIds;
  }
  public void setFunctionIds(String functionIds) {
    this.functionIds = functionIds;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public String getStartTimeISO() {
    return startTimeISO;
  }

  public void setStartTimeISO(String startTimeISO) {
    this.startTimeISO = startTimeISO;
  }

  public String getEndTimeISO() {
    return endTimeISO;
  }

  public void setEndTimeISO(String endTimeISO) {
    this.endTimeISO = endTimeISO;
  }

  public boolean isWeekOverWeek() {
    return weekOverWeek;
  }

  public void setWeekOverWeek(boolean weekOverWeek) {
    this.weekOverWeek = weekOverWeek;
  }
}
