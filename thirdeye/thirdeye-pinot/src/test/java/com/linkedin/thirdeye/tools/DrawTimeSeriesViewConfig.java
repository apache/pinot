package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.tools.DrawTimeSeriesView.ComparisonMode;

public class DrawTimeSeriesViewConfig {

  // File containing db details
  private String persistenceFile;
  // Anomaly detector host who will run adhoc function
  private String dashboardHost;
  private int dashboardPort;

  private String startTimeISO;
  private String endTimeISO;
  private ComparisonMode comparisonMode;

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

  public ComparisonMode getComparisonMode() {
    return comparisonMode;
  }

  public void setComparisonMode(ComparisonMode comparisonMode) {
    this.comparisonMode = comparisonMode;
  }
}
