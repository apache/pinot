package com.linkedin.thirdeye.reporting.api;

import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.reporting.api.anomaly.AnomalyReportTable;

public class ReportEmailDataModel {

  private ReportConfig reportConfig;
  private List<Table> tables;
  private Map<String, AnomalyReportTable> anomalyReportTables;
  private List<TimeRange> missingSegments;
  private ScheduleSpec scheduleSpec;
  private ReportEmailCssSpec reportEmailCssSpec;

  public ReportEmailDataModel(ReportConfig reportConfig, List<Table> tables,
      Map<String, AnomalyReportTable> anomalyReportTables, List<TimeRange> missingSegments, ScheduleSpec scheduleSpec, ReportEmailCssSpec reportEmailCssSpec) {
    this.reportConfig = reportConfig;
    this.tables = tables;
    this.anomalyReportTables = anomalyReportTables;
    this.missingSegments = missingSegments;
    this.scheduleSpec = scheduleSpec;
    this.reportEmailCssSpec = reportEmailCssSpec;
  }



  public ReportEmailCssSpec getReportEmailCssSpec() {
    return reportEmailCssSpec;
  }



  public void setReportEmailCssSpec(ReportEmailCssSpec reportEmailCssSpec) {
    this.reportEmailCssSpec = reportEmailCssSpec;
  }



  public ReportConfig getReportConfig() {
    return reportConfig;
  }
  public void setReportConfig(ReportConfig reportConfig) {
    this.reportConfig = reportConfig;
  }
  public List<Table> getTables() {
    return tables;
  }
  public void setTables(List<Table> tables) {
    this.tables = tables;
  }
  public Map<String, AnomalyReportTable> getAnomalyReportTables() {
    return anomalyReportTables;
  }
  public void setAnomalyReportTables(Map<String, AnomalyReportTable> anomalyReportTables) {
    this.anomalyReportTables = anomalyReportTables;
  }
  public List<TimeRange> getMissingSegments() {
    return missingSegments;
  }
  public void setMissingSegments(List<TimeRange> missingSegments) {
    this.missingSegments = missingSegments;
  }
  public ScheduleSpec getScheduleSpec() {
    return scheduleSpec;
  }
  public void setScheduleSpec(ScheduleSpec scheduleSpec) {
    this.scheduleSpec = scheduleSpec;
  }


}
