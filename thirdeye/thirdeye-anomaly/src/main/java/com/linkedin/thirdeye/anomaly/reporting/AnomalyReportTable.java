package com.linkedin.thirdeye.anomaly.reporting;

import java.util.List;

/**
 *
 */
public class AnomalyReportTable {

  String dimensionSchema;

  int topLevelViolationCount;
  int totalViolationCount;

  List<AnomalyReportTableRow> reportRows;

  public String getDimensionSchema() {
    return dimensionSchema;
  }

  public void setDimensionSchema(String dimensionSchema) {
    this.dimensionSchema = dimensionSchema;
  }

  public List<AnomalyReportTableRow> getReportRows() {
    return reportRows;
  }

  public void setReportRows(List<AnomalyReportTableRow> reportRows) {
    this.reportRows = reportRows;
  }

  public int getTopLevelViolationCount() {
    return topLevelViolationCount;
  }

  public void setTopLevelViolationCount(int topLevelViolationCount) {
    this.topLevelViolationCount = topLevelViolationCount;
  }

  public int getTotalViolationCount() {
    return totalViolationCount;
  }

  public void setTotalViolationCount(int totalViolationCount) {
    this.totalViolationCount = totalViolationCount;
  }

  @Override
  public String toString() {
    return "AnomalyReportTable [dimensionSchema=" + dimensionSchema + ", topLevelViolationCount="
        + topLevelViolationCount + ", totalViolationCount=" + totalViolationCount + ", reportRows=" + reportRows + "]";
  }

}
