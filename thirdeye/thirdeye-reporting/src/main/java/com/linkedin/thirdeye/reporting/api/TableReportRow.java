package com.linkedin.thirdeye.reporting.api;

import java.util.ArrayList;
import java.util.List;

public class TableReportRow {
  private GroupBy groupByDimensions;
  private List<ReportRow> rows;

  public TableReportRow() {
    rows = new ArrayList<ReportRow>();
  }


  public TableReportRow(GroupBy groupByDimensions, List<ReportRow> rows) {
    this.groupByDimensions = groupByDimensions;
    this.rows = rows;
  }


  public GroupBy getGroupByDimensions() {
    return groupByDimensions;
  }
  public void setGroupByDimensions(GroupBy groupByDimensions) {
    this.groupByDimensions = groupByDimensions;
  }
  public List<ReportRow> getRows() {
    return rows;
  }
  public void setRows(List<ReportRow> rows) {
    this.rows = rows;
  }


}
