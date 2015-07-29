package com.linkedin.thirdeye.reporting.api;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class Table {
  Map<String, List<ReportRow>> metricReportRows;
  TableSpec tableSpec;
  List<GroupBy> groupBy;
  URL thirdeyeURI;

  public Table(Map<String, List<ReportRow>> metricReportRows, TableSpec tableSpec, List<GroupBy> groupBy, URL thirdeyeUri) {
    this.metricReportRows = metricReportRows;
    this.tableSpec = tableSpec;
    this.groupBy = groupBy;
    this.thirdeyeURI = thirdeyeUri;
  }



  public List<GroupBy> getGroupBy() {
    return groupBy;
  }



  public void setGroupBy(List<GroupBy> groupBy) {
    this.groupBy = groupBy;
  }



  public Map<String, List<ReportRow>> getMetricReportRows() {
    return metricReportRows;
  }

  public void setMetricReportRows(Map<String, List<ReportRow>> metricReportRows) {
    this.metricReportRows = metricReportRows;
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }

  public void setTableSpec(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }



  public URL getThirdeyeURI() {
    return thirdeyeURI;
  }



  public void setThirdeyeURI(URL thirdeyeURI) {
    this.thirdeyeURI = thirdeyeURI;
  }


}
