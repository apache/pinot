package com.linkedin.thirdeye.reporting.api;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class Table {
  TableSpec tableSpec;
  List<TableReportRow> tableReportRows;
  URL thirdeyeURI;

  public Table(List<TableReportRow> tableReportRows, TableSpec tableSpec, URL thirdeyeUri) {
    this.tableReportRows = tableReportRows;
    this.tableSpec = tableSpec;
    this.thirdeyeURI = thirdeyeUri;
  }



  public List<TableReportRow> getTableReportRows() {
    return tableReportRows;
  }



  public void setTableReportRows(List<TableReportRow> tableReportRows) {
    this.tableReportRows = tableReportRows;
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
