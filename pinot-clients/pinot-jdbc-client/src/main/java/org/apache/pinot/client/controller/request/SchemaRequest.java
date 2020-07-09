package org.apache.pinot.client.controller.request;

public class SchemaRequest {
  private String _table;

  public SchemaRequest(String table) {
    _table = table;
  }

  public String getTable() {
    return _table;
  }

  public void setTable(String table) {
    _table = table;
  }
}
