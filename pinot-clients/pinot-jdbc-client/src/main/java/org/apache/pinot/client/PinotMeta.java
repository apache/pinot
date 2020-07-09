package org.apache.pinot.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PinotMeta {

  private List<List<Object>> _rows;
  private Map<String, List<String>> _dataSchema = new HashMap<>();

  public PinotMeta() {

  }

  public PinotMeta(List<List<Object>> rows, Map<String, List<String>> dataSchema) {
    _rows = rows;
    _dataSchema = dataSchema;
  }

  public List<List<Object>> getRows() {
    return _rows;
  }

  public void setRows(List<List<Object>> rows) {
    _rows = rows;
  }

  public void addRow(List<Object> row) {
    if (_rows == null) {
      _rows = new ArrayList<>();
    }
    _rows.add(row);
  }

  public Map<String, List<String>> getDataSchema() {
    return _dataSchema;
  }

  public void setDataSchema(Map<String, List<String>> dataSchema) {
    _dataSchema = dataSchema;
  }

  public void setColumnNames(String[] columnNames) {
    _dataSchema.put("columnNames", Arrays.asList(columnNames));
  }

  public void setColumnDataTypes(String[] columnDataTypes) {
    _dataSchema.put("columnDataTypes", Arrays.asList(columnDataTypes));
  }
}
