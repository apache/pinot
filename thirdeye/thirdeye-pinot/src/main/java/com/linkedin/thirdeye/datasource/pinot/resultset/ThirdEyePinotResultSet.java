package com.linkedin.thirdeye.datasource.pinot.resultset;

public interface ThirdEyePinotResultSet {
  int getRowCount();

  int getColumnCount();

  String getColumnName(int columnIdx);

  long getLong(int rowIdx);

  double getDouble(int rowIdx);

  String getString(int rowIdx);

  long getLong(int rowIdx, int columnIdx);

  double getDouble(int rowIdx, int columnIdx);

  String getString(int rowIdx, int columnIdx);

  int getGroupKeyLength();

  String getGroupKeyColumnName(int columnIdx);

  String getGroupKeyColumnValue(int rowIdx, int columnIdx);
}
