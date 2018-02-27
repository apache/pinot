package com.linkedin.thirdeye.datasource.pinot.resultset;

public abstract class AbstractThirdEyeResultSet implements ThirdEyeResultSet {

  public long getLong(int rowIndex) {
    return this.getLong(rowIndex, 0);
  }

  public double getDouble(int rowIndex) {
    return this.getDouble(rowIndex, 0);
  }

  public String getString(int rowIndex) {
    return this.getString(rowIndex, 0);
  }

  public long getLong(int rowIndex, int columnIndex) {
    return Long.parseLong(this.getString(rowIndex, columnIndex));
  }

  public double getDouble(int rowIndex, int columnIndex) {
    return Double.parseDouble(this.getString(rowIndex, columnIndex));
  }
}
