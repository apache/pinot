package com.linkedin.pinot.common.data;

import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.data.FieldSpec.DataType;

public class GranularitySpec {

  DataType dType;
  TimeUnit timeType;
  String columnName;

  public GranularitySpec(DataType dataType, TimeUnit timeType, String columnName) {
    this.dType = dataType;
    this.timeType = timeType;
    this.columnName = columnName;
  }

  public DataType getdType() {
    return dType;
  }

  public void setdType(DataType dType) {
    this.dType = dType;
  }

  public TimeUnit getTimeType() {
    return timeType;
  }

  public void setTimeType(TimeUnit timeType) {
    this.timeType = timeType;
  }

  protected String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public boolean equals(GranularitySpec spec) {
    if (this.getColumnName() == spec.getColumnName() && spec.getdType() == this.getdType() && this.getTimeType() == spec.getTimeType())
      return true;

    return false;
  }
}
