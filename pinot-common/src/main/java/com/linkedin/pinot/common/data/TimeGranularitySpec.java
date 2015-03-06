package com.linkedin.pinot.common.data;

import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.data.FieldSpec.DataType;

public class TimeGranularitySpec {

  DataType dType;
  TimeUnit timeType;
  String columnName;

  public TimeGranularitySpec(DataType dataType, TimeUnit timeType, String columnName) {
    this.dType = dataType;
    this.timeType = timeType;
    this.columnName = columnName;
  }

  public DataType getDataType() {
    return dType;
  }

  public void setDataType(DataType dType) {
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

  public boolean equals(Object object) {
    if (!(object instanceof TimeGranularitySpec))
      return false;

    TimeGranularitySpec spec = (TimeGranularitySpec) object;
    if (this.getColumnName().equals(spec.getColumnName()) && spec.getDataType() == this.getDataType() && this.getTimeType() == spec.getTimeType())
      return true;

    return false;
  }
}
