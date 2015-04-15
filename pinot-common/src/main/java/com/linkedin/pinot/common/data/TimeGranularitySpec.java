/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

  @Override
  public int hashCode() {
    int result = dType != null ? dType.hashCode() : 0;
    result = 31 * result + (timeType != null ? timeType.hashCode() : 0);
    result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
    return result;
  }
}
