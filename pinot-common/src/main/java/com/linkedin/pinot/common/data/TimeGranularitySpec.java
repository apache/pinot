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

  private static int DEFAULT_TIME_SIZE = 1;

  DataType dataType;
  TimeUnit timeType;
  int size = DEFAULT_TIME_SIZE;
  String name;

  // Default constructor required by JSON de-serielizer.
  public TimeGranularitySpec() {
  }

  public TimeGranularitySpec(DataType dataType, TimeUnit timeType, String name) {
    this.dataType = dataType;
    this.timeType = timeType;
    this.name = name;
  }

  public TimeGranularitySpec(DataType dataType, TimeGranularity timeGranularity, String name) {
    this.dataType = dataType;
    this.timeType = timeGranularity.getTimeUnit();
    this.size = timeGranularity.getSize();
    this.name = name;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dType) {
    this.dataType = dType;
  }

  public TimeUnit getTimeType() {
    return timeType;
  }

  public void setTimeType(TimeUnit timeType) {
    this.timeType = timeType;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public String getName() {
    return name;
  }

  public void setName(String columnName) {
    this.name = columnName;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof TimeGranularitySpec)) {
      return false;
    }

    TimeGranularitySpec spec = (TimeGranularitySpec) object;
    if (this.getName().equals(spec.getName()) && spec.getDataType() == this.getDataType()
        && this.getTimeType() == spec.getTimeType() && this.getSize() == spec.getSize()) {
      return true;
    }

    return false;
  }

  @Override
  public int hashCode() {
    int result = dataType != null ? dataType.hashCode() : 0;
    result = 31 * result + (timeType != null ? timeType.hashCode() : 0);
    result = 31 * result +  Integer.valueOf(size).hashCode();
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
