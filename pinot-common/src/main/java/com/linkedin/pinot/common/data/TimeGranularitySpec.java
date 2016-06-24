/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.linkedin.pinot.common.data.FieldSpec.DataType;

/**
 * TimeGranularitySpec contains all spec related to timeField
 * name is the name of the time column in the schema
 * dataType is the data type of the time column (eg. LONG, INT)
 * timeType is the TimeUnit of the time column (eg. HOURS, MINUTES)
 * timeunitSize is the size of the time buckets (eg. 10 MINUTES, 2 HOURS). By default this will be
 * set to 1.
 * eg. If the time column is in milliseconds, constructor can be invoked as
 * TimeGranularitySpec(LONG, MILLISECONDS, timeColumnName)
 * If the time column is aggregated in tenMinutesSinceEpoch, constructor can be invoked as
 * TimeGranularitySpec(LONG, 10, MINUTES, timeColumnName)
 */
public class TimeGranularitySpec {

  private static final int DEFAULT_TIME_SIZE = 1;

  private DataType _dataType;
  private TimeUnit _timeType;
  private int _timeunitSize = DEFAULT_TIME_SIZE;
  private String _name;

  // Default constructor required by JSON de-serielizer.
  public TimeGranularitySpec() {
  }

  public TimeGranularitySpec(DataType dataType, TimeUnit timeType, String name) {
    _dataType = dataType;
    _timeType = timeType;
    _name = name;
  }

  public TimeGranularitySpec(DataType dataType, int timeunitSize, TimeUnit timeType, String name) {
    _dataType = dataType;
    _timeType = timeType;
    _timeunitSize = timeunitSize;
    _name = name;
  }

  public void setDataType(DataType dataType) {
    // Data type should not be null.
    Preconditions.checkNotNull(dataType);

    _dataType = dataType;
  }

  public DataType getDataType() {
    return _dataType;
  }

  public void setTimeType(TimeUnit timeType) {
    // Time type should not be null.
    Preconditions.checkNotNull(timeType);

    _timeType = timeType;
  }

  public TimeUnit getTimeType() {
    return _timeType;
  }

  public void setTimeunitSize(int timeunitSize) {
    _timeunitSize = timeunitSize;
  }

  public int getTimeunitSize() {
    return _timeunitSize;
  }

  public void setName(String name) {
    // Name should not be null.
    Preconditions.checkNotNull(name);

    _name = name;
  }

  public String getName() {
    return _name;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof TimeGranularitySpec)) {
      return false;
    }

    TimeGranularitySpec spec = (TimeGranularitySpec) object;
    if (this.getName().equals(spec.getName()) && spec.getDataType() == this.getDataType()
        && this.getTimeType() == spec.getTimeType() && this.getTimeunitSize() == spec.getTimeunitSize()) {
      return true;
    }

    return false;
  }

  /**
   * converts the timeSinceEpoch to DateTime
   * @param timeSinceEpoch assumes this confirms to granularity spec
   * @return
   */
  public DateTime toDateTime(long timeSinceEpoch) {
    return new DateTime(_timeType.toMillis(timeSinceEpoch * _timeunitSize));
  }

  @Override
  public int hashCode() {
    int result = _dataType != null ? _dataType.hashCode() : 0;
    result = 31 * result + (_timeType != null ? _timeType.hashCode() : 0);
    result = 31 * result + Integer.valueOf(_timeunitSize).hashCode();
    result = 31 * result + (_name != null ? _name.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "< data type: " + getDataType() + ", time type: " + getTimeType() + ", time unit size: " + getTimeunitSize()
        + ", name: " + getName() + " >";
  }
}
