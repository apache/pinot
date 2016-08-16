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
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;


/**
 * The <code>TimeGranularitySpec</code> class contains all specs related to time field.
 * <p>- <code>DataType</code>: data type of the time column (e.g. INT, LONG).
 * <p>- <code>TimeType</code>: time unit of the time column (e.g. MINUTES, HOURS).
 * <p>- <code>TimeUnitSize</code>: size of the time buckets (e.g. 10 MINUTES, 2 HOURS). By default this is set to 1.
 * <p>- <code>Name</code>: name of the time column.
 * <p>E.g.
 * <p>If the time column is in millisecondsSinceEpoch, constructor can be invoked as:
 * <p><code>TimeGranularitySpec(LONG, MILLISECONDS, timeColumnName)</code>
 * <p>If the time column is in tenMinutesSinceEpoch, constructor can be invoked as:
 * <p><code>TimeGranularitySpec(LONG, 10, MINUTES, timeColumnName)</code>
 */
public class TimeGranularitySpec {
  private static final int DEFAULT_TIME_UNIT_SIZE = 1;

  private DataType _dataType;
  private TimeUnit _timeType;
  private int _timeUnitSize = DEFAULT_TIME_UNIT_SIZE;
  private String _name;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public TimeGranularitySpec() {
  }

  public TimeGranularitySpec(DataType dataType, TimeUnit timeType, String name) {
    Preconditions.checkNotNull(dataType);
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);

    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _name = name;
  }

  public TimeGranularitySpec(DataType dataType, int timeUnitSize, TimeUnit timeType, String name) {
    Preconditions.checkNotNull(dataType);
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);

    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _timeUnitSize = timeUnitSize;
    _name = name;
  }

  public void setDataType(DataType dataType) {
    Preconditions.checkNotNull(dataType);

    _dataType = dataType.getStoredType();
  }

  public DataType getDataType() {
    return _dataType;
  }

  public void setTimeType(TimeUnit timeType) {
    Preconditions.checkNotNull(timeType);

    _timeType = timeType;
  }

  public TimeUnit getTimeType() {
    return _timeType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setTimeUnitSize(int timeUnitSize) {
    _timeUnitSize = timeUnitSize;
  }

  // Required by JSON de-serializer (for backward compatible). DO NOT REMOVE.
  public void setTimeunitSize(int timeUnitSize) {
    _timeUnitSize = timeUnitSize;
  }

  public int getTimeUnitSize() {
    return _timeUnitSize;
  }

  public void setName(String name) {
    Preconditions.checkNotNull(name);

    _name = name;
  }

  public String getName() {
    return _name;
  }

  /**
   * Convert the units of time since epoch to {@link DateTime} format using current <code>TimeGranularitySpec</code>.
   */
  public DateTime toDateTime(long timeSinceEpoch) {
    return new DateTime(_timeType.toMillis(timeSinceEpoch * _timeUnitSize));
  }

  @Override
  public String toString() {
    return "< data type: " + _dataType + ", time type: " + _timeType + ", time unit size: " + _timeUnitSize
        + ", name: " + _name + " >";
  }

  @Override
  public boolean equals(Object anObject) {
    if (this == anObject) {
      return true;
    }
    if (anObject instanceof TimeGranularitySpec) {
      TimeGranularitySpec anotherTimeGranularitySpec = (TimeGranularitySpec) anObject;
      return _dataType.equals(anotherTimeGranularitySpec._dataType)
          && _timeType.equals(anotherTimeGranularitySpec._timeType)
          && _timeUnitSize == anotherTimeGranularitySpec._timeUnitSize
          && _name.equals(anotherTimeGranularitySpec._name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = _dataType.hashCode();
    result = EqualityUtils.hashCodeOf(result, _timeType);
    result = EqualityUtils.hashCodeOf(result, _timeUnitSize);
    result = EqualityUtils.hashCodeOf(result, _name);
    return result;
  }
}
