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
import javax.annotation.Nonnull;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.joda.time.DateTime;


/**
 * The <code>TimeGranularitySpec</code> class contains all specs related to time field.
 * <p>- <code>DataType</code>: data type of the time column (e.g. INT, LONG).
 * <p>- <code>TimeType</code>: time unit of the time column (e.g. MINUTES, HOURS).
 * <p>- <code>TimeUnitSize</code>: size of the time buckets (e.g. 10 MINUTES, 2 HOURS). By default this is set to 1.
 * <p>- <code>TimeFormat</code>: Can be either EPOCH (default) or SIMPLE_DATE_FORMAT:pattern e.g SIMPLE_DATE_FORMAT:yyyyMMdd
 * <p>- <code>Name</code>: name of the time column.
 * <p>E.g.
 * <p>If the time column is in millisecondsSinceEpoch, constructor can be invoked as:
 * <p><code>TimeGranularitySpec(LONG, MILLISECONDS, timeColumnName)</code>
 * <p>If the time column is in tenMinutesSinceEpoch, constructor can be invoked as:
 * <p><code>TimeGranularitySpec(LONG, 10, MINUTES, timeColumnName)</code>
 * <p>If the time column is in Simple Date Format:
 * <p><code>new TimeGranularitySpec(DataType.STRING, 1, TimeUnit.HOURS, TimeFormat.SIMPLE_DATE_FORMAT.toString() +":yyyyMMdd", "hour");</code>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeGranularitySpec {
  private static final int DEFAULT_TIME_UNIT_SIZE = 1;

  private DataType _dataType;
  private TimeUnit _timeType;
  private int _timeUnitSize = DEFAULT_TIME_UNIT_SIZE;
  private String _timeFormat = TimeFormat.EPOCH.toString();
  private String _name;
  /*
   * Can be either EPOCH (default) or SIMPLE_DATE_FORMAT:pattern e.g SIMPLE_DATE_FORMAT:yyyyMMdd
   */
  public enum TimeFormat {
    EPOCH, //default
    SIMPLE_DATE_FORMAT
  }
  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public TimeGranularitySpec() {
  }

  /**
   *
   * @param dataType
   * @param timeType
   * @param name
   */
  public TimeGranularitySpec(@Nonnull DataType dataType, @Nonnull TimeUnit timeType, @Nonnull String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);

    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _name = name;
  }
  /**
   *
   * @param dataType
   * @param timeType
   * @param timeFormat Can be either EPOCH (default) or SIMPLE_DATE_FORMAT:pattern e.g SIMPLE_DATE_FORMAT:yyyyMMdd
   * @param name
   */
  public TimeGranularitySpec(@Nonnull DataType dataType, @Nonnull TimeUnit timeType, @Nonnull String timeFormat,
      @Nonnull String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(timeFormat);
    Preconditions.checkArgument(timeFormat.equals(TimeFormat.EPOCH.toString())
        || (timeFormat.startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())));
    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _name = name;
    _timeFormat = timeFormat;
  }
  /**
   *
   * @param dataType
   * @param timeUnitSize
   * @param timeType
   * @param name
   */
  public TimeGranularitySpec(@Nonnull DataType dataType, int timeUnitSize, @Nonnull TimeUnit timeType,
      @Nonnull String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);

    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _timeUnitSize = timeUnitSize;
    _name = name;
  }

  /**
   *
   * @param dataType
   * @param timeUnitSize
   * @param timeType
   * @param timeFormat Can be either EPOCH (default) or SIMPLE_DATE_FORMAT:pattern e.g SIMPLE_DATE_FORMAT:yyyyMMdd
   * @param name
   */
  public TimeGranularitySpec(@Nonnull DataType dataType, int timeUnitSize, @Nonnull TimeUnit timeType,
      @Nonnull String timeFormat, @Nonnull String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(timeFormat);
    Preconditions.checkArgument(timeFormat.equals(TimeFormat.EPOCH.toString())
        || (timeFormat.startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())));
    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _timeUnitSize = timeUnitSize;
    _name = name;
    _timeFormat = timeFormat;
  }

  public DataType getDataType() {
    return _dataType;
  }

  public void setDataType(@Nonnull DataType dataType) {
    _dataType = dataType.getStoredType();
  }

  public TimeUnit getTimeType() {
    return _timeType;
  }

  public void setTimeType(@Nonnull TimeUnit timeType) {
    Preconditions.checkNotNull(timeType);

    _timeType = timeType;
  }

  public int getTimeUnitSize() {
    return _timeUnitSize;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setTimeUnitSize(int timeUnitSize) {
    Preconditions.checkArgument(timeUnitSize > 0);

    _timeUnitSize = timeUnitSize;
  }

  // Required by JSON de-serializer (for backward compatible). DO NOT REMOVE.
  public void setTimeunitSize(int timeUnitSize) {
    Preconditions.checkArgument(timeUnitSize > 0);

    _timeUnitSize = timeUnitSize;
  }

  public String getName() {
    return _name;
  }

  public void setName(@Nonnull String name) {
    Preconditions.checkNotNull(name);

    _name = name;
  }

  public void setTimeFormat(String timeFormat) {
    this._timeFormat = timeFormat;
  }

  public String getTimeFormat() {
    return _timeFormat;
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
        + ", name: " + _name + ", timeFormat: " + _timeFormat + " >";
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
          && _name.equals(anotherTimeGranularitySpec._name)
          && _timeFormat.equals(anotherTimeGranularitySpec._timeFormat);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = _dataType.hashCode();
    result = EqualityUtils.hashCodeOf(result, _timeType);
    result = EqualityUtils.hashCodeOf(result, _timeUnitSize);
    result = EqualityUtils.hashCodeOf(result, _name);
    result = EqualityUtils.hashCodeOf(result, _timeFormat);
    return result;
  }
}
