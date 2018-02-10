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
import com.google.gson.JsonObject;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;


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
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeGranularitySpec {
  private static final int DEFAULT_TIME_UNIT_SIZE = 1;
  private static final String DEFAULT_TIME_FORMAT = TimeFormat.EPOCH.toString();
  private static final String COLON_SEPARATOR = ":";

  private String _name;
  private DataType _dataType;
  private TimeUnit _timeType;
  private int _timeUnitSize = DEFAULT_TIME_UNIT_SIZE;
  private String _timeFormat = DEFAULT_TIME_FORMAT;

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

  public TimeGranularitySpec(@Nonnull DataType dataType, @Nonnull TimeUnit timeType, @Nonnull String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);

    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _name = name;
  }

  public TimeGranularitySpec(@Nonnull DataType dataType, @Nonnull TimeUnit timeType, @Nonnull String timeFormat,
      @Nonnull String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(timeFormat);
    Preconditions.checkArgument(timeFormat.equals(TimeFormat.EPOCH.toString()) || (timeFormat.startsWith(
        TimeFormat.SIMPLE_DATE_FORMAT.toString())));

    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _name = name;
    _timeFormat = timeFormat;
  }

  public TimeGranularitySpec(@Nonnull DataType dataType, int timeUnitSize, @Nonnull TimeUnit timeType,
      @Nonnull String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);

    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _timeUnitSize = timeUnitSize;
    _name = name;
  }

  public TimeGranularitySpec(@Nonnull DataType dataType, int timeUnitSize, @Nonnull TimeUnit timeType,
      @Nonnull String timeFormat, @Nonnull String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(timeFormat);
    Preconditions.checkArgument(timeFormat.equals(TimeFormat.EPOCH.toString()) || (timeFormat.startsWith(
        TimeFormat.SIMPLE_DATE_FORMAT.toString())));

    _dataType = dataType.getStoredType();
    _timeType = timeType;
    _timeUnitSize = timeUnitSize;
    _name = name;
    _timeFormat = timeFormat;
  }

  @Nonnull
  public String getName() {
    return _name;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setName(@Nonnull String name) {
    _name = name;
  }

  @Nonnull
  public DataType getDataType() {
    return _dataType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDataType(@Nonnull DataType dataType) {
    _dataType = dataType.getStoredType();
  }

  @Nonnull
  public TimeUnit getTimeType() {
    return _timeType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setTimeType(@Nonnull TimeUnit timeType) {
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

  @Nonnull
  public String getTimeFormat() {
    return _timeFormat;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setTimeFormat(@Nonnull String timeFormat) {
    _timeFormat = timeFormat;
  }

  /**
   * Convert the units of time since epoch to {@link DateTime} format using current <code>TimeGranularitySpec</code>.
   */
  public DateTime toDateTime(long timeSinceEpoch) {
    return new DateTime(_timeType.toMillis(timeSinceEpoch * _timeUnitSize));
  }

  /**
   * Convert the timeColumnValue to millis
   *
   * eg:
   * 1) given timeColumnValue = 416359 and timeGranularitySpec:{timeUnitSize=1,timetype=HOURS,timeFormat=EPOCH},
   * timeGranularitySpec.toMillis(416359) = 1498892400000 (i.e. timeColumnValue*60*60*1000)
   *
   * 2) given timeColumnValue = 4996308 and timeGranularitySpec:{timeUnitSize=5,timetype=MINUTES,timeFormat=EPOCH},
   * timeGranularitySpec.toMillis(4996308) = 1498892400000 (i.e. timeColumnValue*5*60*1000)
   *
   * 3) given timeColumnValue = 20170701 and timeGranularitySpec:{timeUnitSize=1,timetype=DAYS,timeFormat=SIMPLE_DATE_FORMAT:yyyyMMdd},
   * timeGranularitySpec.toMillis(20170701) = 1498892400000
   *
   * @param timeColumnValue - time column value to convert
   * @return time column value in millis
   */
  public Long toMillis(Object timeColumnValue) {
    Preconditions.checkNotNull(timeColumnValue);
    Long timeColumnValueMs;
    if (_timeFormat.equals(TimeFormat.EPOCH.toString())) {
      timeColumnValueMs = TimeUnit.MILLISECONDS.convert((Long) timeColumnValue * _timeUnitSize, _timeType);
    } else {
      String pattern = _timeFormat.split(COLON_SEPARATOR)[1];
      timeColumnValueMs = DateTimeFormat.forPattern(pattern).parseMillis(String.valueOf(timeColumnValue));
    }
    return timeColumnValueMs;
  }

  /**
   * Convert the time value in millis to the format from timeGranularitySpec
   * eg:
   * 1) given timeColumnValueMS = 1498892400000 and timeGranularitySpec:{timeUnitSize=1,timetype=HOURS,timeFormat=EPOCH},
   * timeGranularitySpec.fromMillis(1498892400000) = 416359 (i.e. timeColumnValueMS/(1000*60*60))
   *
   * 2) given timeColumnValueMS = 1498892400000 and timeGranularitySpec:{timeUnitSize=5,timetype=MINUTES,timeFormat=EPOCH},
   * timeGranularitySpec.fromMillis(1498892400000) = 4996308 (i.e. timeColumnValueMS/(1000*60*5))
   *
   * 3) given timeColumnValueMS = 1498892400000 and timeGranularitySpec:{timeUnitSize=1,timetype=DAYS,timeFormat=SIMPLE_DATE_FORMAT:yyyyMMdd},
   * timeGranularitySpec.fromMillis(1498892400000) = 20170701
   *
   * @param timeColumnValueMS - millis value to convert
   * @return time value in timeGranularitySpec format
   */
  public Object fromMillis(Long timeColumnValueMS) {
    Preconditions.checkNotNull(timeColumnValueMS);
    Object timeColumnValue;
    if (_timeFormat.equals(TimeFormat.EPOCH.toString())) {
      timeColumnValue = _timeType.convert(timeColumnValueMS, TimeUnit.MILLISECONDS) / _timeUnitSize;
    } else {
      String pattern = _timeFormat.split(COLON_SEPARATOR)[1];
      timeColumnValue = DateTimeFormat.forPattern(pattern).print(timeColumnValueMS);
    }
    return timeColumnValue;
  }

  /**
   * Returns the {@link JsonObject} representing the time granularity spec.
   * <p>Only contains fields with non-default value.
   * <p>NOTE: here we use {@link JsonObject} to preserve the insertion order.
   */
  public JsonObject toJsonObject() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("name", _name);
    jsonObject.addProperty("dataType", _dataType.name());
    jsonObject.addProperty("timeType", _timeType.name());
    if (_timeUnitSize != DEFAULT_TIME_UNIT_SIZE) {
      jsonObject.addProperty("timeUnitSize", _timeUnitSize);
    }
    if (!_timeFormat.equals(DEFAULT_TIME_FORMAT)) {
      jsonObject.addProperty("timeFormat", _timeFormat);
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    return "< field name: " + _name + ", data type: " + _dataType + ", time type: " + _timeType + ", time unit size: "
        + _timeUnitSize + ", time format: " + _timeFormat + " >";
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    TimeGranularitySpec that = (TimeGranularitySpec) o;
    return EqualityUtils.isEqual(_name, that._name) && EqualityUtils.isEqual(_dataType, that._dataType) && EqualityUtils
        .isEqual(_timeType, that._timeType) && EqualityUtils.isEqual(_timeUnitSize, that._timeUnitSize) && EqualityUtils
        .isEqual(_timeFormat, that._timeFormat);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_name);
    result = EqualityUtils.hashCodeOf(result, _dataType);
    result = EqualityUtils.hashCodeOf(result, _timeType);
    result = EqualityUtils.hashCodeOf(result, _timeUnitSize);
    result = EqualityUtils.hashCodeOf(result, _timeFormat);
    return result;
  }
}
