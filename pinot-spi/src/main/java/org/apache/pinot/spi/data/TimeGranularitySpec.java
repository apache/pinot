/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * @deprecated Use DateTimeFieldSpec instead.
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
 * <p><code>new TimeGranularitySpec(DataType.STRING, 1, TimeUnit.HOURS, TimeFormat.SIMPLE_DATE_FORMAT.toString() +":yyyyMMdd", "hour");
 * </code>
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeGranularitySpec implements Serializable {
  private static final int DEFAULT_TIME_UNIT_SIZE = 1;
  private static final String DEFAULT_TIME_FORMAT = TimeFormat.EPOCH.toString();
  private static final String COLON_SEPARATOR = ":";

  private String _name;
  private DataType _dataType;
  private TimeUnit _timeType;
  private int _timeUnitSize = DEFAULT_TIME_UNIT_SIZE;
  private String _timeFormat = DEFAULT_TIME_FORMAT;

  /*
  Deprecated. Use {@link DateTimeFieldSpec.TimeFormat} instead
   * Can be either EPOCH (default) or SIMPLE_DATE_FORMAT:pattern e.g SIMPLE_DATE_FORMAT:yyyyMMdd
   */
  @Deprecated
  public enum TimeFormat {
    EPOCH, //default
    SIMPLE_DATE_FORMAT
  }

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public TimeGranularitySpec() {
  }

  public TimeGranularitySpec(DataType dataType, TimeUnit timeType, String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);

    _dataType = dataType;
    _timeType = timeType;
    _name = name;
  }

  public TimeGranularitySpec(DataType dataType, TimeUnit timeType, String timeFormat, String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(timeFormat);
    Preconditions
        .checkArgument(timeFormat.equals(TimeFormat.EPOCH.toString()) || (timeFormat.startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())));

    _dataType = dataType;
    _timeType = timeType;
    _name = name;
    _timeFormat = timeFormat;
  }

  public TimeGranularitySpec(DataType dataType, int timeUnitSize, TimeUnit timeType, String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);

    _dataType = dataType;
    _timeType = timeType;
    _timeUnitSize = timeUnitSize;
    _name = name;
  }

  public TimeGranularitySpec(DataType dataType, int timeUnitSize, TimeUnit timeType, String timeFormat, String name) {
    Preconditions.checkNotNull(timeType);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(timeFormat);
    Preconditions
        .checkArgument(timeFormat.equals(TimeFormat.EPOCH.toString()) || (timeFormat.startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())));

    _dataType = dataType;
    _timeType = timeType;
    _timeUnitSize = timeUnitSize;
    _name = name;
    _timeFormat = timeFormat;
  }

  public String getName() {
    return _name;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setName(String name) {
    _name = name;
  }

  public DataType getDataType() {
    return _dataType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDataType(DataType dataType) {
    _dataType = dataType;
  }

  public TimeUnit getTimeType() {
    return _timeType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setTimeType(TimeUnit timeType) {
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

  public String getTimeFormat() {
    return _timeFormat;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setTimeFormat(String timeFormat) {
    _timeFormat = timeFormat;
  }

  /**
   * Returns the {@link ObjectNode} representing the time granularity spec.
   * <p>Only contains fields with non-default value.
   * <p>NOTE: here we use {@link ObjectNode} to preserve the insertion order.
   */
  public ObjectNode toJsonObject() {
    ObjectNode jsonObject = JsonUtils.newObjectNode();
    jsonObject.put("name", _name);
    jsonObject.put("dataType", _dataType.name());
    jsonObject.put("timeType", _timeType.name());
    if (_timeUnitSize != DEFAULT_TIME_UNIT_SIZE) {
      jsonObject.put("timeUnitSize", _timeUnitSize);
    }
    if (!_timeFormat.equals(DEFAULT_TIME_FORMAT)) {
      jsonObject.put("timeFormat", _timeFormat);
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    return "< field name: " + _name + ", data type: " + _dataType + ", time type: " + _timeType + ", time unit size: " + _timeUnitSize
        + ", time format: " + _timeFormat + " >";
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
