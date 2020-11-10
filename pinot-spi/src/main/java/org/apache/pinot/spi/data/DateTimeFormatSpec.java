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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;


/**
 * Class to represent format from {@link DateTimeFieldSpec}
 */
public class DateTimeFormatSpec {

  public static final String NUMBER_REGEX = "[1-9][0-9]*";
  public static final String COLON_SEPARATOR = ":";

  /* DateTimeFieldSpec format is of format size:timeUnit:timeformat:pattern tz(timezone)
   * tz(timezone) is optional. If not specified, UTC timezone is used */
  public static final int FORMAT_SIZE_POSITION = 0;
  public static final int FORMAT_UNIT_POSITION = 1;
  public static final int FORMAT_TIMEFORMAT_POSITION = 2;
  public static final int FORMAT_PATTERN_POSITION = 3;
  public static final int MIN_FORMAT_TOKENS = 3;
  public static final int MAX_FORMAT_TOKENS = 4;

  private final String _format;
  private final int _size;
  private final DateTimeFormatUnitSpec _unitSpec;
  private final DateTimeFormatPatternSpec _patternSpec;

  public DateTimeFormatSpec(String format) {
    _format = format;
    validateFormat(format);
    String[] formatTokens = format.split(COLON_SEPARATOR, MAX_FORMAT_TOKENS);
    _size = Integer.parseInt(formatTokens[FORMAT_SIZE_POSITION]);
    _unitSpec = new DateTimeFormatUnitSpec(formatTokens[FORMAT_UNIT_POSITION]);
    if (formatTokens.length == MAX_FORMAT_TOKENS) {
      _patternSpec = new DateTimeFormatPatternSpec(formatTokens[FORMAT_TIMEFORMAT_POSITION],
          formatTokens[FORMAT_PATTERN_POSITION]);
    } else {
      _patternSpec = new DateTimeFormatPatternSpec(formatTokens[FORMAT_TIMEFORMAT_POSITION], null);
    }
  }

  /**
   * Constructs a dateTimeSpec format, given the components of a format
   */
  public DateTimeFormatSpec(int columnSize, String columnUnit, String columnTimeFormat) {
    _format = Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit, columnTimeFormat);
    validateFormat(_format);

    _size = columnSize;
    _unitSpec = new DateTimeFormatUnitSpec(columnUnit);
    _patternSpec = new DateTimeFormatPatternSpec(columnTimeFormat, null);
  }

  /**
   * Constructs a dateTimeSpec format, given the components of a format
   * @param sdfPattern and tz
   */
  public DateTimeFormatSpec(int columnSize, String columnUnit, String columnTimeFormat, String sdfPattern) {
    _format = Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit, columnTimeFormat, sdfPattern);
    validateFormat(_format);

    _size = columnSize;
    _unitSpec = new DateTimeFormatUnitSpec(columnUnit);
    _patternSpec = new DateTimeFormatPatternSpec(columnTimeFormat, sdfPattern);
  }

  public String getFormat() {
    return _format;
  }

  public int getColumnSize() {
    return _size;
  }

  public TimeUnit getColumnUnit() {
    return _unitSpec.getTimeUnit();
  }

  public DateTimeFormatUnitSpec.DateTimeTransformUnit getColumnDateTimeTransformUnit() {
    return _unitSpec.getDateTimeTransformUnit();
  }

  public TimeFormat getTimeFormat() {
    return _patternSpec.getTimeFormat();
  }

  public String getSDFPattern() {
    return _patternSpec.getSdfPattern();
  }

  public DateTimeZone getDateTimezone() {
    return _patternSpec.getDateTimeZone();
  }

  public DateTimeFormatter getDateTimeFormatter() {
    return _patternSpec.getDateTimeFormatter();
  }

  /**
   * Converts the time in millis to the date time format.
   * <ul>
   *   <li>Given timeMs=1498892400000 and format='1:HOURS:EPOCH', returns 1498892400000/(1000*60*60)='416359'</li>
   *   <li>Given timeMs=1498892400000 and format='5:MINUTES:EPOCH', returns 1498892400000/(1000*60*5)='4996308'</li>
   *   <li>Given timeMs=1498892400000 and format='1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd', returns '20170701'</li>
   * </ul>
   */
  public String fromMillisToFormat(long timeMs) {
    if (_patternSpec.getTimeFormat() == TimeFormat.EPOCH) {
      return Long.toString(_unitSpec.getTimeUnit().convert(timeMs, TimeUnit.MILLISECONDS) / _size);
    } else {
      return _patternSpec.getDateTimeFormatter().print(timeMs);
    }
  }

  /**
   * Converts the date time value to the time in millis.
   * <ul>
   *   <li>Given dateTimeValue='416359' and format='1:HOURS:EPOCH', returns 416359*(1000*60*60)=1498892400000</li>
   *   <li>Given dateTimeValue='4996308' and format='5:MINUTES:EPOCH', returns 4996308*(1000*60*5)=1498892400000</li>
   *   <li>Given dateTimeValue='20170701' and format='1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd', returns 1498892400000</li>
   * </ul>
   */
  public long fromFormatToMillis(String dateTimeValue) {
    if (_patternSpec.getTimeFormat() == TimeFormat.EPOCH) {
      return TimeUnit.MILLISECONDS.convert(Long.parseLong(dateTimeValue) * _size, _unitSpec.getTimeUnit());
    } else {
      return _patternSpec.getDateTimeFormatter().parseMillis(dateTimeValue);
    }
  }

  /**
   * Validates the format string in the dateTimeFieldSpec
   */
  public static void validateFormat(String format) {
    Preconditions.checkNotNull(format, "Format string in dateTimeFieldSpec must not be null");
    String[] formatTokens = format.split(COLON_SEPARATOR, MAX_FORMAT_TOKENS);
    Preconditions.checkState(formatTokens.length >= MIN_FORMAT_TOKENS && formatTokens.length <= MAX_FORMAT_TOKENS,
        "Incorrect format: %s. Must be of format 'size:timeunit:timeformat(:pattern)'", format);
    Preconditions.checkState(formatTokens[FORMAT_SIZE_POSITION].matches(NUMBER_REGEX),
        "Incorrect format size: %s in format: %s. Must be of format '[0-9]+:<TimeUnit>:<TimeFormat>(:pattern)'",
        formatTokens[FORMAT_SIZE_POSITION], format);

    DateTimeFormatUnitSpec.validateUnitSpec(formatTokens[FORMAT_UNIT_POSITION]);

    if (formatTokens.length == MIN_FORMAT_TOKENS) {
      Preconditions.checkState(formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.EPOCH.toString()),
          "Incorrect format type: %s in format: %s. Must be of '[0-9]+:<TimeUnit>:EPOCH'",
          formatTokens[FORMAT_TIMEFORMAT_POSITION], format);
    } else {
      Preconditions
          .checkState(formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.SIMPLE_DATE_FORMAT.toString()),
              "Incorrect format type: %s in format: %s. Must be of '[0-9]+:<TimeUnit>:SIMPLE_DATE_FORMAT:pattern'",
              formatTokens[FORMAT_TIMEFORMAT_POSITION], format);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    DateTimeFormatSpec that = (DateTimeFormatSpec) o;

    return EqualityUtils.isEqual(_size, that._size) && EqualityUtils.isEqual(_format, that._format) && EqualityUtils
        .isEqual(_unitSpec, that._unitSpec) && EqualityUtils.isEqual(_patternSpec, that._patternSpec);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_format);
    result = EqualityUtils.hashCodeOf(result, _size);
    result = EqualityUtils.hashCodeOf(result, _unitSpec);
    result = EqualityUtils.hashCodeOf(result, _patternSpec);
    return result;
  }

  @Override
  public String toString() {
    return "DateTimeFormatSpec{" + "_format='" + _format + '\'' + ", _size=" + _size + ", _unitSpec=" + _unitSpec
        + ", _patternSpec=" + _patternSpec + '}';
  }
}
