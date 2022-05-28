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
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.spi.utils.TimestampUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;


/**
 * Class to represent format from {@link DateTimeFieldSpec}
 */
public class DateTimeFormatSpec {

  public static final String NUMBER_REGEX = "[1-9][0-9]*";
  public static final String COLON_SEPARATOR = ":";
  public static final String PIPE_SEPARATOR = "|";

  /* DateTimeFieldSpec format is of format size:timeUnit:timeformat:pattern tz(timezone)
   * tz(timezone) is optional. If not specified, UTC timezone is used */
  public static final int FORMAT_SIZE_POSITION = 0;
  public static final int FORMAT_UNIT_POSITION = 1;
  public static final int FORMAT_TIMEFORMAT_POSITION = 2;
  public static final int FORMAT_PATTERN_POSITION = 3;
  public static final int MIN_FORMAT_TOKENS = 3;
  public static final int MAX_FORMAT_TOKENS = 4;

  public static final int FORMAT_TIMEFORMAT_POSITION_PIPE = 0;
  public static final int MIN_FORMAT_TOKENS_PIPE = 1;
  public static final int MAX_FORMAT_TOKENS_PIPE = 3;

  //Applicable for SDF|<timeFormat>(|<timezone>)
  public static final int SDF_PATTERN_POSITION = 1;
  public static final int SDF_TIMEZONE_POSITION = 2;

  //Applicable for EPOCH|<timeUnit>(|<size>)
  public static final int EPOCH_UNIT_POSITION = 1;
  public static final int EPOCH_SIZE_POSITION = 2;

  private final String _format;
  private final int _size;
  private final DateTimeFormatUnitSpec _unitSpec;
  private final DateTimeFormatPatternSpec _patternSpec;

  public DateTimeFormatSpec(String format) {
    _format = format;
    if (Character.isDigit(_format.charAt(0))) {
      String[] formatTokens = validateFormat(format);
      if (formatTokens.length == MAX_FORMAT_TOKENS) {
        _patternSpec = new DateTimeFormatPatternSpec(formatTokens[FORMAT_TIMEFORMAT_POSITION],
            formatTokens[FORMAT_PATTERN_POSITION]);
      } else {
        _patternSpec = new DateTimeFormatPatternSpec(formatTokens[FORMAT_TIMEFORMAT_POSITION]);
      }
      if (_patternSpec.getTimeFormat() == TimeFormat.TIMESTAMP) {
        // TIMESTAMP type stores millis since epoch
        _size = 1;
        _unitSpec = new DateTimeFormatUnitSpec("MILLISECONDS");
      } else {
        _size = Integer.parseInt(formatTokens[FORMAT_SIZE_POSITION]);
        _unitSpec = new DateTimeFormatUnitSpec(formatTokens[FORMAT_UNIT_POSITION]);
      }
    } else {
      String[] formatTokens = validatePipeFormat(format);
      if (formatTokens[FORMAT_TIMEFORMAT_POSITION_PIPE].equals(TimeFormat.EPOCH.toString())) {
        _patternSpec = new DateTimeFormatPatternSpec(formatTokens[FORMAT_TIMEFORMAT_POSITION_PIPE]);
        _unitSpec = new DateTimeFormatUnitSpec(formatTokens[EPOCH_UNIT_POSITION]);
        if (formatTokens.length == MAX_FORMAT_TOKENS_PIPE) {
          _size = Integer.parseInt(formatTokens[EPOCH_SIZE_POSITION]);
        } else {
          _size = 1;
        }
      } else if (formatTokens[FORMAT_TIMEFORMAT_POSITION_PIPE].equals(TimeFormat.SIMPLE_DATE_FORMAT.toString())) {
        if (formatTokens.length == MAX_FORMAT_TOKENS_PIPE) {
          _patternSpec = new DateTimeFormatPatternSpec(TimeFormat.valueOf(
              formatTokens[FORMAT_TIMEFORMAT_POSITION_PIPE]),
              formatTokens[SDF_PATTERN_POSITION],
              formatTokens[SDF_TIMEZONE_POSITION]);
        } else {
          _patternSpec = new DateTimeFormatPatternSpec(TimeFormat.valueOf(
              formatTokens[FORMAT_TIMEFORMAT_POSITION_PIPE]),
              formatTokens[SDF_PATTERN_POSITION], DateTimeFormatPatternSpec.DEFAULT_DATETIMEZONE.toString());
        }
        _unitSpec = new DateTimeFormatUnitSpec(TimeUnit.DAYS.toString());
        _size = 1;
      } else {
        _patternSpec = new DateTimeFormatPatternSpec(formatTokens[FORMAT_TIMEFORMAT_POSITION_PIPE]);
        _unitSpec = new DateTimeFormatUnitSpec(TimeUnit.MILLISECONDS.toString());
        _size = 1;
      }
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
    _patternSpec = new DateTimeFormatPatternSpec(columnTimeFormat);
  }

  /**
   * Constructs a dateTimeSpec format, given the components of a format
   */
  public DateTimeFormatSpec(String columnTimeFormat, String columnUnit, int columnSize) {
    if (columnSize != 1) {
      _format = Joiner.on(PIPE_SEPARATOR).join(columnTimeFormat, columnUnit, columnSize);
    } else {
      _format = Joiner.on(PIPE_SEPARATOR).join(columnTimeFormat, columnUnit);
    }

    validatePipeFormat(_format);

    _size = columnSize;
    _unitSpec = new DateTimeFormatUnitSpec(columnUnit);
    _patternSpec = new DateTimeFormatPatternSpec(columnTimeFormat);
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

  public DateTimeFormatSpec(String timeFormat, String columnTimeFormat, @Nullable String timeZone) {
    if (timeZone == null) {
      timeZone = DateTimeFormatPatternSpec.DEFAULT_DATETIMEZONE.toString();
      _format = Joiner.on(PIPE_SEPARATOR).join(timeFormat, columnTimeFormat);
    } else {
      _format = Joiner.on(PIPE_SEPARATOR).join(timeFormat, columnTimeFormat, timeZone);
    }
    validatePipeFormat(_format);
    _size = 1;
    _unitSpec = new DateTimeFormatUnitSpec("DAYS");
    _patternSpec = new DateTimeFormatPatternSpec(TimeFormat.valueOf(timeFormat), columnTimeFormat, timeZone);
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
   *   <li>Given timeMs=1498892400000 and format='1:MILLISECONDS:TIMESTAMP', returns '2017-07-01 00:00:00.0'</li>
   *   <li>Given timeMs=1498892400000 and format='1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd', returns '20170701'</li>
   * </ul>
   */
  public String fromMillisToFormat(long timeMs) {
    switch (_patternSpec.getTimeFormat()) {
      case EPOCH:
        return Long.toString(_unitSpec.getTimeUnit().convert(timeMs, TimeUnit.MILLISECONDS) / _size);
      case TIMESTAMP:
        return new Timestamp(timeMs).toString();
      case SIMPLE_DATE_FORMAT:
        return _patternSpec.getDateTimeFormatter().print(timeMs);
      default:
        throw new IllegalStateException("Unsupported time format: " + _patternSpec.getTimeFormat());
    }
  }

  /**
   * Converts the date time value to the time in millis.
   * <ul>
   *   <li>Given dateTimeValue='416359' and format='1:HOURS:EPOCH', returns 416359*(1000*60*60)=1498892400000</li>
   *   <li>Given dateTimeValue='4996308' and format='5:MINUTES:EPOCH', returns 4996308*(1000*60*5)=1498892400000</li>
   *   <li>Given dateTimeValue='2017-07-01 00:00:00' and format='1:MILLISECONDS:TIMESTAMP', returns 1498892400000</li>
   *   <li>Given dateTimeValue='1498892400000' and format='1:DAYS:TIMESTAMP', returns 1498892400000</li>
   *   <li>Given dateTimeValue='20170701' and format='1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd', returns 1498892400000</li>
   * </ul>
   */
  public long fromFormatToMillis(String dateTimeValue) {
    switch (_patternSpec.getTimeFormat()) {
      case EPOCH:
        return TimeUnit.MILLISECONDS.convert(Long.parseLong(dateTimeValue) * _size, _unitSpec.getTimeUnit());
      case TIMESTAMP:
        return TimestampUtils.toMillisSinceEpoch(dateTimeValue);
      case SIMPLE_DATE_FORMAT:
        return _patternSpec.getDateTimeFormatter().parseMillis(dateTimeValue);
      default:
        throw new IllegalStateException("Unsupported time format: " + _patternSpec.getTimeFormat());
    }
  }

  /**
   * Validates the format string in the dateTimeFieldSpec
   */
  public static String[] validateFormat(String format) {
    Preconditions.checkNotNull(format, "Format string in dateTimeFieldSpec must not be null");
    String[] formatTokens = StringUtils.split(format, COLON_SEPARATOR, MAX_FORMAT_TOKENS);
    Preconditions.checkState(formatTokens.length >= MIN_FORMAT_TOKENS && formatTokens.length <= MAX_FORMAT_TOKENS,
        "Incorrect format: %s. Must be of format 'size:timeunit:timeformat(:pattern)'", format);
    Preconditions.checkState(formatTokens[FORMAT_SIZE_POSITION].matches(NUMBER_REGEX),
        "Incorrect format size: %s in format: %s. Must be of format '[0-9]+:<TimeUnit>:<TimeFormat>(:pattern)'",
        formatTokens[FORMAT_SIZE_POSITION], format);

    DateTimeFormatUnitSpec.validateUnitSpec(formatTokens[FORMAT_UNIT_POSITION]);

    if (formatTokens.length == MIN_FORMAT_TOKENS) {
      Preconditions.checkState(formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.EPOCH.toString())
              || formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.TIMESTAMP.toString()),
          "Incorrect format type: %s in format: %s. Must be of '[0-9]+:<TimeUnit>:EPOCH|TIMESTAMP'",
          formatTokens[FORMAT_TIMEFORMAT_POSITION], format);
    } else {
      Preconditions
          .checkState(formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.SIMPLE_DATE_FORMAT.toString()),
              "Incorrect format type: %s in format: %s. Must be of '[0-9]+:<TimeUnit>:SIMPLE_DATE_FORMAT:pattern'",
              formatTokens[FORMAT_TIMEFORMAT_POSITION], format);
    }
    return formatTokens;
  }

  /**
   * Validates the pipe format string in the dateTimeFieldSpec
   */
  public static String[] validatePipeFormat(String format) {
    Preconditions.checkNotNull(format, "Format string in dateTimeFieldSpec must not be null");
    String[] formatTokens = StringUtils.split(format, PIPE_SEPARATOR, MAX_FORMAT_TOKENS_PIPE);
    Preconditions.checkState(formatTokens.length >= MIN_FORMAT_TOKENS_PIPE
            && formatTokens.length <= MAX_FORMAT_TOKENS_PIPE,
        "Incorrect format: %s. Must be of the format 'EPOCH|<timeUnit>(|<size>)'"
            + " or 'SDF|<timeFormat>(|<timezone>)' or 'TIMESTAMP'");
    if (formatTokens.length == MIN_FORMAT_TOKENS_PIPE) {
      Preconditions.checkState(formatTokens[FORMAT_TIMEFORMAT_POSITION_PIPE].equals(TimeFormat.TIMESTAMP.toString()),
          "Incorrect format type: %s. Must be of TIMESTAMP", formatTokens[FORMAT_TIMEFORMAT_POSITION_PIPE]);
    } else {
      Preconditions.checkState(formatTokens[FORMAT_SIZE_POSITION].equals(TimeFormat.EPOCH.toString())
              || formatTokens[FORMAT_SIZE_POSITION].equals(TimeFormat.SIMPLE_DATE_FORMAT.toString()),
          "Incorrect format %s. Must be of 'EPOCH|<timeUnit>(|<size>)' or" + "'SDF|<timeFormat>(|<timezone>)'");

      if (formatTokens.length == MAX_FORMAT_TOKENS_PIPE
          && formatTokens[FORMAT_SIZE_POSITION].equals(TimeFormat.EPOCH.toString())) {
          Preconditions.checkState(formatTokens[EPOCH_SIZE_POSITION].matches(NUMBER_REGEX),
              "Incorrect format size: %s in format: %s. Must be of format 'EPOCH|<timeUnit>|[0-9]+'",
              formatTokens[EPOCH_SIZE_POSITION], format);
      }
    }
    return formatTokens;
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
