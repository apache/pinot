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

import com.google.common.base.Preconditions;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.TimestampUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


/**
 * Class to represent format from {@link DateTimeFieldSpec}
 */
public class DateTimeFormatSpec {

  // Colon format: 'size:timeUnit:timeFormat:pattern tz(timeZone)'
  // 'pattern' applies to the 'SIMPLE_DATE_FORMAT' time format
  // 'tz(timeZone)' is optional in the 'pattern'. If not specified, UTC timezone is used.
  private static final char COLON_SEPARATOR = ':';
  private static final int COLON_FORMAT_SIZE_POSITION = 0;
  private static final int COLON_FORMAT_TIME_UNIT_POSITION = 1;
  private static final int COLON_FORMAT_TIME_FORMAT_POSITION = 2;
  private static final int COLON_FORMAT_PATTERN_POSITION = 3;
  private static final int COLON_FORMAT_MIN_TOKENS = 3;
  private static final int COLON_FORMAT_MAX_TOKENS = 4;

  // Pipe format:
  // - EPOCH|timeUnit(|size)
  // - SIMPLE_DATE_FORMAT|pattern(|timeZone)
  // - TIMESTAMP
  private static final char PIPE_SEPARATOR = '|';
  private static final int PIPE_FORMAT_TIME_FORMAT_POSITION = 0;
  private static final int PIPE_FORMAT_TIME_UNIT_POSITION = 1;
  private static final int PIPE_FORMAT_SIZE_POSITION = 2;
  private static final int PIPE_FORMAT_PATTERN_POSITION = 1;
  private static final int PIPE_FORMAT_TIME_ZONE_POSITION = 2;
  private static final int PIPE_FORMAT_MIN_TOKENS = 1;
  private static final int PIPE_FORMAT_MAX_TOKENS = 3;

  private static final DateTimeFormatSpec TIMESTAMP =
      new DateTimeFormatSpec(1, DateTimeFormatUnitSpec.MILLISECONDS, DateTimeFormatPatternSpec.TIMESTAMP);

  // For EPOCH
  private final int _size;
  private final DateTimeFormatUnitSpec _unitSpec;
  // For SIMPLE_DATE_FORMAT
  private final DateTimeFormatPatternSpec _patternSpec;

  public DateTimeFormatSpec(String format) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(format), "Must provide format");

    if (Character.isDigit(format.charAt(0))) {
      // Colon format

      String[] tokens = StringUtil.split(format, COLON_SEPARATOR, COLON_FORMAT_MAX_TOKENS);
      Preconditions.checkArgument(tokens.length >= COLON_FORMAT_MIN_TOKENS && tokens.length <= COLON_FORMAT_MAX_TOKENS,
          "Invalid format: %s, must be of format 'size:timeUnit:timeFormat(:patternWithTz)'", format);

      TimeFormat timeFormat;
      try {
        timeFormat = TimeFormat.valueOf(tokens[COLON_FORMAT_TIME_FORMAT_POSITION]);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Invalid time format: %s in format: %s", tokens[COLON_FORMAT_TIME_FORMAT_POSITION], format));
      }

      switch (timeFormat) {
        case EPOCH:
          String sizeStr = tokens[COLON_FORMAT_SIZE_POSITION];
          try {
            _size = Integer.parseInt(sizeStr);
          } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Invalid size: %s in format: %s", sizeStr, format));
          }
          Preconditions.checkArgument(_size > 0, "Invalid size: %s in format: %s, must be positive", _size, format);
          String timeUnitStr = tokens[COLON_FORMAT_TIME_UNIT_POSITION];
          try {
            _unitSpec = new DateTimeFormatUnitSpec(timeUnitStr);
          } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Invalid time unit: %s in format: %s", timeUnitStr, format));
          }
          _patternSpec = DateTimeFormatPatternSpec.EPOCH;
          break;
        case TIMESTAMP:
          _size = 1;
          _unitSpec = DateTimeFormatUnitSpec.MILLISECONDS;
          _patternSpec = DateTimeFormatPatternSpec.TIMESTAMP;
          break;
        case SIMPLE_DATE_FORMAT:
          _size = 1;
          _unitSpec = DateTimeFormatUnitSpec.MILLISECONDS;
          String patternStr =
              tokens.length > COLON_FORMAT_PATTERN_POSITION ? tokens[COLON_FORMAT_PATTERN_POSITION] : null;
          try {
            _patternSpec = new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, patternStr);
          } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Invalid SIMPLE_DATE_FORMAT pattern: %s in format: %s", patternStr, format));
          }
          break;
        default:
          throw new IllegalStateException("Unsupported time format: " + timeFormat);
      }
    } else {
      // Pipe format

      String[] tokens = StringUtil.split(format, PIPE_SEPARATOR, PIPE_FORMAT_MAX_TOKENS);
      Preconditions.checkArgument(tokens.length >= PIPE_FORMAT_MIN_TOKENS && tokens.length <= PIPE_FORMAT_MAX_TOKENS,
          "Invalid format: %s, must be of format 'EPOCH|<timeUnit>(|<size>)' or "
              + "'SIMPLE_DATE_FORMAT|<pattern>(|<timeZone>)' or 'TIMESTAMP'", format);

      TimeFormat timeFormat;
      try {
        timeFormat = TimeFormat.valueOf(tokens[PIPE_FORMAT_TIME_FORMAT_POSITION]);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Invalid time format: %s in format: %s", tokens[PIPE_FORMAT_TIME_FORMAT_POSITION], format));
      }

      switch (timeFormat) {
        case EPOCH:
          if (tokens.length > PIPE_FORMAT_SIZE_POSITION) {
            try {
              _size = Integer.parseInt(tokens[PIPE_FORMAT_SIZE_POSITION]);
            } catch (Exception e) {
              throw new IllegalArgumentException(
                  String.format("Invalid size: %s in format: %s", tokens[COLON_FORMAT_SIZE_POSITION], format));
            }
            Preconditions.checkArgument(_size > 0, "Invalid size: %s in format: %s, must be positive", _size, format);
          } else {
            _size = 1;
          }
          try {
            _unitSpec = tokens.length > PIPE_FORMAT_TIME_UNIT_POSITION ? new DateTimeFormatUnitSpec(
                tokens[PIPE_FORMAT_TIME_UNIT_POSITION]) : DateTimeFormatUnitSpec.MILLISECONDS;
          } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Invalid time unit: %s in format: %s", tokens[PIPE_FORMAT_TIME_UNIT_POSITION], format));
          }
          _patternSpec = DateTimeFormatPatternSpec.EPOCH;
          break;
        case TIMESTAMP:
          _size = 1;
          _unitSpec = DateTimeFormatUnitSpec.MILLISECONDS;
          _patternSpec = DateTimeFormatPatternSpec.TIMESTAMP;
          break;
        case SIMPLE_DATE_FORMAT:
          _size = 1;
          _unitSpec = DateTimeFormatUnitSpec.MILLISECONDS;
          if (tokens.length > PIPE_FORMAT_TIME_ZONE_POSITION) {
            try {
              _patternSpec =
                  new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, tokens[PIPE_FORMAT_PATTERN_POSITION],
                      tokens[PIPE_FORMAT_TIME_ZONE_POSITION]);
            } catch (Exception e) {
              throw new IllegalArgumentException(
                  String.format("Invalid SIMPLE_DATE_FORMAT pattern: %s, time zone: %s in format: %s",
                      tokens[PIPE_FORMAT_PATTERN_POSITION], tokens[PIPE_FORMAT_TIME_ZONE_POSITION], format));
            }
          } else {
            try {
              String pattern =
                  tokens.length > PIPE_FORMAT_PATTERN_POSITION ? tokens[PIPE_FORMAT_PATTERN_POSITION] : null;
              _patternSpec = new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, pattern);
            } catch (Exception e) {
              throw new IllegalArgumentException(String.format("Invalid SIMPLE_DATE_FORMAT pattern: %s in format: %s",
                  tokens[PIPE_FORMAT_PATTERN_POSITION], format));
            }
          }
          break;
        default:
          throw new IllegalStateException("Unsupported time format: " + timeFormat);
      }
    }
  }

  private DateTimeFormatSpec(int size, DateTimeFormatUnitSpec unitSpec, DateTimeFormatPatternSpec patternSpec) {
    _size = size;
    _unitSpec = unitSpec;
    _patternSpec = patternSpec;
  }

  public static DateTimeFormatSpec forTimestamp() {
    return TIMESTAMP;
  }

  public static DateTimeFormatSpec forEpoch(String timeUnit) {
    return forEpoch(1, timeUnit);
  }

  public static DateTimeFormatSpec forEpoch(int size, String timeUnit) {
    Preconditions.checkArgument(size > 0, "Invalid size: {}, must be positive", size);
    Preconditions.checkArgument(timeUnit != null, "Must provide time unit");
    return new DateTimeFormatSpec(size, new DateTimeFormatUnitSpec(timeUnit), DateTimeFormatPatternSpec.EPOCH);
  }

  public static DateTimeFormatSpec forSimpleDateFormat(String patternWithTz) {
    return new DateTimeFormatSpec(1, DateTimeFormatUnitSpec.MILLISECONDS,
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, patternWithTz));
  }

  public static DateTimeFormatSpec forSimpleDateFormat(String pattern, @Nullable String timeZone) {
    return new DateTimeFormatSpec(1, DateTimeFormatUnitSpec.MILLISECONDS,
        new DateTimeFormatPatternSpec(TimeFormat.SIMPLE_DATE_FORMAT, pattern, timeZone));
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

  public DateTimeFormatPatternSpec getDateTimeFormatPattenSpec() {
    return _patternSpec;
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
        if (_patternSpec.getSdfPattern() != null) {
          return _patternSpec.getDateTimeFormatter().print(timeMs);
        } else {
          return ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeFormatPatternSpec.DEFAULT_DATE_TIME_ZONE)
              .withLocale(DateTimeFormatPatternSpec.DEFAULT_LOCALE).print(timeMs);
        }
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DateTimeFormatSpec that = (DateTimeFormatSpec) o;
    return _size == that._size && _unitSpec.equals(that._unitSpec) && _patternSpec.equals(that._patternSpec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_size, _unitSpec, _patternSpec);
  }

  @Override
  public String toString() {
    return "DateTimeFormatSpec{" + "_size=" + _size + ", _unitSpec=" + _unitSpec + ", _patternSpec=" + _patternSpec
        + '}';
  }
}
