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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to represent format from {@link DateTimeFieldSpec}
 */
public class DateTimeFormatSpec {

  private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeFormatSpec.class);

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
  private int _size;
  private DateTimeFormatUnitSpec _unitSpec;
  private DateTimeFormatPatternSpec _patternSpec;

  public DateTimeFormatSpec(String format) {
    _format = format;
    Preconditions.checkArgument(isValidFormat(format),
        "Invalid format string:%. Must be of format [0-9]+:<TimeUnit>:<TimeFormat>(:pattern)", format);
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
    Preconditions.checkArgument(isValidFormat(_format),
        "Invalid format:%. Must be of format [0-9]+:<TimeUnit>:<TimeFormat>(:pattern)", _format);

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
    Preconditions.checkArgument(isValidFormat(_format),
        "Invalid format:%. Must be of format [0-9]+:<TimeUnit>:<TimeFormat>(:pattern)", _format);

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
   * <ul>
   * <li>Given a timestamp in millis, convert it to the given format
   * This method should not do validation of outputGranularity.
   * The validation should be handled by caller using {@link #isValidFormat(String)}</li>
   * <ul>
   * <li>1) given dateTimeColumnValueMS = 1498892400000 and format=1:HOURS:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 416359 (i.e. dateTimeColumnValueMS/(1000*60*60))</li>
   * <li>2) given dateTimeColumnValueMS = 1498892400000 and format=5:MINUTES:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 4996308 (i.e. timeColumnValueMS/(1000*60*5))</li>
   * <li>3) given dateTimeColumnValueMS = 1498892400000 and
   * format=1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd, dateTimeSpec.fromMillis(1498892400000) = 20170701</li>
   * </ul>
   * </ul>
   * @param type - type of return value (can be int/long or string depending on time format)
   * @return dateTime column value in dateTimeFieldSpec
   */
  public <T extends Object> T fromMillisToFormat(Long dateTimeColumnValueMS, Class<T> type) {
    Preconditions.checkNotNull(dateTimeColumnValueMS);

    Object dateTimeColumnValue;
    if (_patternSpec.getTimeFormat().equals(TimeFormat.EPOCH)) {
      dateTimeColumnValue = _unitSpec.getTimeUnit().convert(dateTimeColumnValueMS, TimeUnit.MILLISECONDS) / _size;
    } else {
      dateTimeColumnValue = _patternSpec.getDateTimeFormatter().print(dateTimeColumnValueMS);
    }
    return type.cast(dateTimeColumnValue);
  }

  /**
   * <ul>
   * <li>Convert a time value in a format, to millis.
   * This method should not do validation of outputGranularity.
   * The validation should be handled by caller using {@link #isValidFormat(String)}</li>
   * <ul>
   * <li>1) given dateTimeColumnValue = 416359 and format=1:HOURS:EPOCH
   * dateTimeSpec.toMillis(416359) = 1498892400000 (i.e. timeColumnValue*60*60*1000)</li>
   * <li>2) given dateTimeColumnValue = 4996308 and format=5:MINUTES:EPOCH
   * dateTimeSpec.toMillis(4996308) = 1498892400000 (i.e. timeColumnValue*5*60*1000)</li>
   * <li>3) given dateTimeColumnValue = 20170701 and format=1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd
   * dateTimeSpec.toMillis(20170701) = 1498892400000</li>
   * </ul>
   * <ul>
   * @param dateTimeColumnValue - datetime Column value to convert to millis
   * @return datetime value in millis
   */
  public Long fromFormatToMillis(Object dateTimeColumnValue) {
    Preconditions.checkNotNull(dateTimeColumnValue);

    long timeColumnValueMS;
    if (_patternSpec.getTimeFormat().equals(TimeFormat.EPOCH)) {
      timeColumnValueMS = TimeUnit.MILLISECONDS.convert((Long) dateTimeColumnValue * _size, _unitSpec.getTimeUnit());
    } else {
      timeColumnValueMS = _patternSpec.getDateTimeFormatter().parseMillis(String.valueOf(dateTimeColumnValue));
    }
    return timeColumnValueMS;
  }

  /**
   * Validates the format string in the dateTimeFieldSpec
   */
  public static boolean isValidFormat(String format) {
    if (format == null) {
      LOGGER.error("Format string in dateTimeFieldSpec is null");
      return false;
    }
    String[] formatTokens = format.split(COLON_SEPARATOR, MAX_FORMAT_TOKENS);
    if (formatTokens.length < MIN_FORMAT_TOKENS || formatTokens.length > MAX_FORMAT_TOKENS) {
      LOGGER.error("Incorrect format:{}. Must be of format size:timeunit:timeformat(:pattern)", format);
      return false;
    }

    if (!formatTokens[FORMAT_SIZE_POSITION].matches(NUMBER_REGEX)) {
      LOGGER.error("Incorrect format size:{} in format:{}. Must be of format [0-9]+:<TimeUnit>:<TimeFormat>(:pattern)",
          formatTokens[FORMAT_SIZE_POSITION], format);
      return false;
    }

    if (!DateTimeFormatUnitSpec.isValidUnitSpec(formatTokens[FORMAT_UNIT_POSITION])) {
      LOGGER.error("Incorrect format unit:{} in format:{}. Must be of format [0-9]+:<TimeUnit>:<TimeFormat>(:pattern)",
          formatTokens[FORMAT_UNIT_POSITION], format);
      return false;
    }

    if (formatTokens.length == MIN_FORMAT_TOKENS) {
      if (!formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.EPOCH.toString())) {
        LOGGER.error("Incorrect format type:{} in format:{}. Must be of [0-9]+:<TimeUnit>:EPOCH",
            formatTokens[FORMAT_TIMEFORMAT_POSITION], format);
        return false;
      }
    } else {
      if (!formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.SIMPLE_DATE_FORMAT.toString())) {
        LOGGER.error("Incorrect format type:{} in format:{}. Must be of [0-9]+:<TimeUnit>:SIMPLE_DATE_FORMAT:pattern",
            formatTokens[FORMAT_TIMEFORMAT_POSITION], format);
        return false;
      }
    }
    return true;
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
