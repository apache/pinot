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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.EnumUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat;

/**
 * Class to represent format from {@link DateTimeFieldSpec}
 */
public class DateTimeFormatSpec {

  public static final String FORMAT_TOKENS_ERROR_STR =
      "format must be of pattern size:timeunit:timeformat(:pattern)";
  public static final String FORMAT_PATTERN_ERROR_STR =
      "format must be of format [0-9]+:<TimeUnit>:<TimeFormat>(:pattern)";
  public static final String TIME_FORMAT_ERROR_STR =
      "format must be of format [0-9]+:<TimeUnit>:EPOCH or [0-9]+:<TimeUnit>:SIMPLE_DATE_FORMAT:<format>";
  public static final String GRANULARITY_TOKENS_ERROR_STR =
      "granularity must be of format size:timeunit";
  public static final String GRANULARITY_PATTERN_ERROR_STR =
      "granularity must be of format [0-9]+:<TimeUnit>";
  public static final String NUMBER_REGEX = "[1-9][0-9]*";

  public static final String COLON_SEPARATOR = ":";

  /* DateTimeFieldSpec format is of format size:timeUnit:timeformat:pattern(if sdf) */
  public static final int FORMAT_SIZE_POSITION = 0;
  public static final int FORMAT_UNIT_POSITION = 1;
  public static final int FORMAT_TIMEFORMAT_POSITION = 2;
  public static final int FORMAT_PATTERN_POSITION = 3;
  public static final int MIN_FORMAT_TOKENS = 3;
  public static final int MAX_FORMAT_TOKENS = 4;


  /**
   * Time unit enum with range from MILLISECONDS to YEARS
   */
  public enum DateTimeTransformUnit {
    YEARS,
    MONTHS,
    WEEKS,
    DAYS,
    HOURS,
    MINUTES,
    SECONDS,
    MILLISECONDS;
  }

  private String _format;

  public DateTimeFormatSpec(String format) {
    _format = format;
  }

  public String getFormat() {
    return _format;
  }

  /**
   * Constructs a dateTimeSpec format, given the components of a format
   * @param columnSize
   * @param columnUnit
   * @param columnTimeFormat
   * @return
   */
  public static DateTimeFormatSpec constructFormat(int columnSize, TimeUnit columnUnit, String columnTimeFormat) {
    Preconditions.checkArgument(columnSize > 0);
    Preconditions.checkNotNull(columnUnit);
    Preconditions.checkNotNull(columnTimeFormat);
    Preconditions.checkArgument(TimeFormat.EPOCH.toString().equals(columnTimeFormat),
        "TimeFormat must be EPOCH if not providing sdf pattern");
    return new DateTimeFormatSpec(Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit, columnTimeFormat));
  }

  /**
   * Constructs a dateTimeSpec format, given the components of a format
   * @param columnSize
   * @param columnUnit
   * @param columnTimeFormat
   * @param sdfPattern
   * @return
   */
  public static DateTimeFormatSpec constructFormat(int columnSize, TimeUnit columnUnit,
      String columnTimeFormat, String sdfPattern) {
    Preconditions.checkArgument(columnSize > 0);
    Preconditions.checkNotNull(columnUnit);
    Preconditions.checkNotNull(columnTimeFormat);
    Preconditions.checkArgument(TimeFormat.SIMPLE_DATE_FORMAT.toString().equals(columnTimeFormat),
        "TimeFormat must be SIMPLE_DATE_FORMAT if providing sdf pattern");
    Preconditions.checkNotNull(sdfPattern);
    return new DateTimeFormatSpec(Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit, columnTimeFormat, sdfPattern));
  }

  /**
   * Extracts the column size from the format of a dateTimeSpec
   * (1st token in colon separated format)
   * This method should not do validation of outputGranularity.
   * The validation should be handled by caller using {@link #isValidFormat(String)}
   * eg: if format=1:HOURS:EPOCH, will return 1
   * @return
   */
  public int getColumnSize() {
    String[] formatTokens = _format.split(COLON_SEPARATOR);
    int size = Integer.valueOf(formatTokens[FORMAT_SIZE_POSITION]);
    return size;
  }

  /**
   * Extracts the column unit from the format of a dateTimeSpec
   * (2nd token in colon separated format)
   * This method should not do validation of outputGranularity.
   * The validation should be handled by caller using {@link #isValidFormat(String)}
   * eg: if format=5:MINUTES:EPOCH, will return MINUTES
   * @return
   */
  public TimeUnit getColumnUnit() {
    String[] formatTokens = _format.split(COLON_SEPARATOR);
    TimeUnit unit = TimeUnit.valueOf(formatTokens[FORMAT_UNIT_POSITION]);
    return unit;
  }

  /**
   * Extracts the column DateTimeTransformUnit from the format of a dateTimeSpec
   * (2nd token in colon separated format)
   * This method should not do validation of outputGranularity.
   * The validation should be handled by caller using {@link #isValidFormat(String)}
   * eg: if format=5:WEEKS:EPOCH, will return WEEKS
   * @return
   */
  public DateTimeTransformUnit getColumnDateTimeTransformUnit() {
    String[] formatTokens = _format.split(COLON_SEPARATOR);
    DateTimeTransformUnit unit = DateTimeTransformUnit.valueOf(formatTokens[FORMAT_UNIT_POSITION]);
    return unit;
  }


  /**
   * Extracts the TimeFormat from the format of a dateTimeSpec
   * (3rd token in colon separated format)
   * This method should not do validation of outputGranularity.
   * The validation should be handled by caller using {@link #isValidFormat(String)}
   * eg: if format=1:DAYS:EPOCH, will return EPOCH
   * @return
   */
  public TimeFormat getTimeFormat() {
    String[] formatTokens = _format.split(COLON_SEPARATOR);
    TimeFormat timeFormat = TimeFormat.valueOf(formatTokens[FORMAT_TIMEFORMAT_POSITION]);
    return timeFormat;
  }

  /**
   * Extracts the simmple date format pattern from the format of a dateTimeSpec
   * (4th token in colon separated format in case of TimeFormat=SIMPLE_DATE_FORMAT)
   * This method should not do validation of outputGranularity.
   * The validation should be handled by caller using {@link #isValidFormat(String)}
   * eg: if format=1:HOURS:EPOCH, will throw exception
   * if format=1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH will return yyyyMMddHH
   * @return
   */
  public String getSDFPattern() {
    String pattern = null;
    // split with limit, as pattern can have ':'
    String[] formatTokens = _format.split(COLON_SEPARATOR, MAX_FORMAT_TOKENS);
    if (formatTokens.length == MAX_FORMAT_TOKENS) {
      pattern = formatTokens[FORMAT_PATTERN_POSITION];
    }
    return pattern;
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
   * @param dateTimeColumnValueMS
   * @param toFormat - the format in which to convert the millis value
   * @param type - type of return value (can be int/long or string depending on time format)
   * @return dateTime column value in dateTimeFieldSpec
   */
  public <T extends Object> T fromMillisToFormat(Long dateTimeColumnValueMS, Class<T> type) {
    Preconditions.checkNotNull(dateTimeColumnValueMS);

    Object dateTimeColumnValue = null;

    TimeFormat timeFormat = getTimeFormat();
    if (timeFormat.equals(TimeFormat.EPOCH)) {
      int size = getColumnSize();
      TimeUnit unit = getColumnUnit();
      dateTimeColumnValue = unit.convert(dateTimeColumnValueMS, TimeUnit.MILLISECONDS) / size;
    } else {
      String pattern = getSDFPattern();
      dateTimeColumnValue =
          org.joda.time.format.DateTimeFormat.forPattern(pattern).withZoneUTC().print(dateTimeColumnValueMS);
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
   * @param fromFormat - the format in which the date time column value is expressed
   * @return datetime value in millis
   */
  public Long fromFormatToMillis(Object dateTimeColumnValue) {
    Preconditions.checkNotNull(dateTimeColumnValue);

    Long timeColumnValueMS = 0L;

    TimeFormat timeFormat = getTimeFormat();
    if (timeFormat.equals(TimeFormat.EPOCH)) {
      int size = getColumnSize();
      TimeUnit unit = getColumnUnit();
      timeColumnValueMS = TimeUnit.MILLISECONDS.convert((Long) dateTimeColumnValue * size, unit);
    } else {
      String pattern = getSDFPattern();
      timeColumnValueMS =
          org.joda.time.format.DateTimeFormat.forPattern(pattern).withZoneUTC()
              .parseMillis(String.valueOf(dateTimeColumnValue));
    }

    return timeColumnValueMS;
  }

  /**
   * Check correctness of format of {@link DateTimeFieldSpec}
   * @param format
   * @return
   */
  public static boolean isValidFormat(String format) {

    Preconditions.checkArgument(checkValidFormat(format));

    String[] formatTokens = format.split(COLON_SEPARATOR, MAX_FORMAT_TOKENS);
    Preconditions.checkArgument(
        EnumUtils.isValidEnum(TimeUnit.class, formatTokens[FORMAT_UNIT_POSITION]),
        FORMAT_PATTERN_ERROR_STR);

    return true;
  }

  /**
   * Check correctness of format of {@link DateTimeFieldSpec}, but with TimeUnit extended to
   * {@link DateTimeTransformUnit}
   * @param format
   * @return
   */
  public static boolean isValidDateTimeTransformFormat(String format) {

    Preconditions.checkArgument(checkValidFormat(format));

    String[] formatTokens = format.split(COLON_SEPARATOR, MAX_FORMAT_TOKENS);
    Preconditions.checkArgument(
        EnumUtils.isValidEnum(DateTimeTransformUnit.class, formatTokens[FORMAT_UNIT_POSITION]),
        FORMAT_PATTERN_ERROR_STR);

    return true;
  }

  private static boolean checkValidFormat(String format) {
    Preconditions.checkNotNull(format);

    String[] formatTokens = format.split(COLON_SEPARATOR, MAX_FORMAT_TOKENS);
    Preconditions.checkArgument(
        formatTokens.length == MIN_FORMAT_TOKENS || formatTokens.length == MAX_FORMAT_TOKENS,
        FORMAT_TOKENS_ERROR_STR);

    Preconditions.checkArgument(
        formatTokens[FORMAT_SIZE_POSITION].matches(NUMBER_REGEX),FORMAT_PATTERN_ERROR_STR);
    if (formatTokens.length == MIN_FORMAT_TOKENS) {
      Preconditions.checkArgument(
          formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.EPOCH.toString()),
          TIME_FORMAT_ERROR_STR);
    } else {
      Preconditions.checkArgument(
          formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.SIMPLE_DATE_FORMAT.toString()),
          TIME_FORMAT_ERROR_STR);
      Preconditions.checkNotNull(formatTokens[FORMAT_PATTERN_POSITION]);
    }
    return true;
  }

  public boolean equals(Object o) {
    if (!(o instanceof DateTimeFormatSpec)) {
      return false;
    }
    DateTimeFormatSpec df = (DateTimeFormatSpec) o;
    return Objects.equals(getFormat(), df.getFormat());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getFormat());
  }


}
