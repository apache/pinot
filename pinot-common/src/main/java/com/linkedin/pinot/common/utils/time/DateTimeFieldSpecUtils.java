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
package com.linkedin.pinot.common.utils.time;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.EnumUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.format.DateTimeFormat;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat;

public class DateTimeFieldSpecUtils {

  private static final String FORMAT_TOKENS_ERROR_STR = "format must be of pattern size:timeunit:timeformat(:pattern)";
  private static final String FORMAT_PATTERN_ERROR_STR = "format must be of format [0-9]+:<TimeUnit>:<TimeFormat>(:pattern)";
  private static final String TIME_FORMAT_ERROR_STR =
      "format must be of format [0-9]+:<TimeUnit>:EPOCH or [0-9]+:<TimeUnit>:SIMPLE_DATE_FORMAT:<format>";
  private static final String GRANULARITY_TOKENS_ERROR_STR = "granularity must be of format size:timeunit";
  private static final String GRANULARITY_PATTERN_ERROR_STR = "granularity must be of format [0-9]+:<TimeUnit>";
  private static final String NUMBER_REGEX = "[1-9][0-9]*";

  public static final String COLON_SEPARATOR = ":";

  /* DateTimeFieldSpec format is of format size:timeUnit:timeformat:pattern(if sdf) */
  private static final int FORMAT_SIZE_POSITION = 0;
  private static final int FORMAT_UNIT_POSITION = 1;
  private static final int FORMAT_TIMEFORMAT_POSITION = 2;
  private static final int FORMAT_PATTERN_POSITION = 3;
  private static final int MIN_FORMAT_TOKENS = 3;
  private static final int MAX_FORMAT_TOKENS = 4;

  /* DateTimeFieldSpec granularity is of format size:timeUnit */
  private static final int GRANULARITY_SIZE_POSITION = 0;
  private static final int GRANULARITY_UNIT_POSITION = 1;
  private static final int MAX_GRANULARITY_TOKENS = 2;


  private static final DateTime EPOCH_START_DATE = new DateTime(0L, DateTimeZone.UTC);

  public enum CustomTimeUnit {
    WEEKS,
    MONTHS,
    YEARS
  }

  /**
   * Constructs a dateTimeSpec format, given the components of a format
   * @param columnSize
   * @param columnUnit
   * @param columnTimeFormat
   * @return
   */
  @JsonIgnore
  public static String constructFormat(int columnSize, TimeUnit columnUnit, String columnTimeFormat) {
    Preconditions.checkArgument(columnSize > 0);
    Preconditions.checkNotNull(columnUnit);
    Preconditions.checkNotNull(columnTimeFormat);
    Preconditions.checkArgument(TimeFormat.EPOCH.toString().equals(columnTimeFormat),
        "TimeFormat must be EPOCH if not providing sdf pattern");
    return Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit, columnTimeFormat);
  }


  /**
   * Constructs a dateTimeSpec format, given the components of a format
   * @param columnSize
   * @param columnUnit
   * @param columnTimeFormat
   * @param sdfPattern
   * @return
   */
  @JsonIgnore
  public static String constructFormat(int columnSize, TimeUnit columnUnit, String columnTimeFormat, String sdfPattern) {
    Preconditions.checkArgument(columnSize > 0);
    Preconditions.checkNotNull(columnUnit);
    Preconditions.checkNotNull(columnTimeFormat);
    Preconditions.checkArgument(TimeFormat.SIMPLE_DATE_FORMAT.toString().equals(columnTimeFormat),
        "TimeFormat must be SIMPLE_DATE_FORMAT if providing sdf pattern");
    Preconditions.checkNotNull(sdfPattern);
    return Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit, columnTimeFormat, sdfPattern);
  }

  /**
   * Constructs a dateTimeSpec granularity given the components of a granularity
   * @param columnSize
   * @param columnUnit
   * @return
   */
  @JsonIgnore
  public static String constructGranularity(int columnSize, TimeUnit columnUnit) {
    Preconditions.checkArgument(columnSize > 0);
    Preconditions.checkNotNull(columnUnit);
    return Joiner.on(COLON_SEPARATOR).join(columnSize, columnUnit);
  }

  /**
   * Extracts the column size from the format of a dateTimeSpec
   * (1st token in colon separated format)
   *
   * eg: if format=1:HOURS:EPOCH, will return 1
   * @return
   */
  @JsonIgnore
  public static int getColumnSizeFromFormat(String format) {
    Preconditions.checkArgument(validFormat(format));
    String[] formatTokens = format.split(COLON_SEPARATOR);
    int size = Integer.valueOf(formatTokens[FORMAT_SIZE_POSITION]);
    return size;
  }

  /**
   * Extracts the column unit from the format of a dateTimeSpec
   * (2nd token in colon separated format)
   *
   * eg: if format=5:MINUTES:EPOCH, will return MINUTES
   * @return
   */
  @JsonIgnore
  public static TimeUnit getColumnUnitFromFormat(String format) {
    Preconditions.checkArgument(validFormat(format));
    String[] formatTokens = format.split(COLON_SEPARATOR);
    TimeUnit unit = TimeUnit.valueOf(formatTokens[FORMAT_UNIT_POSITION]);
    return unit;
  }

  /**
   * Extracts the TimeFormat from the format of a dateTimeSpec
   * (3rd token in colon separated format)
   *
   * eg: if format=1:DAYS:EPOCH, will return EPOCH
   * @return
   */
  @JsonIgnore
  public static TimeFormat getTimeFormatFromFormat(String format) {
    Preconditions.checkArgument(validFormat(format));
    String[] formatTokens = format.split(COLON_SEPARATOR);
    TimeFormat timeFormat = TimeFormat.valueOf(formatTokens[FORMAT_TIMEFORMAT_POSITION]);
    return timeFormat;
  }

  /**
   * Extracts the simmple date format pattern from the format of a dateTimeSpec
   * (4th token in colon separated format in case of TimeFormat=SIMPLE_DATE_FORMAT)
   *
   * eg: if format=1:HOURS:EPOCH, will throw exception
   * if format=1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH will return yyyyMMddHH
   * @return
   */
  @JsonIgnore
  public static String getSDFPatternFromFormat(String format) {
    Preconditions.checkArgument(validFormat(format));
    String pattern = null;
    String[] formatTokens = format.split(COLON_SEPARATOR);
    Preconditions.checkArgument(formatTokens.length == MAX_FORMAT_TOKENS, "SDF pattern does not exist for given format");
    Preconditions.checkArgument(formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.SIMPLE_DATE_FORMAT.toString()),
        "third token must be SIMPLE_DATE_FORMAT for an sdf pattern");
    pattern = formatTokens[FORMAT_PATTERN_POSITION];
    return pattern;
  }

  /**
   * Given a timestamp in millis, convert it to the given format
   * eg:
   * 1) given dateTimeColumnValueMS = 1498892400000 and format=1:HOURS:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 416359 (i.e. dateTimeColumnValueMS/(1000*60*60))
   *
   * 2) given dateTimeColumnValueMS = 1498892400000 and format=5:MINUTES:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 4996308 (i.e. timeColumnValueMS/(1000*60*5))
   *
   * 3) given dateTimeColumnValueMS = 1498892400000 and format=1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd,
   * dateTimeSpec.fromMillis(1498892400000) = 20170701
   * @param dateTimeColumnValueMS
   * @param toFormat - the format in which to convert the millis value
   * @return dateTime column value in dateTimeFieldSpec
   */
  @JsonIgnore
  public static Object fromMillisToFormat(Long dateTimeColumnValueMS, String toFormat) {
    Preconditions.checkNotNull(dateTimeColumnValueMS);
    Preconditions.checkArgument(validFormat(toFormat));

    Object dateTimeColumnValue = null;

    TimeFormat timeFormat = getTimeFormatFromFormat(toFormat);

    if (timeFormat.equals(TimeFormat.EPOCH)) {
      int size = getColumnSizeFromFormat(toFormat);
      TimeUnit unit = getColumnUnitFromFormat(toFormat);
      dateTimeColumnValue = unit.convert(dateTimeColumnValueMS, TimeUnit.MILLISECONDS) / size;
    } else {
      String pattern = getSDFPatternFromFormat(toFormat);
      dateTimeColumnValue = DateTimeFormat.forPattern(pattern).withZoneUTC().print(dateTimeColumnValueMS);
    }
    return dateTimeColumnValue;
  }


  /**
   * Convert a time value in a format, to millis
   * eg:
   * 1) given dateTimeColumnValue = 416359 and format=1:HOURS:EPOCH
   * dateTimeSpec.toMillis(416359) = 1498892400000 (i.e. timeColumnValue*60*60*1000)
   *
   * 2) given dateTimeColumnValue = 4996308 and format=5:MINUTES:EPOCH
   * dateTimeSpec.toMillis(4996308) = 1498892400000 (i.e. timeColumnValue*5*60*1000)
   *
   * 3) given dateTimeColumnValue = 20170701 and format=1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd
   * dateTimeSpec.toMillis(20170701) = 1498892400000
   *
   * @param dateTimeColumnValue - datetime Column value to convert to millis
   * @param fromFormat - the format in which the date time column value is expressed
   * @return datetime value in millis
   */
  @JsonIgnore
  public static Long fromFormatToMillis(Object dateTimeColumnValue, String fromFormat) {
    Preconditions.checkNotNull(dateTimeColumnValue);
    Preconditions.checkArgument(validFormat(fromFormat));

    Long timeColumnValueMS = 0L;

    TimeFormat timeFormat = getTimeFormatFromFormat(fromFormat);

    if (timeFormat.equals(TimeFormat.EPOCH)) {
      int size = getColumnSizeFromFormat(fromFormat);
      TimeUnit unit = getColumnUnitFromFormat(fromFormat);
      timeColumnValueMS = TimeUnit.MILLISECONDS.convert((Long) dateTimeColumnValue * size, unit);
    } else {
      String pattern = getSDFPatternFromFormat(fromFormat);
      timeColumnValueMS = DateTimeFormat.forPattern(pattern).withZoneUTC().parseMillis((String) dateTimeColumnValue);
    }

    return timeColumnValueMS;
  }


  /**
   * Convert a granularity to millis
   * eg:
   * 1) granularityToMillis(1:HOURS) = 3600000 (60*60*1000)
   *
   * 2) granularityToMillis(1:MILLISECONDS) = 1
   *
   * 3) granularityToMillis(15:MINUTES) = 900000 (15*60*1000)
   *
   * @param granularity - granularity to convert to millis
   * @return
   */
  @JsonIgnore
  public static Long granularityToMillis(String granularity) {
    Preconditions.checkArgument(validGranularity(granularity));

    long granularityInMillis = 0;

    String[] granularityTokens = granularity.split(COLON_SEPARATOR);
    Preconditions.checkArgument(granularityTokens.length == MAX_GRANULARITY_TOKENS, "granularity must have 2 tokens of format [0-9]+:<TimeUnit>");

    granularityInMillis = TimeUnit.MILLISECONDS.convert(Integer.valueOf(granularityTokens[GRANULARITY_SIZE_POSITION]),
        TimeUnit.valueOf(granularityTokens[GRANULARITY_UNIT_POSITION]));
    return granularityInMillis;
  }

  /**
   * Helper method to bucket a given timestamp in millis, into a bucket granularity
   * @param dateTimeColumnValueMS - value to bucket
   * @param outputGranularity - granularity to bucket into
   * @return millis value, bucketed in the granularity
   */
  public static Long bucketDateTimeValueMS(Long dateTimeColumnValueMS, String outputGranularity) {
    Preconditions.checkNotNull(dateTimeColumnValueMS);
    Preconditions.checkArgument(validGranularity(outputGranularity));

    long granularityMillis = DateTimeFieldSpecUtils.granularityToMillis(outputGranularity);
    long bucketedDateTimeValueMS = (dateTimeColumnValueMS / granularityMillis) * granularityMillis;
    return bucketedDateTimeValueMS;
  }

  /**
   * Check correctness of format of {@link DateTimeFieldSpec}
   * @param format
   * @return
   */
  public static boolean validFormat(String format) {

    Preconditions.checkNotNull(format);
    String[] formatTokens = format.split(COLON_SEPARATOR);
    Preconditions.checkArgument(formatTokens.length == MIN_FORMAT_TOKENS || formatTokens.length == MAX_FORMAT_TOKENS,
        FORMAT_TOKENS_ERROR_STR);
    Preconditions.checkArgument(formatTokens[FORMAT_SIZE_POSITION].matches(NUMBER_REGEX)
        && EnumUtils.isValidEnum(TimeUnit.class, formatTokens[FORMAT_UNIT_POSITION]), FORMAT_PATTERN_ERROR_STR);
    if (formatTokens.length == MIN_FORMAT_TOKENS) {
      Preconditions.checkArgument(formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.EPOCH.toString()),
          TIME_FORMAT_ERROR_STR);
    } else {
      Preconditions.checkArgument(formatTokens[FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.SIMPLE_DATE_FORMAT.toString()),
          TIME_FORMAT_ERROR_STR);
      Preconditions.checkNotNull(formatTokens[FORMAT_PATTERN_POSITION]);
    }
    return true;
  }

  /**
   * Check correctness of granularity of {@link DateTimeFieldSpec}
   * @param granularity
   * @return
   */
  public static boolean validGranularity(String granularity) {
    Preconditions.checkNotNull(granularity);
    String[] granularityTokens = granularity.split(COLON_SEPARATOR);
    Preconditions.checkArgument(granularityTokens.length == MAX_GRANULARITY_TOKENS, GRANULARITY_TOKENS_ERROR_STR);
    Preconditions.checkArgument(granularityTokens[GRANULARITY_SIZE_POSITION].matches(NUMBER_REGEX)
        && EnumUtils.isValidEnum(TimeUnit.class, granularityTokens[GRANULARITY_UNIT_POSITION]), GRANULARITY_PATTERN_ERROR_STR);

    return true;
  }

  /**
   * Given a timestamp in millis, convert it to the format specified, while also handling time units such as WEEKS, MONTHS, YEARS
   * This is a wrapper over the fromMillisToFormat method, which only accepts units in TimeUnits
   * eg:
   * 1) given dateTimeColumnValueMS = 1498892400000 and format=1:HOURS:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 416359 (i.e. dateTimeColumnValueMS/(1000*60*60))
   *
   * 2) given dateTimeColumnValueMS = 1498892400000 and format=1:WEEKS:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 2478 (i.e. timeColumnValueMS/(1000*60*60*24*7))
   *
   * 3) given dateTimeColumnValueMS = 1498892400000 and format=1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd,
   * dateTimeSpec.fromMillis(1498892400000) = 20170701
   * @param dateTimeValueMillis - date time value in millis to convert
   * @param outputFormat
   * @return
   */
  public static Long convertMillisToCustomFormat(Long dateTimeValueMillis, String outputFormat) {
    Preconditions.checkNotNull(dateTimeValueMillis);
    Preconditions.checkNotNull(outputFormat);

    Long convertedDateTime = null;
    String timeUnit = outputFormat.split(COLON_SEPARATOR)[FORMAT_UNIT_POSITION];

    if (EnumUtils.isValidEnum(TimeUnit.class, timeUnit)) {
      // fromMillisToFormat returns String for SIMPLE_DATE_FORMAT and Long for EPOCH.
      // the transform UDFs can only handle Long
      convertedDateTime =
          Long.valueOf(String.valueOf(DateTimeFieldSpecUtils.fromMillisToFormat(dateTimeValueMillis, outputFormat)));

    } else if (EnumUtils.isValidEnum(CustomTimeUnit.class, timeUnit)) {
      MutableDateTime inputDateTime = new MutableDateTime(dateTimeValueMillis, DateTimeZone.UTC);
      switch (CustomTimeUnit.valueOf(timeUnit)) {
        case WEEKS:
          convertedDateTime = (long) Weeks.weeksBetween(EPOCH_START_DATE, inputDateTime).getWeeks();
          break;

        case MONTHS:
          convertedDateTime = (long) Months.monthsBetween(EPOCH_START_DATE, inputDateTime).getMonths();
          break;

        case YEARS:
          convertedDateTime = (long) Years.yearsBetween(EPOCH_START_DATE, inputDateTime).getYears();
          break;

        default:
          throw new IllegalArgumentException("Illegal argument for time unit: " + timeUnit);
      }
    } else {
      throw new IllegalArgumentException("Illegal argument for time unit: " + timeUnit);
    }

    return convertedDateTime;
  }


}
