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
package com.linkedin.pinot.core.operator.transform;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.EnumUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat;
import com.linkedin.pinot.common.utils.time.DateTimeFieldSpecUtils;

/**
 * Converts the time column value based on the inputFormat, outputFormat and outputGranularity
 */
public class DateTimeConversionEvaluator {

  private static final DateTime EPOCH_START_DATE = new DateTime(0L, DateTimeZone.UTC);

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

  private String inputFormat;
  private String outputFormat;
  private String outputGranularity;
  private DateTimeFormatter inputDateTimeFormatter = null;
  private DateTimeFormatter outputDateTimeFormatter = null;

  public DateTimeConversionEvaluator(String inputFormat, String outputFormat, String outputGranularity) {

    Preconditions.checkArgument(DateTimeFieldSpecUtils.isValidFormat(inputFormat));
    Preconditions.checkArgument(isValidFormat(outputFormat));
    Preconditions.checkArgument(DateTimeFieldSpecUtils.isValidGranularity(outputGranularity));
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.outputGranularity = outputGranularity;
    String inputSDFFormat = DateTimeFieldSpecUtils.getSDFPatternFromFormat(inputFormat);
    if (inputSDFFormat != null) {
      inputDateTimeFormatter = DateTimeFormat.forPattern(inputSDFFormat).withZoneUTC();
    }
    String outputSDFFormat = DateTimeFieldSpecUtils.getSDFPatternFromFormat(outputFormat);
    if (outputSDFFormat != null) {
      outputDateTimeFormatter = DateTimeFormat.forPattern(outputSDFFormat).withZoneUTC();
    }
  }

  /**
   * Transforms the given input value into the new format and bucket
   * @param inputValue
   * @return
   */
  public long transformDateTime(Object inputValue) {
    Long dateTimeColumnValueMS = convertFromInputFormatToMillis(inputValue);
    Long bucketedDateTimevalueMS = bucketDateTimeValueMS(dateTimeColumnValueMS);
    Long dateTimeValueConverted = convertFromMillisToOutputFormat(bucketedDateTimevalueMS);
    return dateTimeValueConverted;
  }

  /**
   * Converts the input from inputFormat to millis
   * @param dateTimeValue
   * @return
   */
  private Long convertFromInputFormatToMillis(Object dateTimeValue) {

    Long timeColumnValueMS = 0L;
    if (dateTimeValue != null) {
      TimeFormat timeFormat = DateTimeFieldSpecUtils.getTimeFormatFromFormat(inputFormat);
      if (timeFormat.equals(TimeFormat.EPOCH)) {
        int size = DateTimeFieldSpecUtils.getColumnSizeFromFormat(inputFormat);
        TimeUnit unit = DateTimeFieldSpecUtils.getColumnUnitFromFormat(inputFormat);
        timeColumnValueMS = TimeUnit.MILLISECONDS.convert((Long) dateTimeValue * size, unit);
      } else {
        timeColumnValueMS = inputDateTimeFormatter.parseMillis(String.valueOf(dateTimeValue));
      }
    }
    return timeColumnValueMS;
  }

  /**
   * Helper method to bucket a given timestamp in millis, into a output granularity
   * @param dateTimeColumnValueMS - value to bucket
   * @return millis value, bucketed in the granularity
   */
  private Long bucketDateTimeValueMS(Long dateTimeColumnValueMS) {
    long granularityMillis = DateTimeFieldSpecUtils.granularityToMillis(outputGranularity);
    long bucketedDateTimeValueMS = (dateTimeColumnValueMS / granularityMillis) * granularityMillis;
    return bucketedDateTimeValueMS;
  }

  /**
   * Given a timestamp in millis, convert it to the output format, while also handling time units
   * such as WEEKS, MONTHS, YEARS
   * <ul>
   * <li>1) given dateTimeColumnValueMS = 1498892400000 and format=1:HOURS:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 416359 (i.e. dateTimeColumnValueMS/(1000*60*60))</li>
   * <li>2) given dateTimeColumnValueMS = 1498892400000 and format=1:WEEKS:EPOCH,
   * dateTimeSpec.fromMillis(1498892400000) = 2478 (i.e. timeColumnValueMS/(1000*60*60*24*7))</li>
   * <li>3) given dateTimeColumnValueMS = 1498892400000 and
   * format=1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd, dateTimeSpec.fromMillis(1498892400000) = 20170701</li>
   * </ul>
   * @param dateTimeValueMillis - date time value in millis to convert
   * @param outputFormat
   * @return
   */
  private Long convertFromMillisToOutputFormat(Long dateTimeValueMillis) {
    Long convertedDateTime = null;
    String[] outputFormatTokens = outputFormat.split(DateTimeFieldSpecUtils.COLON_SEPARATOR);
    int size = DateTimeFieldSpecUtils.getColumnSizeFromFormat(outputFormat);
    DateTimeTransformUnit timeUnit =
        DateTimeTransformUnit.valueOf(outputFormatTokens[DateTimeFieldSpecUtils.FORMAT_UNIT_POSITION]);
    TimeFormat timeFormat = DateTimeFieldSpecUtils.getTimeFormatFromFormat(outputFormat);

    if (timeFormat.equals(TimeFormat.EPOCH)) {
      MutableDateTime inputDateTime = new MutableDateTime(dateTimeValueMillis, DateTimeZone.UTC);
      Interval interval = new Interval(EPOCH_START_DATE, inputDateTime);
      switch (timeUnit) {

      case YEARS:
        convertedDateTime = (long) Years.yearsIn(interval).getYears();
        break;
      case MONTHS:
        convertedDateTime = (long) Months.monthsIn(interval).getMonths();
        break;
      case WEEKS:
        convertedDateTime = (long) Weeks.weeksIn(interval).getWeeks();
        break;
      case DAYS:
        convertedDateTime = (long) Days.daysIn(interval).getDays();
        break;
      case HOURS:
        convertedDateTime = (long) Hours.hoursIn(interval).getHours();
        break;
      case MINUTES:
        convertedDateTime = (long) Minutes.minutesIn(interval).getMinutes();
        break;
      case SECONDS:
        convertedDateTime = (long) Seconds.secondsIn(interval).getSeconds();
        break;
      case MILLISECONDS:
        convertedDateTime = dateTimeValueMillis;
        break;
      default:
        throw new IllegalArgumentException("Illegal argument for time unit: " + timeUnit);
      }
      convertedDateTime = convertedDateTime / size;
    } else {
      convertedDateTime = Long.valueOf(outputDateTimeFormatter.print(dateTimeValueMillis));
    }
    return convertedDateTime;
  }

  /**
   * Check correctness of format of {@link DateTimeFieldSpec}, but with TimeUnit extended to {@link DateTimeTransformUnit}
   * @param format
   * @return
   */
  private boolean isValidFormat(String format) {

    Preconditions.checkNotNull(format);
    String[] formatTokens = format.split(DateTimeFieldSpecUtils.COLON_SEPARATOR, 4);
    Preconditions.checkArgument(formatTokens.length == DateTimeFieldSpecUtils.MIN_FORMAT_TOKENS
        || formatTokens.length == DateTimeFieldSpecUtils.MAX_FORMAT_TOKENS, DateTimeFieldSpecUtils.FORMAT_TOKENS_ERROR_STR);
    Preconditions.checkArgument(formatTokens[DateTimeFieldSpecUtils.FORMAT_SIZE_POSITION].matches(DateTimeFieldSpecUtils.NUMBER_REGEX)
        && EnumUtils.isValidEnum(DateTimeTransformUnit.class, formatTokens[DateTimeFieldSpecUtils.FORMAT_UNIT_POSITION]),
        DateTimeFieldSpecUtils.FORMAT_PATTERN_ERROR_STR);
    if (formatTokens.length == DateTimeFieldSpecUtils.MIN_FORMAT_TOKENS) {
      Preconditions.checkArgument(
          formatTokens[DateTimeFieldSpecUtils.FORMAT_TIMEFORMAT_POSITION].equals(TimeFormat.EPOCH.toString()),
          DateTimeFieldSpecUtils.TIME_FORMAT_ERROR_STR);
    } else {
      Preconditions
          .checkArgument(formatTokens[DateTimeFieldSpecUtils.FORMAT_TIMEFORMAT_POSITION]
              .equals(TimeFormat.SIMPLE_DATE_FORMAT.toString()), DateTimeFieldSpecUtils.TIME_FORMAT_ERROR_STR);
      Preconditions.checkNotNull(formatTokens[DateTimeFieldSpecUtils.FORMAT_PATTERN_POSITION]);
    }
    return true;
  }
}
