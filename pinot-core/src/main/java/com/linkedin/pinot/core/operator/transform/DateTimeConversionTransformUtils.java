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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.DateTimeFieldSpec.TimeFormat;
import com.linkedin.pinot.common.utils.time.DateTimeFieldSpecUtils;

public class DateTimeConversionTransformUtils {

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

  /**
   * Given a timestamp in millis, convert it to the format specified, while also handling time units
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
  public static Long convertMillisToFormat(Long dateTimeValueMillis, String outputFormat) {
    Preconditions.checkNotNull(dateTimeValueMillis);
    Preconditions.checkNotNull(outputFormat);

    Long convertedDateTime = null;
    String[] outputFormatTokens = outputFormat.split(DateTimeFieldSpecUtils.COLON_SEPARATOR);
    int size = Integer.valueOf(outputFormatTokens[DateTimeFieldSpecUtils.FORMAT_SIZE_POSITION]);
    DateTimeTransformUnit timeUnit =
        DateTimeTransformUnit
            .valueOf(outputFormatTokens[DateTimeFieldSpecUtils.FORMAT_UNIT_POSITION]);
    TimeFormat timeFormat =
        TimeFormat.valueOf(outputFormatTokens[DateTimeFieldSpecUtils.FORMAT_TIMEFORMAT_POSITION]);

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
      String pattern = outputFormatTokens[DateTimeFieldSpecUtils.FORMAT_PATTERN_POSITION];
      convertedDateTime =
          Long.valueOf(DateTimeFormat.forPattern(pattern).withZoneUTC().print(dateTimeValueMillis));
    }
    return convertedDateTime;
  }

}
