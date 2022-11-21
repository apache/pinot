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
package org.apache.pinot.common.function.scalar;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.function.DateTimePatternHandler;
import org.apache.pinot.common.function.DateTimeUtils;
import org.apache.pinot.common.function.TimeZoneKey;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

/**
 * Inbuilt date time related transform functions
 *
 *   NOTE:
 *   <code>toEpochXXXBucket</code> methods are only needed to convert from TimeFieldSpec to DateTimeFieldSpec, to
 *   maintain the backward compatibility.
 *   Practically, we should only need the <code>toEpochXXXRounded</code> methods.
 *   Use of <code>toEpochXXXBucket</code> bucket functions is discouraged unless you know what you are doing -
 *   (e.g. 5-minutes-since-epoch does not make sense to someone looking at the timestamp, or writing queries. instead,
 *   Millis-since-epoch rounded to 5 minutes makes a lot more sense)
 *
 *   An example timeFieldSpec that needs the bucketing function:
 *   <code>
 *     "timeFieldSpec": {
 *     "incomingGranularitySpec": {
 *       "name": "incoming",
 *       "dataType": "LONG",
 *       "timeType": "MILLISECONDS"
 *     },
 *     "outgoingGranularitySpec": {
 *        "name": "outgoing",
 *        "dataType": "LONG",
 *        "timeType": "MINUTES",
 *        "timeSize": 5
 *      }
 *   }
 *   </code>
 *   An equivalent dateTimeFieldSpec is
 *   <code>
 *     "dateTimeFieldSpecs": [{
 *       "name": "outgoing",
 *       "dataType": "LONG",
 *       "format": "5:MINUTES:EPOCH",
 *       "granularity": "5:MINUTES",
 *       "transformFunction": "toEpochMinutesBucket(incoming, 5)"
 *     }]
 *   </code>
 */
public class DateTimeFunctions {
  private DateTimeFunctions() {
  }

  /**
   * Convert epoch millis to epoch seconds
   */
  @ScalarFunction
  public static long toEpochSeconds(long millis) {
    return TimeUnit.MILLISECONDS.toSeconds(millis);
  }

  /**
   * Convert epoch millis to epoch minutes
   */
  @ScalarFunction
  public static long toEpochMinutes(long millis) {
    return TimeUnit.MILLISECONDS.toMinutes(millis);
  }

  /**
   * Convert epoch millis to epoch hours
   */
  @ScalarFunction
  public static long toEpochHours(long millis) {
    return TimeUnit.MILLISECONDS.toHours(millis);
  }

  /**
   * Convert epoch millis to epoch days
   */
  @ScalarFunction
  public static long toEpochDays(long millis) {
    return TimeUnit.MILLISECONDS.toDays(millis);
  }

  /**
   * Convert epoch millis to epoch seconds, round to nearest rounding bucket
   */
  @ScalarFunction
  public static long toEpochSecondsRounded(long millis, long roundToNearest) {
    return (TimeUnit.MILLISECONDS.toSeconds(millis) / roundToNearest) * roundToNearest;
  }

  /**
   * Convert epoch millis to epoch minutes, round to nearest rounding bucket
   */
  @ScalarFunction
  public static long toEpochMinutesRounded(long millis, long roundToNearest) {
    return (TimeUnit.MILLISECONDS.toMinutes(millis) / roundToNearest) * roundToNearest;
  }

  /**
   * Convert epoch millis to epoch hours, round to nearest rounding bucket
   */
  @ScalarFunction
  public static long toEpochHoursRounded(long millis, long roundToNearest) {
    return (TimeUnit.MILLISECONDS.toHours(millis) / roundToNearest) * roundToNearest;
  }

  /**
   * Convert epoch millis to epoch days, round to nearest rounding bucket
   */
  @ScalarFunction
  public static long toEpochDaysRounded(long millis, long roundToNearest) {
    return (TimeUnit.MILLISECONDS.toDays(millis) / roundToNearest) * roundToNearest;
  }

  /**
   * Convert epoch millis to epoch seconds, divided by given bucket, to get nSecondsSinceEpoch
   */
  @ScalarFunction
  public static long toEpochSecondsBucket(long millis, long bucket) {
    return TimeUnit.MILLISECONDS.toSeconds(millis) / bucket;
  }

  /**
   * Convert epoch millis to epoch minutes, divided by given bucket, to get nMinutesSinceEpoch
   */
  @ScalarFunction
  public static long toEpochMinutesBucket(long millis, long bucket) {
    return TimeUnit.MILLISECONDS.toMinutes(millis) / bucket;
  }

  /**
   * Convert epoch millis to epoch hours, divided by given bucket, to get nHoursSinceEpoch
   */
  @ScalarFunction
  public static long toEpochHoursBucket(long millis, long bucket) {
    return TimeUnit.MILLISECONDS.toHours(millis) / bucket;
  }

  /**
   * Convert epoch millis to epoch days, divided by given bucket, to get nDaysSinceEpoch
   */
  @ScalarFunction
  public static long toEpochDaysBucket(long millis, long bucket) {
    return TimeUnit.MILLISECONDS.toDays(millis) / bucket;
  }

  /**
   * Converts epoch seconds to epoch millis
   */
  @ScalarFunction
  public static long fromEpochSeconds(long seconds) {
    return TimeUnit.SECONDS.toMillis(seconds);
  }

  /**
   * Converts epoch minutes to epoch millis
   */
  @ScalarFunction
  public static long fromEpochMinutes(long minutes) {
    return TimeUnit.MINUTES.toMillis(minutes);
  }

  /**
   * Converts epoch hours to epoch millis
   */
  @ScalarFunction
  public static long fromEpochHours(long hours) {
    return TimeUnit.HOURS.toMillis(hours);
  }

  /**
   * Converts epoch days to epoch millis
   */
  @ScalarFunction
  public static long fromEpochDays(long days) {
    return TimeUnit.DAYS.toMillis(days);
  }

  /**
   * Converts nSecondsSinceEpoch (seconds that have been divided by a bucket), to epoch millis
   */
  @ScalarFunction
  public static long fromEpochSecondsBucket(long seconds, long bucket) {
    return TimeUnit.SECONDS.toMillis(seconds * bucket);
  }

  /**
   * Converts nMinutesSinceEpoch (minutes that have been divided by a bucket), to epoch millis
   */
  @ScalarFunction
  public static long fromEpochMinutesBucket(long minutes, long bucket) {
    return TimeUnit.MINUTES.toMillis(minutes * bucket);
  }

  /**
   * Converts nHoursSinceEpoch (hours that have been divided by a bucket), to epoch millis
   */
  @ScalarFunction
  public static long fromEpochHoursBucket(long hours, long bucket) {
    return TimeUnit.HOURS.toMillis(hours * bucket);
  }

  /**
   * Converts nDaysSinceEpoch (days that have been divided by a bucket), to epoch millis
   */
  @ScalarFunction
  public static long fromEpochDaysBucket(long days, long bucket) {
    return TimeUnit.DAYS.toMillis(days * bucket);
  }

  /**
   * Converts epoch millis to Timestamp
   */
  @ScalarFunction
  public static Timestamp toTimestamp(long millis) {
    return new Timestamp(millis);
  }

  /**
   * Converts Timestamp to epoch millis
   */
  @ScalarFunction
  public static long fromTimestamp(Timestamp timestamp) {
    return timestamp.getTime();
  }

  /**
   * Converts epoch millis to DateTime string represented by pattern
   */
  @ScalarFunction
  public static String toDateTime(long millis, String pattern) {
    return DateTimePatternHandler.parseEpochMillisToDateTimeString(millis, pattern);
  }

  /**
   * Converts epoch millis to DateTime string represented by pattern and the time zone id.
   */
  @ScalarFunction
  public static String toDateTime(long millis, String pattern, String timezoneId) {
    return DateTimePatternHandler.parseEpochMillisToDateTimeString(millis, pattern, timezoneId);
  }

  /**
   * Converts DateTime string represented by pattern to epoch millis
   */
  @ScalarFunction
  public static long fromDateTime(String dateTimeString, String pattern) {
    return DateTimePatternHandler.parseDateTimeStringToEpochMillis(dateTimeString, pattern);
  }

  /**
   * Converts DateTime string represented by pattern to epoch millis
   */
  @ScalarFunction
  public static long fromDateTime(String dateTimeString, String pattern, String timeZoneId) {
    return DateTimePatternHandler.parseDateTimeStringToEpochMillis(dateTimeString, pattern, timeZoneId);
  }

  /**
   * Round the given time value to nearest multiple
   * @return the original value but rounded to the nearest multiple of @param roundToNearest
   */
  @ScalarFunction
  public static long round(long timeValue, long roundToNearest) {
    return (timeValue / roundToNearest) * roundToNearest;
  }

  /**
   * Return current time as epoch millis
   * TODO: Consider changing the return type to Timestamp
   */
  @ScalarFunction
  public static long now() {
    return System.currentTimeMillis();
  }

  /**
   * Return time as epoch millis before the given period (in ISO-8601 duration format).
   * Examples:
   *           "PT20.345S" -- parses as "20.345 seconds"
   *           "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
   *           "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
   *           "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
   *           "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
   *           "P-6H3M"    -- parses as "-6 hours and +3 minutes"
   *           "-P6H3M"    -- parses as "-6 hours and -3 minutes"
   *           "-P-6H+3M"  -- parses as "+6 hours and -3 minutes"
   */
  @ScalarFunction
  public static long ago(String periodString) {
    Duration period = Duration.parse(periodString);
    return System.currentTimeMillis() - period.toMillis();
  }

  /**
   * The {@code timezoneId} for the following methods must be of a Joda-Time format:
   * https://www.joda.org/joda-time/timezones.html
   */

  /**
   * Returns the hour of the time zone offset.
   */
  @ScalarFunction
  public static int timezoneHour(String timezoneId) {
    return new DateTime(DateTimeZone.forID(timezoneId).getOffset(null), DateTimeZone.UTC).getHourOfDay();
  }

  /**
   * Returns the minute of the time zone offset.
   */
  @ScalarFunction
  public static int timezoneMinute(String timezoneId) {
    return new DateTime(DateTimeZone.forID(timezoneId).getOffset(null), DateTimeZone.UTC).getMinuteOfHour();
  }

  /**
   * Returns the year from the given epoch millis in UTC timezone.
   */
  @ScalarFunction
  public static int year(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getYear();
  }

  /**
   * Returns the year from the given epoch millis and timezone id.
   */
  @ScalarFunction
  public static int year(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getYear();
  }

  /**
   * Returns the year of the ISO week from the given epoch millis in UTC timezone.
   */
  @ScalarFunction(names = {"yearOfWeek", "year_of_week", "yow"})
  public static int yearOfWeek(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getWeekyear();
  }

  /**
   * Returns the year of the ISO week from the given epoch millis and timezone id.
   */
  @ScalarFunction(names = {"yearOfWeek", "year_of_week", "yow"})
  public static int yearOfWeek(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getWeekyear();
  }

  /**
   * Returns the quarter of the year from the given epoch millis in UTC timezone. The value ranges from 1 to 4.
   */
  @ScalarFunction
  public static int quarter(long millis) {
    return (monthOfYear(millis) - 1) / 3 + 1;
  }

  /**
   * Returns the quarter of the year from the given epoch millis and timezone id. The value ranges from 1 to 4.
   */
  @ScalarFunction
  public static int quarter(long millis, String timezoneId) {
    return (monthOfYear(millis, timezoneId) - 1) / 3 + 1;
  }

  /**
   * Returns the month of the year from the given epoch millis in UTC timezone. The value ranges from 1 to 12.
   */
  @ScalarFunction(names = {"month", "month_of_year", "monthOfYear"})
  public static int monthOfYear(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getMonthOfYear();
  }

  /**
   * Returns the month of the year from the given epoch millis and timezone id. The value ranges from 1 to 12.
   */
  @ScalarFunction(names = {"month", "month_of_year", "monthOfYear"})
  public static int monthOfYear(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getMonthOfYear();
  }

  /**
   * Returns the ISO week of the year from the given epoch millis in UTC timezone.The value ranges from 1 to 53.
   */
  @ScalarFunction(names = {"weekOfYear", "week_of_year", "week"})
  public static int weekOfYear(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getWeekOfWeekyear();
  }

  /**
   * Returns the ISO week of the year from the given epoch millis and timezone id. The value ranges from 1 to 53.
   */
  @ScalarFunction(names = {"weekOfYear", "week_of_year", "week"})
  public static int weekOfYear(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getWeekOfWeekyear();
  }

  /**
   * Returns the day of the year from the given epoch millis in UTC timezone. The value ranges from 1 to 366.
   */
  @ScalarFunction(names = {"dayOfYear", "day_of_year", "doy"})
  public static int dayOfYear(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getDayOfYear();
  }

  /**
   * Returns the day of the year from the given epoch millis and timezone id. The value ranges from 1 to 366.
   */
  @ScalarFunction(names = {"dayOfYear", "day_of_year", "doy"})
  public static int dayOfYear(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getDayOfYear();
  }

  /**
   * Returns the day of the month from the given epoch millis in UTC timezone. The value ranges from 1 to 31.
   */
  @ScalarFunction(names = {"day", "dayOfMonth", "day_of_month"})
  public static int dayOfMonth(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getDayOfMonth();
  }

  /**
   * Returns the day of the month from the given epoch millis and timezone id. The value ranges from 1 to 31.
   */
  @ScalarFunction(names = {"day", "dayOfMonth", "day_of_month"})
  public static int dayOfMonth(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getDayOfMonth();
  }

  /**
   * Returns the day of the week from the given epoch millis in UTC timezone. The value ranges from 1 (Monday) to 7
   * (Sunday).
   */
  @ScalarFunction(names = {"dayOfWeek", "day_of_week", "dow"})
  public static int dayOfWeek(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getDayOfWeek();
  }

  /**
   * Returns the day of the week from the given epoch millis and timezone id. The value ranges from 1 (Monday) to 7
   * (Sunday).
   */
  @ScalarFunction(names = {"dayOfWeek", "day_of_week", "dow"})
  public static int dayOfWeek(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getDayOfWeek();
  }

  /**
   * Returns the hour of the day from the given epoch millis in UTC timezone. The value ranges from 0 to 23.
   */
  @ScalarFunction
  public static int hour(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getHourOfDay();
  }

  /**
   * Returns the hour of the day from the given epoch millis and timezone id. The value ranges from 0 to 23.
   */
  @ScalarFunction
  public static int hour(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getHourOfDay();
  }

  /**
   * Returns the minute of the hour from the given epoch millis in UTC timezone. The value ranges from 0 to 59.
   */
  @ScalarFunction
  public static int minute(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getMinuteOfHour();
  }

  /**
   * Returns the minute of the hour from the given epoch millis and timezone id. The value ranges from 0 to 59.
   */
  @ScalarFunction
  public static int minute(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getMinuteOfHour();
  }

  /**
   * Returns the second of the minute from the given epoch millis in UTC timezone. The value ranges from 0 to 59.
   */
  @ScalarFunction
  public static int second(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getSecondOfMinute();
  }

  /**
   * Returns the second of the minute from the given epoch millis and timezone id. The value ranges from 0 to 59.
   */
  @ScalarFunction
  public static int second(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getSecondOfMinute();
  }

  /**
   * Returns the millisecond of the second from the given epoch millis in UTC timezone. The value ranges from 0 to 999.
   */
  @ScalarFunction
  public static int millisecond(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getMillisOfSecond();
  }

  /**
   * Returns the millisecond of the second from the given epoch millis and timezone id. The value ranges from 0 to 999.
   */
  @ScalarFunction
  public static int millisecond(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getMillisOfSecond();
  }

  /**
   * The sql compatible date_trunc function for epoch time.
   *
   * @param unit truncate to unit (millisecond, second, minute, hour, day, week, month, quarter, year)
   * @param timeValue value to truncate
   * @return truncated timeValue in TimeUnit.MILLISECONDS
   */
  @ScalarFunction
  public static long dateTrunc(String unit, long timeValue) {
    return dateTrunc(unit, timeValue, TimeUnit.MILLISECONDS, ISOChronology.getInstanceUTC(), TimeUnit.MILLISECONDS);
  }

  /**
   * The sql compatible date_trunc function for epoch time.
   *
   * @param unit truncate to unit (millisecond, second, minute, hour, day, week, month, quarter, year)
   * @param timeValue value to truncate
   * @param inputTimeUnitStr TimeUnit of value, expressed in Java's joda TimeUnit
   * @return truncated timeValue in same TimeUnit as the input
   */
  @ScalarFunction
  public static long dateTrunc(String unit, long timeValue, String inputTimeUnitStr) {
    TimeUnit inputTimeUnit = TimeUnit.valueOf(inputTimeUnitStr);
    return dateTrunc(unit, timeValue, inputTimeUnit, ISOChronology.getInstanceUTC(), inputTimeUnit);
  }

  /**
   * The sql compatible date_trunc function for epoch time.
   *
   * @param unit truncate to unit (millisecond, second, minute, hour, day, week, month, quarter, year)
   * @param timeValue value to truncate
   * @param inputTimeUnitStr TimeUnit of value, expressed in Java's joda TimeUnit
   * @param timeZone timezone of the input
   * @return truncated timeValue in same TimeUnit as the input
   */
  @ScalarFunction
  public static long dateTrunc(String unit, long timeValue, String inputTimeUnitStr, String timeZone) {
    TimeUnit inputTimeUnit = TimeUnit.valueOf(inputTimeUnitStr);
    return dateTrunc(unit, timeValue, inputTimeUnit, DateTimeUtils.getChronology(TimeZoneKey.getTimeZoneKey(timeZone)),
        inputTimeUnit);
  }

  /**
   * The sql compatible date_trunc function for epoch time.
   *
   * @param unit truncate to unit (millisecond, second, minute, hour, day, week, month, quarter, year)
   * @param timeValue value to truncate
   * @param inputTimeUnitStr TimeUnit of value, expressed in Java's joda TimeUnit
   * @param timeZone timezone of the input
   * @param outputTimeUnitStr TimeUnit to convert the output to
   * @return truncated timeValue
   *
   */
  @ScalarFunction
  public static long dateTrunc(String unit, long timeValue, String inputTimeUnitStr, String timeZone,
      String outputTimeUnitStr) {
    return dateTrunc(unit, timeValue, TimeUnit.valueOf(inputTimeUnitStr),
        DateTimeUtils.getChronology(TimeZoneKey.getTimeZoneKey(timeZone)), TimeUnit.valueOf(outputTimeUnitStr));
  }

  private static long dateTrunc(String unit, long timeValue, TimeUnit inputTimeUnit, ISOChronology chronology,
      TimeUnit outputTimeUnit) {
    return outputTimeUnit.convert(DateTimeUtils.getTimestampField(chronology, unit)
        .roundFloor(TimeUnit.MILLISECONDS.convert(timeValue, inputTimeUnit)), TimeUnit.MILLISECONDS);
  }

  /**
   * Add a time period to the provided timestamp.
   * e.g. timestampAdd('days', 10, NOW()) will add 10 days to the current timestamp and return the value
   * @param unit the timeunit of the period to add. e.g. milliseconds, seconds, days, year
   * @param interval value of the period to add.
   * @param timestamp
   * @return
   */
  @ScalarFunction(names = {"timestampAdd", "dateAdd"})
  public static long timestampAdd(String unit, long interval, long timestamp) {
    ISOChronology chronology = ISOChronology.getInstanceUTC();
    long millis = DateTimeUtils.getTimestampField(chronology, unit).add(timestamp, interval);
    return millis;
  }

  /**
   * Get difference between two timestamps and return the result in the specified timeunit.
   * e.g. timestampDiff('days', ago('10D'), ago('2D')) will return 8 i.e. 8 days
   * @param unit
   * @param timestamp1
   * @param timestamp2
   * @return
   */
  @ScalarFunction(names = {"timestampDiff", "dateDiff"})
  public static long timestampDiff(String unit, long timestamp1, long timestamp2) {
    ISOChronology chronology = ISOChronology.getInstanceUTC();
    return DateTimeUtils.getTimestampField(chronology, unit).getDifferenceAsLong(timestamp2, timestamp1);
  }
}
