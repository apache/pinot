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

  @ScalarFunction
  public static long[] toEpochSecondsMV(long[] millis) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochSeconds(millis[i]);
    }
    return results;
  }

  /**
   * Convert epoch millis to epoch minutes
   */
  @ScalarFunction
  public static long toEpochMinutes(long millis) {
    return TimeUnit.MILLISECONDS.toMinutes(millis);
  }

  @ScalarFunction
  public static long[] toEpochMinutesMV(long[] millis) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochMinutes(millis[i]);
    }
    return results;
  }

  /**
   * Convert epoch millis to epoch hours
   */
  @ScalarFunction
  public static long toEpochHours(long millis) {
    return TimeUnit.MILLISECONDS.toHours(millis);
  }

  @ScalarFunction
  public static long[] toEpochHoursMV(long[] millis) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochHours(millis[i]);
    }
    return results;
  }

  /**
   * Convert epoch millis to epoch days
   */
  @ScalarFunction
  public static long toEpochDays(long millis) {
    return TimeUnit.MILLISECONDS.toDays(millis);
  }

  @ScalarFunction
  public static long[] toEpochDaysMV(long[] millis) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochDays(millis[i]);
    }
    return results;
  }

  /**
   * Convert epoch millis to epoch seconds, round to nearest rounding bucket
   */
  @ScalarFunction
  public static long toEpochSecondsRounded(long millis, long roundToNearest) {
    return (TimeUnit.MILLISECONDS.toSeconds(millis) / roundToNearest) * roundToNearest;
  }

  @ScalarFunction
  public static long[] toEpochSecondsRoundedMV(long[] millis, long roundToNearest) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochSecondsRounded(millis[i], roundToNearest);
    }
    return results;
  }


  /**
   * Convert epoch millis to epoch minutes, round to nearest rounding bucket
   */
  @ScalarFunction
  public static long toEpochMinutesRounded(long millis, long roundToNearest) {
    return (TimeUnit.MILLISECONDS.toMinutes(millis) / roundToNearest) * roundToNearest;
  }

  @ScalarFunction
  public static long[] toEpochMinutesRoundedMV(long[] millis, long roundToNearest) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochMinutesRounded(millis[i], roundToNearest);
    }
    return results;
  }


  /**
   * Convert epoch millis to epoch hours, round to nearest rounding bucket
   */
  @ScalarFunction
  public static long toEpochHoursRounded(long millis, long roundToNearest) {
    return (TimeUnit.MILLISECONDS.toHours(millis) / roundToNearest) * roundToNearest;
  }

  @ScalarFunction
  public static long[] toEpochHoursRoundedMV(long[] millis, long roundToNearest) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochHoursRounded(millis[i], roundToNearest);
    }
    return results;
  }


  /**
   * Convert epoch millis to epoch days, round to nearest rounding bucket
   */
  @ScalarFunction
  public static long toEpochDaysRounded(long millis, long roundToNearest) {
    return (TimeUnit.MILLISECONDS.toDays(millis) / roundToNearest) * roundToNearest;
  }

  @ScalarFunction
  public static long[] toEpochDaysRoundedMV(long[] millis, long roundToNearest) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochDaysRounded(millis[i], roundToNearest);
    }
    return results;
  }


  /**
   * Convert epoch millis to epoch seconds, divided by given bucket, to get nSecondsSinceEpoch
   */
  @ScalarFunction
  public static long toEpochSecondsBucket(long millis, long bucket) {
    return TimeUnit.MILLISECONDS.toSeconds(millis) / bucket;
  }

  @ScalarFunction
  public static long[] toEpochSecondsBucketMV(long[] millis, long bucket) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochSecondsBucket(millis[i], bucket);
    }
    return results;
  }


  /**
   * Convert epoch millis to epoch minutes, divided by given bucket, to get nMinutesSinceEpoch
   */
  @ScalarFunction
  public static long toEpochMinutesBucket(long millis, long bucket) {
    return TimeUnit.MILLISECONDS.toMinutes(millis) / bucket;
  }

  @ScalarFunction
  public static long[] toEpochMinutesBucketMV(long[] millis, long bucket) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochMinutesBucket(millis[i], bucket);
    }
    return results;
  }


  /**
   * Convert epoch millis to epoch hours, divided by given bucket, to get nHoursSinceEpoch
   */
  @ScalarFunction
  public static long toEpochHoursBucket(long millis, long bucket) {
    return TimeUnit.MILLISECONDS.toHours(millis) / bucket;
  }

  @ScalarFunction
  public static long[] toEpochHoursBucketMV(long[] millis, long bucket) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochHoursBucket(millis[i], bucket);
    }
    return results;
  }


  /**
   * Convert epoch millis to epoch days, divided by given bucket, to get nDaysSinceEpoch
   */
  @ScalarFunction
  public static long toEpochDaysBucket(long millis, long bucket) {
    return TimeUnit.MILLISECONDS.toDays(millis) / bucket;
  }

  @ScalarFunction
  public static long[] toEpochDaysBucketMV(long[] millis, long bucket) {
    long[] results = new long[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toEpochDaysBucket(millis[i], bucket);
    }
    return results;
  }


  /**
   * Converts epoch seconds to epoch millis
   */
  @ScalarFunction
  public static long fromEpochSeconds(long seconds) {
    return TimeUnit.SECONDS.toMillis(seconds);
  }

  @ScalarFunction
  public static long[] fromEpochSecondsMV(long[] seconds) {
    long[] results = new long[seconds.length];
    for (int i = 0; i < seconds.length; i++) {
      results[i] = fromEpochSeconds(seconds[i]);
    }
    return results;
  }

  /**
   * Converts epoch minutes to epoch millis
   */
  @ScalarFunction
  public static long fromEpochMinutes(long minutes) {
    return TimeUnit.MINUTES.toMillis(minutes);
  }

  @ScalarFunction
  public static long[] fromEpochMinutesMV(long[] minutes) {
    long[] results = new long[minutes.length];
    for (int i = 0; i < minutes.length; i++) {
      results[i] = fromEpochMinutes(minutes[i]);
    }
    return results;
  }

  /**
   * Converts epoch hours to epoch millis
   */
  @ScalarFunction
  public static long fromEpochHours(long hours) {
    return TimeUnit.HOURS.toMillis(hours);
  }

  @ScalarFunction
  public static long[] fromEpochHoursMV(long[] hours) {
    long[] results = new long[hours.length];
    for (int i = 0; i < hours.length; i++) {
      results[i] = fromEpochHours(hours[i]);
    }
    return results;
  }

  /**
   * Converts epoch days to epoch millis
   */
  @ScalarFunction
  public static long fromEpochDays(long days) {
    return TimeUnit.DAYS.toMillis(days);
  }

  @ScalarFunction
  public static long[] fromEpochDaysMV(long[] days) {
    long[] results = new long[days.length];
    for (int i = 0; i < days.length; i++) {
      results[i] = fromEpochDays(days[i]);
    }
    return results;
  }

  /**
   * Converts nSecondsSinceEpoch (seconds that have been divided by a bucket), to epoch millis
   */
  @ScalarFunction
  public static long fromEpochSecondsBucket(long seconds, long bucket) {
    return TimeUnit.SECONDS.toMillis(seconds * bucket);
  }

  @ScalarFunction
  public static long[] fromEpochSecondsBucketMV(long[] seconds, long bucket) {
    long[] results = new long[seconds.length];
    for (int i = 0; i < seconds.length; i++) {
      results[i] = fromEpochSecondsBucket(seconds[i], bucket);
    }
    return results;
  }

  /**
   * Converts nMinutesSinceEpoch (minutes that have been divided by a bucket), to epoch millis
   */
  @ScalarFunction
  public static long fromEpochMinutesBucket(long minutes, long bucket) {
    return TimeUnit.MINUTES.toMillis(minutes * bucket);
  }

  @ScalarFunction
  public static long[] fromEpochMinutesBucketMV(long[] minutes, long bucket) {
    long[] results = new long[minutes.length];
    for (int i = 0; i < minutes.length; i++) {
      results[i] = fromEpochMinutesBucket(minutes[i], bucket);
    }
    return results;
  }

  /**
   * Converts nHoursSinceEpoch (hours that have been divided by a bucket), to epoch millis
   */
  @ScalarFunction
  public static long fromEpochHoursBucket(long hours, long bucket) {
    return TimeUnit.HOURS.toMillis(hours * bucket);
  }

  @ScalarFunction
  public static long[] fromEpochHoursBucketMV(long[] hours, long bucket) {
    long[] results = new long[hours.length];
    for (int i = 0; i < hours.length; i++) {
      results[i] = fromEpochHoursBucket(hours[i], bucket);
    }
    return results;
  }

  /**
   * Converts nDaysSinceEpoch (days that have been divided by a bucket), to epoch millis
   */
  @ScalarFunction
  public static long fromEpochDaysBucket(long days, long bucket) {
    return TimeUnit.DAYS.toMillis(days * bucket);
  }

  @ScalarFunction
  public static long[] fromEpochDaysBucketMV(long[] days, long bucket) {
    long[] results = new long[days.length];
    for (int i = 0; i < days.length; i++) {
      results[i] = fromEpochDaysBucket(days[i], bucket);
    }
    return results;
  }

  /**
   * Converts epoch millis to Timestamp
   */
  @ScalarFunction
  public static Timestamp toTimestamp(long millis) {
    return new Timestamp(millis);
  }

  @ScalarFunction
  public static Timestamp[] toTimestampMV(long[] millis) {
    Timestamp[] results = new Timestamp[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toTimestamp(millis[i]);
    }
    return results;
  }

  /**
   * Converts Timestamp to epoch millis
   */
  @ScalarFunction
  public static long fromTimestamp(Timestamp timestamp) {
    return timestamp.getTime();
  }

  @ScalarFunction
  public static long[] fromTimestampMV(Timestamp[] timestamp) {
    long[] results = new long[timestamp.length];
    for (int i = 0; i < timestamp.length; i++) {
      results[i] = fromTimestamp(timestamp[i]);
    }
    return results;
  }

  /**
   * Converts epoch millis to DateTime string represented by pattern
   */
  @ScalarFunction
  public static String toDateTime(long millis, String pattern) {
    return DateTimePatternHandler.parseEpochMillisToDateTimeString(millis, pattern);
  }

  @ScalarFunction
  public static String[] toDateTimeMV(long[] millis, String pattern) {
    String[] results = new String[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toDateTime(millis[i], pattern);
    }
    return results;
  }

  /**
   * Converts epoch millis to DateTime string represented by pattern and the time zone id.
   */
  @ScalarFunction
  public static String toDateTime(long millis, String pattern, String timezoneId) {
    return DateTimePatternHandler.parseEpochMillisToDateTimeString(millis, pattern, timezoneId);
  }

  @ScalarFunction
  public static String[] toDateTimeMV(long[] millis, String pattern, String timezoneId) {
    String[] results = new String[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = toDateTime(millis[i], pattern, timezoneId);
    }
    return results;
  }

  /**
   * Converts DateTime string represented by pattern to epoch millis
   */
  @ScalarFunction
  public static long fromDateTime(String dateTimeString, String pattern) {
    return DateTimePatternHandler.parseDateTimeStringToEpochMillis(dateTimeString, pattern);
  }

  @ScalarFunction
  public static long[] fromDateTimeMV(String[] dateTimeString, String pattern) {
    long[] results = new long[dateTimeString.length];
    for (int i = 0; i < dateTimeString.length; i++) {
      results[i] = fromDateTime(dateTimeString[i], pattern);
    }
    return results;
  }

  /**
   * Converts DateTime string represented by pattern to epoch millis
   */
  @ScalarFunction
  public static long fromDateTime(String dateTimeString, String pattern, String timeZoneId) {
    return DateTimePatternHandler.parseDateTimeStringToEpochMillis(dateTimeString, pattern, timeZoneId);
  }

  @ScalarFunction
  public static long[] fromDateTimeMV(String[] dateTimeString, String pattern, String timeZoneId) {
    long[] results = new long[dateTimeString.length];
    for (int i = 0; i < dateTimeString.length; i++) {
      results[i] = fromDateTime(dateTimeString[i], pattern, timeZoneId);
    }
    return results;
  }

  /**
   * Round the given time value to nearest multiple
   * @return the original value but rounded to the nearest multiple of @param roundToNearest
   */
  @ScalarFunction
  public static long round(long timeValue, long roundToNearest) {
    return (timeValue / roundToNearest) * roundToNearest;
  }

  @ScalarFunction
  public static long[] roundMV(long[] timeValue, long roundToNearest) {
    long[] results = new long[timeValue.length];
    for (int i = 0; i < timeValue.length; i++) {
      results[i] = round(timeValue[i], roundToNearest);
    }
    return results;
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

  @ScalarFunction
  public static long[] agoMV(String[] periodString) {
    long[] results = new long[periodString.length];
    for (int i = 0; i < periodString.length; i++) {
      results[i] = ago(periodString[i]);
    }
    return results;
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
    return timezoneHour(timezoneId, 0);
  }

  @ScalarFunction
  public static int[] timezoneHourMV(String[] timezoneId) {
    int[] results = new int[timezoneId.length];
    for (int i = 0; i < timezoneId.length; i++) {
      results[i] = timezoneHour(timezoneId[i], 0);
    }
    return results;
  }
  /**
   * Returns the hour of the time zone offset, for the UTC timestamp at {@code millis}. This will
   * properly handle daylight savings time.
   */
  @ScalarFunction
  public static int timezoneHour(String timezoneId, long millis) {
    return (int) TimeUnit.MILLISECONDS.toHours(DateTimeZone.forID(timezoneId).getOffset(millis));
  }

  @ScalarFunction
  public static int[] timezoneHourMV(String timezoneId, long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = timezoneHour(timezoneId, millis[i]);
    }
    return results;
  }

  /**
   * Returns the minute of the time zone offset.
   */
  @ScalarFunction
  public static int timezoneMinute(String timezoneId) {
    return timezoneMinute(timezoneId, 0);
  }

  @ScalarFunction
  public static int[] timezoneMinuteMV(String[] timezoneId) {
    int[] results = new int[timezoneId.length];
    for (int i = 0; i < timezoneId.length; i++) {
      results[i] = timezoneMinute(timezoneId[i], 0);
    }
    return results;
  }

  /**
   * Returns the minute of the time zone offset, for the UTC timestamp at {@code millis}. This will
   * properly handle daylight savings time
   */
  @ScalarFunction
  public static int timezoneMinute(String timezoneId, long millis) {
    return (int) TimeUnit.MILLISECONDS.toMinutes(DateTimeZone.forID(timezoneId).getOffset(millis)) % 60;
  }

  @ScalarFunction
  public static int[] timezoneMinuteMV(String timezoneId, long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = timezoneMinute(timezoneId, millis[i]);
    }
    return results;
  }

  /**
   * Returns the year from the given epoch millis in UTC timezone.
   */
  @ScalarFunction
  public static int year(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getYear();
  }

  @ScalarFunction
  public static int[] yearMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = year(millis[i]);
    }
    return results;
  }

  /**
   * Returns the year from the given epoch millis and timezone id.
   */
  @ScalarFunction
  public static int year(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getYear();
  }

  @ScalarFunction
  public static int[] yearMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = year(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the year of the ISO week from the given epoch millis in UTC timezone.
   */
  @ScalarFunction(names = {"yearOfWeek", "year_of_week", "yow"})
  public static int yearOfWeek(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getWeekyear();
  }

  @ScalarFunction(names = {"yearOfWeekMV", "year_of_week_mv", "yowmv"})
  public static int[] yearOfWeekMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = yearOfWeek(millis[i]);
    }
    return results;
  }

  /**
   * Returns the year of the ISO week from the given epoch millis and timezone id.
   */
  @ScalarFunction(names = {"yearOfWeek", "year_of_week", "yow"})
  public static int yearOfWeek(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getWeekyear();
  }

  @ScalarFunction(names = {"yearOfWeekMV", "year_of_week_mv", "yowmv"})
  public static int[] yearOfWeekMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = yearOfWeek(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the quarter of the year from the given epoch millis in UTC timezone. The value ranges from 1 to 4.
   */
  @ScalarFunction
  public static int quarter(long millis) {
    return (monthOfYear(millis) - 1) / 3 + 1;
  }

  @ScalarFunction
  public static int[] quarterMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = quarter(millis[i]);
    }
    return results;
  }

  /**
   * Returns the quarter of the year from the given epoch millis and timezone id. The value ranges from 1 to 4.
   */
  @ScalarFunction
  public static int quarter(long millis, String timezoneId) {
    return (monthOfYear(millis, timezoneId) - 1) / 3 + 1;
  }

  @ScalarFunction
  public static int[] quarterMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = quarter(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the month of the year from the given epoch millis in UTC timezone. The value ranges from 1 to 12.
   */
  @ScalarFunction(names = {"month", "month_of_year", "monthOfYear"})
  public static int monthOfYear(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getMonthOfYear();
  }

  @ScalarFunction(names = {"monthMV", "month_of_year_mv", "monthOfYearMV"})
  public static int[] monthOfYearMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = monthOfYear(millis[i]);
    }
    return results;
  }

  /**
   * Returns the month of the year from the given epoch millis and timezone id. The value ranges from 1 to 12.
   */
  @ScalarFunction(names = {"month", "month_of_year", "monthOfYear"})
  public static int monthOfYear(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getMonthOfYear();
  }

  @ScalarFunction(names = {"monthMV", "month_of_year_mv", "monthOfYearMV"})
  public static int[] monthOfYearMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = monthOfYear(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the ISO week of the year from the given epoch millis in UTC timezone.The value ranges from 1 to 53.
   */
  @ScalarFunction(names = {"weekOfYear", "week_of_year", "week"})
  public static int weekOfYear(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getWeekOfWeekyear();
  }

  @ScalarFunction(names = {"weekOfYearMV", "week_of_year_mv", "weekMV"})
  public static int[] weekOfYearMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = weekOfYear(millis[i]);
    }
    return results;
  }

  /**
   * Returns the ISO week of the year from the given epoch millis and timezone id. The value ranges from 1 to 53.
   */
  @ScalarFunction(names = {"weekOfYear", "week_of_year", "week"})
  public static int weekOfYear(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getWeekOfWeekyear();
  }

  @ScalarFunction(names = {"weekOfYearMV", "week_of_year_mv", "weekMV"})
  public static int[] weekOfYearMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = weekOfYear(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the day of the year from the given epoch millis in UTC timezone. The value ranges from 1 to 366.
   */
  @ScalarFunction(names = {"dayOfYear", "day_of_year", "doy"})
  public static int dayOfYear(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getDayOfYear();
  }

  @ScalarFunction(names = {"dayOfYearMV", "day_of_year_mv", "doyMV"})
  public static int[] dayOfYear(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = dayOfYear(millis[i]);
    }
    return results;
  }

  /**
   * Returns the day of the year from the given epoch millis and timezone id. The value ranges from 1 to 366.
   */
  @ScalarFunction(names = {"dayOfYear", "day_of_year", "doy"})
  public static int dayOfYear(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getDayOfYear();
  }

  @ScalarFunction(names = {"dayOfYearMV", "day_of_year_mv", "doyMV"})
  public static int[] dayOfYear(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = dayOfYear(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the day of the month from the given epoch millis in UTC timezone. The value ranges from 1 to 31.
   */
  @ScalarFunction(names = {"day", "dayOfMonth", "day_of_month"})
  public static int dayOfMonth(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getDayOfMonth();
  }

  @ScalarFunction(names = {"dayMV", "dayOfMonthMV", "day_of_month_mv"})
  public static int[] dayOfMonthMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = dayOfMonth(millis[i]);
    }
    return results;
  }

  /**
   * Returns the day of the month from the given epoch millis and timezone id. The value ranges from 1 to 31.
   */
  @ScalarFunction(names = {"day", "dayOfMonth", "day_of_month"})
  public static int dayOfMonth(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getDayOfMonth();
  }

  @ScalarFunction(names = {"dayMV", "dayOfMonthMV", "day_of_month_mv"})
  public static int[] dayOfMonthMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = dayOfMonth(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the day of the week from the given epoch millis in UTC timezone. The value ranges from 1 (Monday) to 7
   * (Sunday).
   */
  @ScalarFunction(names = {"dayOfWeek", "day_of_week", "dow"})
  public static int dayOfWeek(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getDayOfWeek();
  }

  @ScalarFunction(names = {"dayOfWeekMV", "day_of_week_mv", "dowMV"})
  public static int[] dayOfWeekMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = dayOfWeek(millis[i]);
    }
    return results;
  }

  /**
   * Returns the day of the week from the given epoch millis and timezone id. The value ranges from 1 (Monday) to 7
   * (Sunday).
   */
  @ScalarFunction(names = {"dayOfWeek", "day_of_week", "dow"})
  public static int dayOfWeek(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getDayOfWeek();
  }

  @ScalarFunction(names = {"dayOfWeekMV", "day_of_week_mv", "dowMV"})
  public static int[] dayOfWeekMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = dayOfWeek(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the hour of the day from the given epoch millis in UTC timezone. The value ranges from 0 to 23.
   */
  @ScalarFunction
  public static int hour(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getHourOfDay();
  }

  @ScalarFunction
  public static int[] hourMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = hour(millis[i]);
    }
    return results;
  }

  /**
   * Returns the hour of the day from the given epoch millis and timezone id. The value ranges from 0 to 23.
   */
  @ScalarFunction
  public static int hour(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getHourOfDay();
  }

  @ScalarFunction
  public static int[] hourMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = hour(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the minute of the hour from the given epoch millis in UTC timezone. The value ranges from 0 to 59.
   */
  @ScalarFunction
  public static int minute(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getMinuteOfHour();
  }

  @ScalarFunction
  public static int[] minuteMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = minute(millis[i]);
    }
    return results;
  }

  /**
   * Returns the minute of the hour from the given epoch millis and timezone id. The value ranges from 0 to 59.
   */
  @ScalarFunction
  public static int minute(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getMinuteOfHour();
  }

  @ScalarFunction
  public static int[] minuteMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = minute(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the second of the minute from the given epoch millis in UTC timezone. The value ranges from 0 to 59.
   */
  @ScalarFunction
  public static int second(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getSecondOfMinute();
  }

  @ScalarFunction
  public static int[] secondMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = second(millis[i]);
    }
    return results;
  }

  /**
   * Returns the second of the minute from the given epoch millis and timezone id. The value ranges from 0 to 59.
   */
  @ScalarFunction
  public static int second(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getSecondOfMinute();
  }

  @ScalarFunction
  public static int[] secondMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = second(millis[i], timezoneId);
    }
    return results;
  }

  /**
   * Returns the millisecond of the second from the given epoch millis in UTC timezone. The value ranges from 0 to 999.
   */
  @ScalarFunction
  public static int millisecond(long millis) {
    return new DateTime(millis, DateTimeZone.UTC).getMillisOfSecond();
  }

  @ScalarFunction
  public static int[] millisecondMV(long[] millis) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = millisecond(millis[i]);
    }
    return results;
  }

  /**
   * Returns the millisecond of the second from the given epoch millis and timezone id. The value ranges from 0 to 999.
   */
  @ScalarFunction
  public static int millisecond(long millis, String timezoneId) {
    return new DateTime(millis, DateTimeZone.forID(timezoneId)).getMillisOfSecond();
  }

  @ScalarFunction
  public static int[] millisecondMV(long[] millis, String timezoneId) {
    int[] results = new int[millis.length];
    for (int i = 0; i < millis.length; i++) {
      results[i] = millisecond(millis[i], timezoneId);
    }
    return results;
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
    return dateTrunc(unit, timeValue, TimeUnit.MILLISECONDS.name(), ISOChronology.getInstanceUTC(),
        TimeUnit.MILLISECONDS.name());
  }

  @ScalarFunction
  public static long[] dateTruncMV(String unit, long[] timeValue) {
    long[] results = new long[timeValue.length];
    for (int i = 0; i < timeValue.length; i++) {
      results[i] = dateTrunc(unit, timeValue[i]);
    }
    return results;
  }

  /**
   * The sql compatible date_trunc function for epoch time.
   *
   * @param unit truncate to unit (millisecond, second, minute, hour, day, week, month, quarter, year)
   * @param timeValue value to truncate
   * @param inputTimeUnit TimeUnit of value, expressed in Java's joda TimeUnit
   * @return truncated timeValue in same TimeUnit as the input
   */
  @ScalarFunction
  public static long dateTrunc(String unit, long timeValue, String inputTimeUnit) {
    return dateTrunc(unit, timeValue, inputTimeUnit, ISOChronology.getInstanceUTC(), inputTimeUnit);
  }

  @ScalarFunction
  public static long[] dateTruncMV(String unit, long[] timeValue, String inputTimeUnit) {
    long[] results = new long[timeValue.length];
    for (int i = 0; i < timeValue.length; i++) {
      results[i] = dateTrunc(unit, timeValue[i], inputTimeUnit);
    }
    return results;
  }

  /**
   * The sql compatible date_trunc function for epoch time.
   *
   * @param unit truncate to unit (millisecond, second, minute, hour, day, week, month, quarter, year)
   * @param timeValue value to truncate
   * @param inputTimeUnit TimeUnit of value, expressed in Java's joda TimeUnit
   * @param timeZone timezone of the input
   * @return truncated timeValue in same TimeUnit as the input
   */
  @ScalarFunction
  public static long dateTrunc(String unit, long timeValue, String inputTimeUnit, String timeZone) {
    return dateTrunc(unit, timeValue, inputTimeUnit, DateTimeUtils.getChronology(TimeZoneKey.getTimeZoneKey(timeZone)),
        inputTimeUnit);
  }

  @ScalarFunction
  public static long[] dateTruncMV(String unit, long[] timeValue, String inputTimeUnit, String timeZone) {
    long[] results = new long[timeValue.length];
    for (int i = 0; i < timeValue.length; i++) {
      results[i] = dateTrunc(unit, timeValue[i], inputTimeUnit, timeZone);
    }
    return results;
  }

  /**
   * The sql compatible date_trunc function for epoch time.
   *
   * @param unit truncate to unit (millisecond, second, minute, hour, day, week, month, quarter, year)
   * @param timeValue value to truncate
   * @param inputTimeUnit TimeUnit of value, expressed in Java's joda TimeUnit
   * @param timeZone timezone of the input
   * @param outputTimeUnit TimeUnit to convert the output to
   * @return truncated timeValue
   *
   */
  @ScalarFunction
  public static long dateTrunc(String unit, long timeValue, String inputTimeUnit, String timeZone,
      String outputTimeUnit) {
    return dateTrunc(unit, timeValue, inputTimeUnit,
        DateTimeUtils.getChronology(TimeZoneKey.getTimeZoneKey(timeZone)), outputTimeUnit);
  }

  @ScalarFunction
  public static long[] dateTruncMV(String unit, long[] timeValue, String inputTimeUnit, String timeZone,
      String outputTimeUnit) {
    long[] results = new long[timeValue.length];
    for (int i = 0; i < timeValue.length; i++) {
      results[i] = dateTrunc(unit, timeValue[i], inputTimeUnit, timeZone, outputTimeUnit);
    }
    return results;
  }

  private static long dateTrunc(String unit, long timeValue, String inputTimeUnit, ISOChronology chronology,
      String outputTimeUnit) {
    return TimeUnit.valueOf(outputTimeUnit.toUpperCase()).convert(DateTimeUtils.getTimestampField(chronology, unit)
            .roundFloor(TimeUnit.MILLISECONDS.convert(timeValue, TimeUnit.valueOf(inputTimeUnit.toUpperCase()))),
        TimeUnit.MILLISECONDS);
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

  @ScalarFunction(names = {"timestampAddMV", "dateAddMV"})
  public static long[] timestampAddMV(String unit, long interval, long[] timestamp) {
    long[] results = new long[timestamp.length];
    for (int i = 0; i < timestamp.length; i++) {
      results[i] = timestampAdd(unit, interval, timestamp[i]);
    }
    return results;
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

  @ScalarFunction(names = {"timestampDiffMV", "dateDiffMV"})
  public static long[] timestampDiffMV(String unit, long[] timestamp1, long timestamp2) {
    long[] results = new long[timestamp1.length];
    for (int i = 0; i < timestamp1.length; i++) {
      results[i] = timestampDiff(unit, timestamp1[i], timestamp2);
    }
    return results;
  }

  @ScalarFunction(names = {"timestampDiffMVReverse", "dateDiffMVReverse"})
  public static long[] timestampDiffMVReverse(String unit, long timestamp1, long[] timestamp2) {
    long[] results = new long[timestamp2.length];
    for (int i = 0; i < timestamp2.length; i++) {
      results[i] = timestampDiff(unit, timestamp1, timestamp2[i]);
    }
    return results;
  }
}
