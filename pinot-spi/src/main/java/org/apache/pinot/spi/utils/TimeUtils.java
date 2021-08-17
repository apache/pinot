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
package org.apache.pinot.spi.utils;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;


public class TimeUtils {
  private TimeUtils() {
  }

  public static final long VALID_MIN_TIME_MILLIS = new DateTime(1971, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
  public static final long VALID_MAX_TIME_MILLIS = new DateTime(2071, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
  public static final Interval VALID_TIME_INTERVAL = new Interval(VALID_MIN_TIME_MILLIS, VALID_MAX_TIME_MILLIS, DateTimeZone.UTC);

  private static final String UPPER_CASE_DAYS = "DAYS";
  private static final String UPPER_CASE_DAYS_SINCE_EPOCH = "DAYSSINCEEPOCH";
  private static final String UPPER_CASE_HOURS = "HOURS";
  private static final String UPPER_CASE_HOURS_SINCE_EPOCH = "HOURSSINCEEPOCH";
  private static final String UPPER_CASE_MINUTES = "MINUTES";
  private static final String UPPER_CASE_MINUTES_SINCE_EPOCH = "MINUTESSINCEEPOCH";
  private static final String UPPER_CASE_SECONDS = "SECONDS";
  private static final String UPPER_CASE_SECONDS_SINCE_EPOCH = "SECONDSSINCEEPOCH";
  private static final String UPPER_CASE_MILLISECONDS = "MILLISECONDS";
  private static final String UPPER_CASE_MILLIS_SINCE_EPOCH = "MILLISSINCEEPOCH";
  private static final String UPPER_CASE_MILLISECONDS_SINCE_EPOCH = "MILLISECONDSSINCEEPOCH";
  private static final String UPPER_CASE_MICROSECONDS = "MICROSECONDS";
  private static final String UPPER_CASE_MICROS_SINCE_EPOCH = "MICROSSINCEEPOCH";
  private static final String UPPER_CASE_MICROSECONDS_SINCE_EPOCH = "MICROSECONDSSINCEEPOCH";
  private static final String UPPER_CASE_NANOSECONDS = "NANOSECONDS";
  private static final String UPPER_CASE_NANOS_SINCE_EPOCH = "NANOSSINCEEPOCH";
  private static final String UPPER_CASE_NANOSECONDS_SINCE_EPOCH = "NANOSECONDSSINCEEPOCH";

  /**
   * Converts a time unit string into {@link TimeUnit}, ignoring case. For {@code null} or empty time unit string,
   * returns {@code null}.
   * <p>Besides the standard time unit, also support the following time unit strings:
   * <ul>
   *   <li>"daysSinceEpoch" -> DAYS</li>
   *   <li>"hoursSinceEpoch" -> HOURS</li>
   *   <li>"minutesSinceEpoch" -> MINUTES</li>
   *   <li>"secondsSinceEpoch" -> SECONDS</li>
   *   <li>"millisSinceEpoch"/"millisecondsSinceEpoch" -> MILLISECONDS</li>
   *   <li>"microsSinceEpoch"/"microsecondsSinceEpoch" -> MICROSECONDS</li>
   *   <li>"nanosSinceEpoch"/"nanosecondsSinceEpoch" -> NANOSECONDS</li>
   * </ul>
   *
   * @param timeUnitString The time unit string to convert, e.g. "DAYS" or "SECONDS"
   * @return The corresponding {@link TimeUnit}
   */
  @Nullable
  public static TimeUnit timeUnitFromString(@Nullable String timeUnitString) {
    // NOTE: for backward-compatibility, return null if time unit string is null or empty
    if (timeUnitString == null || timeUnitString.isEmpty()) {
      return null;
    }
    switch (timeUnitString.toUpperCase()) {
      case UPPER_CASE_DAYS:
      case UPPER_CASE_DAYS_SINCE_EPOCH:
        return TimeUnit.DAYS;
      case UPPER_CASE_HOURS:
      case UPPER_CASE_HOURS_SINCE_EPOCH:
        return TimeUnit.HOURS;
      case UPPER_CASE_MINUTES:
      case UPPER_CASE_MINUTES_SINCE_EPOCH:
        return TimeUnit.MINUTES;
      case UPPER_CASE_SECONDS:
      case UPPER_CASE_SECONDS_SINCE_EPOCH:
        return TimeUnit.SECONDS;
      case UPPER_CASE_MILLISECONDS:
      case UPPER_CASE_MILLIS_SINCE_EPOCH:
      case UPPER_CASE_MILLISECONDS_SINCE_EPOCH:
        return TimeUnit.MILLISECONDS;
      case UPPER_CASE_MICROSECONDS:
      case UPPER_CASE_MICROS_SINCE_EPOCH:
      case UPPER_CASE_MICROSECONDS_SINCE_EPOCH:
        return TimeUnit.MICROSECONDS;
      case UPPER_CASE_NANOSECONDS:
      case UPPER_CASE_NANOS_SINCE_EPOCH:
      case UPPER_CASE_NANOSECONDS_SINCE_EPOCH:
        return TimeUnit.NANOSECONDS;
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + timeUnitString);
    }
  }

  /**
   * Given a time value, returns true if the value is between a valid range, false otherwise.
   * <p>The current valid range used is between beginning of 1971 and beginning of 2071.
   */
  public static boolean timeValueInValidRange(long timeValueInMillis) {
    return timeValueInMillis >= VALID_MIN_TIME_MILLIS && timeValueInMillis <= VALID_MAX_TIME_MILLIS;
  }

  /**
   * Given a time interval, returns true if the interval is between a valid range, false otherwise.
   * <p>The current valid range used is between beginning of 1971 and beginning of 2071.
   */
  public static boolean isValidTimeInterval(Interval timeInterval) {
    return timeInterval.getStartMillis() >= VALID_MIN_TIME_MILLIS && timeInterval.getEndMillis() <= VALID_MAX_TIME_MILLIS;
  }

  /**
   * Returns the minimum valid time in milliseconds.
   */
  public static long getValidMinTimeMillis() {
    return VALID_MIN_TIME_MILLIS;
  }

  /**
   * Returns the maximum valid time in milliseconds.
   */
  public static long getValidMaxTimeMillis() {
    return VALID_MAX_TIME_MILLIS;
  }

  private static final PeriodFormatter PERIOD_FORMATTER =
      new PeriodFormatterBuilder().appendDays().appendSuffix("d").appendHours().appendSuffix("h").appendMinutes().appendSuffix("m").appendSeconds()
          .appendSuffix("s").toFormatter();

  /**
   * Converts a string representing a period/duration to corresponding milliseconds.
   * For ex, input "1d" would return 86400000L. Supported units are days (d), hours (h),
   * minutes (m) and seconds (s).
   *
   * @param timeStr string representing the duration
   * @return the corresponding time converted to milliseconds
   * @throws IllegalArgumentException if the string does not conform to the expected format
   */
  public static Long convertPeriodToMillis(String timeStr) {
    Long millis = 0L;
    if (timeStr != null) {
      try {
        Period p = PERIOD_FORMATTER.parsePeriod(timeStr);
        millis = p.toStandardDuration().getStandardSeconds() * 1000L;
      } catch (IllegalArgumentException e) {
        // rethrowing with more contextual information
        throw new IllegalArgumentException("Invalid time spec '" + timeStr + "' (Valid examples: '3h', '4h30m')", e);
      }
    }
    return millis;
  }

  /**
   * Converts milliseconds into human readable duration string. For ex, input of 86400000L would
   * return "1d".
   *
   * @param millis the value in milliseconds to be converted
   * @return the corresponding human readable string or empty string if value cannot be converted
   */
  public static String convertMillisToPeriod(Long millis) {
    String periodStr = null;
    if (millis != null) {
      Period p = new Period(new Duration(millis));
      periodStr = PERIOD_FORMATTER.print(p);
    }
    return periodStr;
  }

  /**
   * Checks if the given period string is valid i.e. has format like 30h, 15d, 10m
   */
  public static boolean isPeriodValid(String timeStr) {
    try {
      PERIOD_FORMATTER.parsePeriod(timeStr);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
