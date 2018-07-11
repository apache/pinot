/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;


public class TimeUtils {
  private static final Map<String, TimeUnit> TIME_UNIT_MAP = new HashMap<>();

  private static final long VALID_MIN_TIME_MILLIS =
      new DateTime(1971, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
  private static final long VALID_MAX_TIME_MILLIS =
      new DateTime(2071, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();

  static {
    for (TimeUnit timeUnit : TimeUnit.values()) {
      TIME_UNIT_MAP.put(timeUnit.name(), timeUnit);
    }
  }

  /**
   * Converts timeValue in timeUnitString to milliseconds
   * @param timeUnitString the time unit string to convert, such as DAYS or SECONDS
   * @param timeValue the time value to convert to milliseconds
   * @return corresponding value in milliseconds or LONG.MIN_VALUE if timeUnitString is invalid
   *         Returning LONG.MIN_VALUE gives consistent beahvior with the java library
   */
  public static long toMillis(String timeUnitString, String timeValue) {
    TimeUnit timeUnit = timeUnitFromString(timeUnitString);
    return (timeUnit == null) ? Long.MIN_VALUE : timeUnit.toMillis(Long.parseLong(timeValue));
  }

  /**
   * Turns a time unit string into a TimeUnit, ignoring case.
   *
   * @param timeUnitString The time unit string to convert, such as DAYS or SECONDS.
   * @return The corresponding time unit or null if it doesn't exist
   */
  public static TimeUnit timeUnitFromString(String timeUnitString) {
    if (timeUnitString == null) {
      return null;
    } else {
      return TIME_UNIT_MAP.get(timeUnitString.toUpperCase());
    }
  }

  /**
   * Given a time value, returns true if the value is between a valid range, false otherwise
   * The current valid range used is between beginning of 1971 and beginning of 2071.
   *
   * @param timeValueInMillis
   * @return True if time value in valid range, false otherwise.
   */
  public static boolean timeValueInValidRange(long timeValueInMillis) {
    return (VALID_MIN_TIME_MILLIS <= timeValueInMillis && timeValueInMillis <= VALID_MAX_TIME_MILLIS);
  }

  /**
   * Return the minimum valid time in milliseconds.
   * @return
   */
  public static long getValidMinTimeMillis() {
    return VALID_MIN_TIME_MILLIS;
  }

  /**
   * Returns the maximum valid time in milliseconds.
   * @return
   */
  public static long getValidMaxTimeMillis() {
    return VALID_MAX_TIME_MILLIS;
  }

  private static final PeriodFormatter PERIOD_FORMATTER = new PeriodFormatterBuilder()
      .appendDays().appendSuffix("d")
      .appendHours().appendSuffix("h")
      .appendMinutes().appendSuffix("m")
      .appendSeconds().appendSuffix("s")
      .toFormatter();

  /**
   * Converts a string representing a period/duration to corresponding milliseconds.
   * For ex, input "1d" would return 86400000L. Supported units are days (d), hours (h),
   * minutes (m) and seconds (s).
   *
   * @param timeStr string representing the duration
   * @return the corresponding time coverted to milliseconds
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
  public static  String convertMillisToPeriod(Long millis) {
    String periodStr = null;
    if (millis != null) {
      Period p = new Period(new Duration(millis));
      periodStr = PERIOD_FORMATTER.print(p);
    }
    return periodStr;
  }
}
