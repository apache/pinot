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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


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
}
