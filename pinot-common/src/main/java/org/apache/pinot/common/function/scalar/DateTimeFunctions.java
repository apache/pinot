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

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.function.DateTimePatternHandler;
import org.apache.pinot.common.function.annotations.ScalarFunction;


/**
 * Inbuilt date time related transform functions
 * TODO: Exhaustively add all time conversion functions
 *  eg:
 *   1) round(time, roundingValue) - round(minutes, 10), round(millis, 15:MINUTES)
 *   2) simple date time transformations
 *   3) convert(from_format, to_format, bucketing)
 *
 *   NOTE:
 *   <code>toEpochXXXBucket</code> methods are only needed to convert from TimeFieldSpec to DateTimeFieldSpec, to maintain the backward compatibility.
 *   Practically, we should only need the <code>toEpochXXXRounded</code> methods.
 *   Use of <code>toEpochXXXBucket</code> bucket functions is discouraged unless you know what you are doing -
 *   (e.g. 5-minutes-since-epoch does not make sense to someone looking at the timestamp, or writing queries. instead, Millis-since-epoch rounded to 5 minutes makes a lot more sense)
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
   * Converts epoch millis to DateTime string represented by pattern
   */
  @ScalarFunction
  public static String toDateTime(long millis, String pattern) {
    return DateTimePatternHandler.parseEpochMillisToDateTimeString(millis, pattern);
  }

  /**
   * Converts DateTime string represented by pattern to epoch millis
   */
  @ScalarFunction
  public static long fromDateTime(String dateTimeString, String pattern) {
    return DateTimePatternHandler.parseDateTimeStringToEpochMillis(dateTimeString, pattern);
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
   */
  @ScalarFunction
  public static long now() {
    return System.currentTimeMillis();
  }
}
