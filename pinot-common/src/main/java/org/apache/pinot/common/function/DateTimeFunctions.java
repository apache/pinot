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
package org.apache.pinot.common.function;

import java.util.concurrent.TimeUnit;
import org.joda.time.format.DateTimeFormat;


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
  /**
   * Convert epoch millis to epoch seconds
   */
  static Long toEpochSeconds(Long millis) {
    return TimeUnit.MILLISECONDS.toSeconds(millis);
  }

  /**
   * Convert epoch millis to epoch minutes
   */
  static Long toEpochMinutes(Long millis) {
    return TimeUnit.MILLISECONDS.toMinutes(millis);
  }

  /**
   * Convert epoch millis to epoch hours
   */
  static Long toEpochHours(Long millis) {
    return TimeUnit.MILLISECONDS.toHours(millis);
  }

  /**
   * Convert epoch millis to epoch days
   */
  static Long toEpochDays(Long millis) {
    return TimeUnit.MILLISECONDS.toDays(millis);
  }

  /**
   * Convert epoch millis to epoch seconds, round to nearest rounding bucket
   */
  static Long toEpochSecondsRounded(Long millis, Number roundToNearest) {
    return (TimeUnit.MILLISECONDS.toSeconds(millis) / roundToNearest.intValue()) * roundToNearest.intValue();
  }

  /**
   * Convert epoch millis to epoch minutes, round to nearest rounding bucket
   */
  static Long toEpochMinutesRounded(Long millis, Number roundToNearest) {
    return (TimeUnit.MILLISECONDS.toMinutes(millis) / roundToNearest.intValue()) * roundToNearest.intValue();
  }

  /**
   * Convert epoch millis to epoch hours, round to nearest rounding bucket
   */
  static Long toEpochHoursRounded(Long millis, Number roundToNearest) {
    return (TimeUnit.MILLISECONDS.toHours(millis) / roundToNearest.intValue()) * roundToNearest.intValue();
  }

  /**
   * Convert epoch millis to epoch days, round to nearest rounding bucket
   */
  static Long toEpochDaysRounded(Long millis, Number roundToNearest) {
    return (TimeUnit.MILLISECONDS.toDays(millis) / roundToNearest.intValue()) * roundToNearest.intValue();
  }

  /**
   * Convert epoch millis to epoch seconds, divided by given bucket, to get nSecondsSinceEpoch
   */
  static Long toEpochSecondsBucket(Long millis, Number bucket) {
    return TimeUnit.MILLISECONDS.toSeconds(millis) / bucket.intValue();
  }

  /**
   * Convert epoch millis to epoch minutes, divided by given bucket, to get nMinutesSinceEpoch
   */
  static Long toEpochMinutesBucket(Long millis, Number bucket) {
    return TimeUnit.MILLISECONDS.toMinutes(millis) / bucket.intValue();
  }

  /**
   * Convert epoch millis to epoch hours, divided by given bucket, to get nHoursSinceEpoch
   */
  static Long toEpochHoursBucket(Long millis, Number bucket) {
    return TimeUnit.MILLISECONDS.toHours(millis) / bucket.intValue();
  }

  /**
   * Convert epoch millis to epoch days, divided by given bucket, to get nDaysSinceEpoch
   */
  static Long toEpochDaysBucket(Long millis, Number bucket) {
    return TimeUnit.MILLISECONDS.toDays(millis) / bucket.intValue();
  }

  /**
   * Converts epoch seconds to epoch millis
   */
  static Long fromEpochSeconds(Long seconds) {
    return TimeUnit.SECONDS.toMillis(seconds);
  }

  /**
   * Converts epoch minutes to epoch millis
   */
  static Long fromEpochMinutes(Number minutes) {
    return TimeUnit.MINUTES.toMillis(minutes.longValue());
  }

  /**
   * Converts epoch hours to epoch millis
   */
  static Long fromEpochHours(Number hours) {
    return TimeUnit.HOURS.toMillis(hours.longValue());
  }

  /**
   * Converts epoch days to epoch millis
   */
  static Long fromEpochDays(Number daysSinceEpoch) {
    return TimeUnit.DAYS.toMillis(daysSinceEpoch.longValue());
  }

  /**
   * Converts nSecondsSinceEpoch (seconds that have been divided by a bucket), to epoch millis
   */
  static Long fromEpochSecondsBucket(Long seconds, Number bucket) {
    return TimeUnit.SECONDS.toMillis(seconds * bucket.intValue());
  }

  /**
   * Converts nMinutesSinceEpoch (minutes that have been divided by a bucket), to epoch millis
   */
  static Long fromEpochMinutesBucket(Number minutes, Number bucket) {
    return TimeUnit.MINUTES.toMillis(minutes.longValue() * bucket.intValue());
  }

  /**
   * Converts nHoursSinceEpoch (hours that have been divided by a bucket), to epoch millis
   */
  static Long fromEpochHoursBucket(Number hours, Number bucket) {
    return TimeUnit.HOURS.toMillis(hours.longValue() * bucket.intValue());
  }

  /**
   * Converts nDaysSinceEpoch (days that have been divided by a bucket), to epoch millis
   */
  static Long fromEpochDaysBucket(Number daysSinceEpoch, Number bucket) {
    return TimeUnit.DAYS.toMillis(daysSinceEpoch.longValue() * bucket.intValue());
  }

  /**
   * Converts epoch millis to DateTime string represented by pattern
   */
  static String toDateTime(Long millis, String pattern) {
    return DateTimePatternHandler.parseEpochMillisToDateTimeString(millis, pattern);
  }

  /**
   * Converts DateTime string represented by pattern to epoch millis
   */
  static Long fromDateTime(String dateTimeString, String pattern) {
    return DateTimePatternHandler.parseDateTimeStringToEpochMillis(dateTimeString, pattern);
  }

  /**
   * Return current time as epoch millis
   */
  static Long now() {
    return System.currentTimeMillis();
  }

  /**
   * Return epoch millis value based on a given date time string and it's corresponding format.
   */
  public static Long formatDatetime(String input, String format) {
    return DateTimeFormat.forPattern(format).parseMillis(input);
  }
}
