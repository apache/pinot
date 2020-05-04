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
package org.apache.pinot.core.data.function;

import java.util.concurrent.TimeUnit;


/**
 * Inbuilt date time related transform functions
 * TODO: Exhaustively add all time conversion functions
 */
public class DateTimeFunctions {

  /**
   * Convert epoch millis to epoch seconds
   */
  static Long toEpochSeconds(Long millis) {
    return TimeUnit.MILLISECONDS.toSeconds(millis);
  }

  /**
   * Convert epoch millis to epoch seconds, bucketed by given bucket granularity
   */
  static Long toEpochSecondsBucket(Long millis, String bucket) {
    return TimeUnit.MILLISECONDS.toSeconds(millis) / Integer.valueOf(bucket);
  }

  /**
   * Convert epoch millis to epoch minutes
   */
  static Long toEpochMinutes(Long millis) {
    return TimeUnit.MILLISECONDS.toMinutes(millis);
  }

  /**
   * Convert epoch millis to epoch minutes, bucketed by given bucket granularity
   */
  static Long toEpochMinutesBucket(Long millis, String bucket) {
    return TimeUnit.MILLISECONDS.toMinutes(millis) / Integer.valueOf(bucket);
  }

  /**
   * Convert epoch millis to epoch hours
   */
  static Long toEpochHours(Long millis) {
    return TimeUnit.MILLISECONDS.toHours(millis);
  }

  /**
   * Convert epoch millis to epoch hours, bucketed by given bucket granularity
   */
  static Long toEpochHoursBucket(Long millis, String bucket) {
    return TimeUnit.MILLISECONDS.toHours(millis) / Integer.valueOf(bucket);
  }

  /**
   * Convert epoch millis to epoch days
   */
  static Long toEpochDays(Long millis) {
    return TimeUnit.MILLISECONDS.toDays(millis);
  }

  /**
   * Convert epoch millis to epoch days, bucketed by given bucket granularity
   */
  static Long toEpochDaysBucket(Long millis, String bucket) {
    return TimeUnit.MILLISECONDS.toDays(millis) / Integer.valueOf(bucket);
  }

  /**
   * Converts epoch seconds to epoch millis
   */
  static Long fromEpochSeconds(Long seconds) {
    return TimeUnit.SECONDS.toMillis(seconds);
  }

  /**
   * Converts bucketed seconds, to epoch millis
   */
  static Long fromEpochSecondsBucket(Long seconds, String bucket) {
    return TimeUnit.SECONDS.toMillis(seconds * Integer.valueOf(bucket));
  }

  /**
   * Converts epoch minutes to epoch millis
   */
  static Long fromEpochMinutes(Number minutes) {
    return TimeUnit.MINUTES.toMillis(minutes.longValue());
  }

  /**
   * Converts bucketed minutes, to epoch millis
   */
  static Long fromEpochMinutesBucket(Number minutes, String bucket) {
    return TimeUnit.MINUTES.toMillis(minutes.longValue() * Integer.valueOf(bucket));
  }

  /**
   * Converts epoch hours to epoch millis
   */
  static Long fromEpochHours(Number hours) {
    return TimeUnit.HOURS.toMillis(hours.longValue());
  }

  /**
   * Converts bucketed hours, to epoch millis
   */
  static Long fromEpochHoursBucket(Number hours, String bucket) {
    return TimeUnit.HOURS.toMillis(hours.longValue() * Integer.valueOf(bucket));
  }

  /**
   * Converts epoch days to epoch millis
   */
  static Long fromEpochDays(Number daysSinceEpoch) {
    return TimeUnit.DAYS.toMillis(daysSinceEpoch.longValue());
  }

  /**
   * Converts bucketed days, to epoch millis
   */
  static Long fromEpochDaysBucket(Number daysSinceEpoch, String bucket) {
    return TimeUnit.DAYS.toMillis(daysSinceEpoch.longValue() * Integer.valueOf(bucket));
  }
}
