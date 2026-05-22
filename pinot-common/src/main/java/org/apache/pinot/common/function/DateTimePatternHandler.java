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

import org.joda.time.DateTimeZone;
import org.joda.time.IllegalInstantException;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * Handles DateTime conversions from long to strings and strings to longs based on passed patterns
 */
public class DateTimePatternHandler {
  private DateTimePatternHandler() {
  }

  /**
   * Converts the dateTimeString of passed pattern into a long of the millis since epoch
   */
  public static long parseDateTimeStringToEpochMillis(String dateTimeString, String pattern) {
    return parseWithDstGapFallback(dateTimeString, pattern, DateTimeZone.UTC);
  }

  /**
   * Converts the dateTimeString of passed pattern into a long of the millis since epoch
   */
  public static long parseDateTimeStringToEpochMillis(String dateTimeString, String pattern, String timezoneId) {
    return parseWithDstGapFallback(dateTimeString, pattern, DateTimeZone.forID(timezoneId));
  }

  /**
   * Converts the dateTimeString of the pattern/timezone and return default value when exception occurs.
   */
  public static long parseDateTimeStringToEpochMillis(String dateTimeString, String pattern, String timezoneId,
      long defaultVal) {
    try {
      return parseWithDstGapFallback(dateTimeString, pattern, DateTimeZone.forID(timezoneId));
    } catch (Exception e) {
      return defaultVal;
    }
  }

  /**
   * Converts the millis representing seconds since epoch into a string of passed pattern
   */
  public static String parseEpochMillisToDateTimeString(long millis, String pattern) {
    return getDateTimeFormatter(pattern).print(millis);
  }

  /**
   * Converts the millis representing seconds since epoch into a string of passed pattern and time zone id
   */
  public static String parseEpochMillisToDateTimeString(long millis, String pattern, String timezoneId) {
    return getDateTimeFormatter(pattern, timezoneId).print(millis);
  }

  /**
   * Parses {@code dateTimeString} as an epoch-millis instant in {@code zone}.
   *
   * <p>On a DST spring-forward transition the local wall-clock time being parsed does not exist (e.g. midnight
   * of 2026-04-24 in {@code Africa/Cairo}). Joda strict parsing throws {@link IllegalInstantException} there,
   * which turns a perfectly valid calendar input into a user-facing parse failure. This method catches that
   * specific exception, re-parses the wall-clock time as if it were in UTC, and then uses
   * {@link DateTimeZone#convertLocalToUTC(long, boolean)} with {@code strict=false} to shift the instant
   * forward past the gap to the next valid wall-clock time in {@code zone}. All other parse errors
   * (unparseable input, out-of-range fields) continue to propagate so typos still surface.
   *
   * <p>This catch is narrow by design. In Joda's strict parsing path, {@code IllegalInstantException} is
   * only thrown for non-existent local times (spring-forward gaps). Ambiguous times during a fall-back
   * transition are not affected — Joda already resolves those to a deterministic offset without throwing.
   *
   * <p>Strict-first (rather than always routing through the fallback) is deliberate. Joda handles the
   * four timezone pattern tokens differently inside {@code DateTimeParserBucket.computeMillis}:
   * <ul>
   *   <li>{@code Z} ({@code -0800}) and {@code ZZ} ({@code -08:00}) populate {@code iOffset}, which
   *       short-circuits the gap check entirely — strict parsing of such inputs never throws
   *       {@code IllegalInstantException}.</li>
   *   <li>{@code ZZZ} ({@code America/Los_Angeles}) and {@code z} overwrite the bucket's {@code iZone},
   *       so the gap check runs against the <em>parsed</em> zone, not the formatter's {@code withZone}
   *       argument. For the common case where the parsed zone has no gap at the parsed local time, strict
   *       parsing succeeds and the fallback is never entered.</li>
   * </ul>
   * If we always routed through the fallback path, a successful {@code withZoneUTC().parseMillis(...)}
   * call on a {@code Z}/{@code ZZ} input would yield a real UTC instant; passing that value to
   * {@link DateTimeZone#convertLocalToUTC(long, boolean)} would then reinterpret it as a local wall-clock
   * value in {@code zone} and produce a doubly-adjusted, incorrect result. Entering the fallback only when
   * strict parsing throws {@code IllegalInstantException} guarantees that either (a) the input carried no
   * explicit zone/offset token, so reinterpreting the parsed value as local-in-{@code zone} is safe, or
   * (b) the input carried a {@code ZZZ}/{@code z} token whose parsed zone itself lay in a gap, in which
   * case the fallback re-parse will throw {@code IllegalInstantException} again and propagate unchanged.
   * Out-of-range field validation (e.g. month 13, day 32) is not a differentiator between the two paths:
   * both paths use strict ISO field-range parsing and reject such inputs identically.
   *
   * <p>The lenient fallback is agnostic to gap size. A 30-minute spring-forward (e.g. Australia/Lord_Howe)
   * resolves to the first valid instant 30 minutes after the strict boundary, and an unusual day-length
   * change — e.g. Pacific/Apia on 2011-12-30, a date that was skipped entirely when Samoa crossed the
   * International Date Line — resolves to the first valid instant on the next calendar day.
   */
  private static long parseWithDstGapFallback(String dateTimeString, String pattern, DateTimeZone zone) {
    DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
    try {
      return formatter.withZone(zone).parseMillis(dateTimeString);
    } catch (IllegalInstantException e) {
      long localAsUtcMillis = formatter.withZoneUTC().parseMillis(dateTimeString);
      return zone.convertLocalToUTC(localAsUtcMillis, false);
    }
  }

  private static DateTimeFormatter getDateTimeFormatter(String pattern, String timezoneId) {
    // This also leverages an internal cache so it won't generate a new DateTimeFormatter for every row with
    // the same pattern
    return DateTimeFormat.forPattern(pattern).withZone(DateTimeZone.forID(timezoneId));
  }

  private static DateTimeFormatter getDateTimeFormatter(String pattern) {
    return getDateTimeFormatter(pattern, DateTimeZone.UTC.getID());
  }
}
