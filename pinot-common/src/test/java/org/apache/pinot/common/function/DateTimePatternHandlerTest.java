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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.IllegalFieldValueException;
import org.joda.time.IllegalInstantException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/**
 * Tests for {@link DateTimePatternHandler}, with particular focus on DST spring-forward gaps where the
 * requested local wall-clock time does not exist in the target zone.
 *
 * <p>Gap dates chosen here are all historical facts already frozen in the tzdata database (Cairo 2010-04-30,
 * Santiago 2024-09-08, Lord_Howe 2010-10-03) so these tests are not fragile against future tzdata releases
 * that might, for example, drop DST from a country that currently observes it.
 */
public class DateTimePatternHandlerTest {

  // Cairo sprang forward at 00:00 local on 2010-04-30 (last Friday of April), so 00:00 did not exist that
  // day; the first valid wall-clock instant is 01:00 EEST (UTC+3). Parsing a date-only string should land
  // on that first valid instant rather than throwing.
  @Test
  public void testDstGapAtMidnightCairo() {
    long actual = DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2010-04-30", "yyyy-MM-dd", "Africa/Cairo");
    long expected = new DateTime(2010, 4, 30, 1, 0, 0, 0, DateTimeZone.forID("Africa/Cairo")).getMillis();
    assertEquals(actual, expected);
  }

  // Same semantics when an explicit gap-local time is given: the parser should shift forward past the gap.
  @Test
  public void testDstGapExplicitGapTimeCairo() {
    long actual = DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2010-04-30 00:30:00", "yyyy-MM-dd HH:mm:ss", "Africa/Cairo");
    // 00:30 lies inside the gap; lenient resolution shifts forward by the gap size (1h) -> 01:30 EEST.
    long expected = new DateTime(2010, 4, 30, 1, 30, 0, 0, DateTimeZone.forID("Africa/Cairo")).getMillis();
    assertEquals(actual, expected);
  }

  // The default-value overload is currently silent on DST gaps: it returns the default and hides data. After the
  // fix, a DST gap should resolve to a valid instant; the default is only used when the input is truly malformed.
  @Test
  public void testDstGapDoesNotFallBackToDefault() {
    long actual = DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2010-04-30", "yyyy-MM-dd", "Africa/Cairo", -1L);
    long expected = new DateTime(2010, 4, 30, 1, 0, 0, 0, DateTimeZone.forID("Africa/Cairo")).getMillis();
    assertEquals(actual, expected);
  }

  // Genuinely unparseable input should still fall through to the default value.
  @Test
  public void testUnparseableStillReturnsDefault() {
    long actual = DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "not-a-date", "yyyy-MM-dd", "UTC", -1L);
    assertEquals(actual, -1L);
  }

  // We only want to be lenient about DST gaps; other field-range errors must remain strict so that typos
  // (e.g. month 13) continue to surface rather than silently rolling into the next year.
  @Test
  public void testOutOfRangeFieldStillThrows() {
    assertThrows(IllegalFieldValueException.class, () -> DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2026-13-05", "yyyy-MM-dd", "UTC"));
  }

  // Ordinary inputs in a zone that has DST must be unaffected by the fallback logic.
  @Test
  public void testNonGapDateInDstZoneUnchanged() {
    long actual = DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2026-05-15 12:00:00", "yyyy-MM-dd HH:mm:ss", "Africa/Cairo");
    long expected = new DateTime(2026, 5, 15, 12, 0, 0, 0, DateTimeZone.forID("Africa/Cairo")).getMillis();
    assertEquals(actual, expected);
  }

  // UTC has no DST, so behavior must be identical to before.
  @Test
  public void testUtcUnaffected() {
    long actual = DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2026-04-24 00:00:00", "yyyy-MM-dd HH:mm:ss");
    long expected = new DateTime(2026, 4, 24, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
    assertEquals(actual, expected);
  }

  // Exercises the Western-hemisphere branch of DateTimeZone.convertLocalToUTC(strict=false). Santiago
  // springs forward at 00:00 local on the first Sunday of September (2024-09-08); the fallback resolution
  // for a negative pre-transition offset picks the post-transition offset so the returned instant lands at
  // 01:00 -03:00, i.e. the first valid wall-clock time after the gap. Without this test, the fix is only
  // exercised on Eastern-hemisphere zones (e.g. Cairo) and the Western branch of convertLocalToUTC is
  // uncovered.
  @Test
  public void testDstGapWesternHemisphereSantiago() {
    long actual = DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2024-09-08", "yyyy-MM-dd", "America/Santiago");
    long expected =
        new DateTime(2024, 9, 8, 1, 0, 0, 0, DateTimeZone.forID("America/Santiago")).getMillis();
    assertEquals(actual, expected);
  }

  // Regression guard for the strict-first ordering. If a future refactor accidentally routes all inputs
  // through the lenient fallback, a pattern with an explicit offset token (Z) would be parsed in UTC and
  // then re-adjusted by convertLocalToUTC as if it were a local wall-clock time in `zone`, producing a
  // doubly-adjusted (wrong) result. The expected value here is the real UTC instant encoded by the input
  // offset, independent of the `zone` argument.
  @Test
  public void testExplicitOffsetTokenNotDoubleAdjustedByFallback() {
    long actual = DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2024-01-01T12:00:00+0500", "yyyy-MM-dd'T'HH:mm:ssZ", "Africa/Cairo");
    long expected = new DateTime(2024, 1, 1, 7, 0, 0, 0, DateTimeZone.UTC).getMillis();
    assertEquals(actual, expected);
  }

  // Lord Howe Island uses a 30-minute DST shift (02:00 -> 02:30 local on the first Sunday of October). The
  // fallback is agnostic to gap size, so a half-hour gap must be handled the same way as a one-hour gap.
  // 2010-10-03 is a historical transition, safe from tzdata drift.
  @Test
  public void testHalfHourDstGapLordHowe() {
    long actual = DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2010-10-03 02:15:00", "yyyy-MM-dd HH:mm:ss", "Australia/Lord_Howe");
    // 02:15 lies inside the 30-minute gap; lenient resolution shifts forward by 30 minutes to 02:45 +11:00.
    long expected =
        new DateTime(2010, 10, 3, 2, 45, 0, 0, DateTimeZone.forID("Australia/Lord_Howe")).getMillis();
    assertEquals(actual, expected);
  }

  // Pins the Javadoc claim that when a ZZZ/z token in the input encodes a zone whose wall-clock time is
  // itself in a gap, the fallback re-parse throws IllegalInstantException again and the exception
  // propagates unchanged. Without this test, a future refactor that quietly broadens what the fallback
  // swallows could silently accept a genuinely broken input.
  @Test
  public void testParsedZoneTokenWhoseZoneIsInGapStillThrows() {
    assertThrows(IllegalInstantException.class, () -> DateTimePatternHandler.parseDateTimeStringToEpochMillis(
        "2010-04-30 00:30:00 Africa/Cairo", "yyyy-MM-dd HH:mm:ss ZZZ", "UTC"));
  }
}
