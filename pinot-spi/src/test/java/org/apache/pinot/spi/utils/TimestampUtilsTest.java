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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class TimestampUtilsTest {
  @Test
  public void testValidTimestampFormats() {
    // Test ISO8601 variations with and without milliseconds and timezones
    assertEquals(
        TimestampUtils.toTimestamp("2024-07-12T15:32:36Z"),
        Timestamp.from(LocalDateTime.of(2024, 7, 12, 15, 32, 36).atZone(ZoneOffset.UTC).toInstant()));
    assertEquals(
        TimestampUtils.toTimestamp("2024-07-12 15:32:36.111Z"),
        Timestamp.from(LocalDateTime.of(2024, 7, 12, 15, 32, 36, 111000000).atZone(ZoneOffset.UTC).toInstant()));
    for (int i = 1; i < 7; i++) {
      int fraction = Integer.parseInt("1".repeat(i) + "0".repeat(9 - i));
      assertEquals(
          TimestampUtils.toTimestamp("2024-07-12T15:32:36." + fraction),
          Timestamp.valueOf("2024-07-12 15:32:36." + fraction));
      assertEquals(
          TimestampUtils.toTimestamp("2024-07-12T15:32:36." + fraction + "Z"),
          Timestamp.from(LocalDateTime.of(2024, 7, 12, 15, 32, 36, fraction).atZone(ZoneOffset.UTC).toInstant()));
    }

    // Test date and time variations without 'T'
    assertEquals(TimestampUtils.toTimestamp("2024-07-12 15:32:36.111"), Timestamp.valueOf("2024-07-12 15:32:36.111"));
    assertEquals(TimestampUtils.toTimestamp("2024-07-12 15:32:36"), Timestamp.valueOf("2024-07-12 15:32:36"));
    assertEquals(TimestampUtils.toTimestamp("2024-07-12 15:32"), Timestamp.valueOf("2024-07-12 15:32:00"));
    assertEquals(TimestampUtils.toTimestamp("2024-07-12"), Timestamp.valueOf("2024-07-12 00:00:00"));
    assertEquals(TimestampUtils.toTimestamp("1720798356111"), new Timestamp(1720798356111L));
  }

  @Test
  public void testValidMillisSinceEpochFormats() {
    // Test ISO8601 variations with and without milliseconds and timezones
    assertEquals(
        TimestampUtils.toMillisSinceEpoch("2024-07-12T15:32:36Z"),
        Timestamp.valueOf("2024-07-12 15:32:36").toLocalDateTime().atZone(ZoneOffset.UTC).toInstant().toEpochMilli());
    assertEquals(
        TimestampUtils.toMillisSinceEpoch("2024-07-12 15:32:36.111Z"),
        Timestamp.valueOf("2024-07-12 15:32:36.111").toLocalDateTime().atZone(ZoneOffset.UTC).toInstant()
            .toEpochMilli());
    for (int i = 1; i < 7; i++) {
      String fraction = "1".repeat(i);
      assertEquals(TimestampUtils.toMillisSinceEpoch("2024-07-12 15:32:36." + fraction),
          Timestamp.valueOf("2024-07-12 15:32:36." + fraction).getTime());
      assertEquals(
          TimestampUtils.toMillisSinceEpoch("2024-07-12T15:32:36." + fraction + "Z"),
          Timestamp.valueOf("2024-07-12 15:32:36." + fraction).toLocalDateTime().atZone(ZoneOffset.UTC).toInstant()
              .toEpochMilli());
    }

    // Test date and time variations without 'T'
    assertEquals(TimestampUtils.toMillisSinceEpoch("2024-07-12 15:32:36.111"),
        Timestamp.valueOf("2024-07-12 15:32:36.111").getTime());
    assertEquals(TimestampUtils.toMillisSinceEpoch("2024-07-12 15:32:36"),
        Timestamp.valueOf("2024-07-12 15:32:36").getTime());
    assertEquals(TimestampUtils.toMillisSinceEpoch("2024-07-12 15:32"),
        Timestamp.valueOf("2024-07-12 15:32:00").getTime());
    assertEquals(TimestampUtils.toMillisSinceEpoch("2024-07-12"),
        Timestamp.valueOf("2024-07-12 00:00:00").getTime());
    assertEquals(TimestampUtils.toMillisSinceEpoch("1720798356111"), 1720798356111L);
  }

  @Test
  public void testTimestampFormatsWithZone() {
    // ISO8601 with various timezone offsets
    assertEquals(TimestampUtils.toTimestamp("2024-07-12T15:32:36+02:00"),
        Timestamp.from(ZonedDateTime.of(2024, 7, 12, 15, 32, 36, 0, ZoneId.of("+02:00")).toInstant()));
    assertEquals(TimestampUtils.toTimestamp("2024-07-12T15:32:36-05:00"),
        Timestamp.from(ZonedDateTime.of(2024, 7, 12, 15, 32, 36, 0, ZoneId.of("-05:00")).toInstant()));

    // ISO8601 with milliseconds and various timezones
    assertEquals(TimestampUtils.toTimestamp("2024-07-12T15:32:36.123Z"),
        Timestamp.from(ZonedDateTime.of(2024, 7, 12, 15, 32, 36, 123000000, ZoneId.of("Z")).toInstant()));
    assertEquals(TimestampUtils.toTimestamp("2024-07-12T15:32:36.123+01:30"),
        Timestamp.from(ZonedDateTime.of(2024, 7, 12, 15, 32, 36, 123000000, ZoneId.of("+01:30")).toInstant()));
    assertEquals(TimestampUtils.toTimestamp("2024-07-12T15:32:36.123-08:00"),
        Timestamp.from(ZonedDateTime.of(2024, 7, 12, 15, 32, 36, 123000000, ZoneId.of("-08:00")).toInstant()));

    // Testing edge cases like half-hour and quarter-hour time zones
    assertEquals(TimestampUtils.toTimestamp("2024-07-12T15:32:36+05:45"),
        Timestamp.from(
            ZonedDateTime.of(2024, 7, 12, 15, 32, 36, 0, ZoneId.of("+05:45")).toInstant())); // Nepal Time Zone
    assertEquals(TimestampUtils.toTimestamp("2024-07-12T15:32:36+08:45"),
        Timestamp.from(ZonedDateTime.of(2024, 7, 12, 15, 32, 36, 0, ZoneId.of("+08:45"))
            .toInstant())); // Australian Central Western Standard Time
  }

  @Test
  public void testInvalidFormatHandling() {
    // Test incorrect date and time formats
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toTimestamp("July 12, 2024"));
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toTimestamp("2024-07-12T25:32:36"));
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toTimestamp("2024-07-12T15:32:36+25:00"));
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toTimestamp("This is not a date"));

    // Test incorrect date and time formats for millisecond conversion
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toMillisSinceEpoch("July 12, 2024"));
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toMillisSinceEpoch("2024-07-12T25:32:36"));
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toMillisSinceEpoch("2024-07-12T15:32:36+25:00"));
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toMillisSinceEpoch("This is not a date"));

    // Incorrect time zone formats
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toTimestamp("2024-07-12T15:32:36+25:00"));

    // Invalid minute in time zone
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toTimestamp("2024-07-12T15:32:36+02:60"));

    //Too many digits in fractional seconds
    assertThrows(IllegalArgumentException.class, () -> TimestampUtils.toTimestamp("2024-07-12T15:32:36.12345678910Z"));
  }

  @Test
  public void testValidLocalDateTimeFormats() {
    // Test ISO8601 variations with and without milliseconds and timezones
    assertEquals(
        TimestampUtils.toLocalDateTime("2024-11-11T15:32:36Z"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36));
    assertEquals(
        TimestampUtils.toLocalDateTime("2024-11-11 15:32:36.111Z"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36, 111000000));
    for (int i = 1; i < 7; i++) {
      int fraction = Integer.parseInt("1".repeat(i) + "0".repeat(9 - i));
      assertEquals(
          TimestampUtils.toLocalDateTime("2024-11-11T15:32:36." + fraction),
          LocalDateTime.of(2024, 11, 11, 15, 32, 36, fraction));
      assertEquals(
          TimestampUtils.toLocalDateTime("2024-11-11T15:32:36." + fraction + "Z"),
          LocalDateTime.of(2024, 11, 11, 15, 32, 36, fraction));
    }

    // Test date and time variations without 'T'
    assertEquals(TimestampUtils.toLocalDateTime("2024-11-11 15:32:36.111"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36, 111000000));
    assertEquals(TimestampUtils.toLocalDateTime("2024-11-11 15:32:36"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36));
    assertEquals(TimestampUtils.toLocalDateTime("2024-11-11 15:32"),
        LocalDateTime.of(2024, 11, 11, 15, 32));
    assertEquals(TimestampUtils.toLocalDateTime("2024-11-11"),
        LocalDateTime.of(2024, 11, 11, 0, 0));
    assertEquals(TimestampUtils.toLocalDateTime("1720798356111"),
        LocalDateTime.ofInstant(Instant.ofEpochMilli(1720798356111L), ZoneId.of("UTC")));
  }

  @Test
  public void testValidLocalDateFormats() {
    // Test ISO8601 variations with and without milliseconds and timezones
    assertEquals(TimestampUtils.toLocalDate("2024-11-11"), LocalDate.of(2024, 11, 11));
    assertEquals(TimestampUtils.toLocalDate("10000"), LocalDate.ofEpochDay(10000));
  }

  @Test
  public void testValidLocalTimeFormats() {
    // Test ISO8601 variations with and without milliseconds and timezones
    assertEquals(TimestampUtils.toLocalTime("15:32:36"), LocalTime.of(15, 32, 36));
    assertEquals(
        TimestampUtils.toLocalTime("15:32:36.111"),
        LocalTime.of(15, 32, 36, 111000000));
    for (int i = 1; i < 7; i++) {
      int fraction = Integer.parseInt("1".repeat(i) + "0".repeat(9 - i));
      assertEquals(
          TimestampUtils.toLocalTime("15:32:36." + fraction),
          LocalTime.of(15, 32, 36, fraction));
    }

    assertEquals(TimestampUtils.toLocalTime("100000"), LocalTime.ofNanoOfDay(100000 * 1000000L));
  }

  @Test
  public void testValidMillisSinceEpochInUTCFormats() {
    // Test ISO8601 variations with and without milliseconds and timezones
    assertEquals(
        TimestampUtils.toMillisSinceEpochInUTC("2024-11-11T15:32:36Z"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36)
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
    assertEquals(
        TimestampUtils.toMillisSinceEpochInUTC("2024-11-11 15:32:36.111Z"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36, 111000000)
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
    for (int i = 1; i < 7; i++) {
      int fraction = Integer.parseInt("1".repeat(i) + "0".repeat(9 - i));
      assertEquals(TimestampUtils.toMillisSinceEpochInUTC("2024-11-11 15:32:36." + fraction),
          LocalDateTime.of(2024, 11, 11, 15, 32, 36, fraction).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
      assertEquals(
          TimestampUtils.toMillisSinceEpochInUTC("2024-11-11T15:32:36." + fraction + "Z"),
          LocalDateTime.of(2024, 11, 11, 15, 32, 36, fraction)
              .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
    }

    // Test date and time variations without 'T'
    // Test date and time variations without 'T'
    assertEquals(TimestampUtils.toMillisSinceEpochInUTC("2024-11-11 15:32:36.111"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36, 111000000)
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
    assertEquals(TimestampUtils.toMillisSinceEpochInUTC("2024-11-11 15:32:36"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36)
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
    assertEquals(TimestampUtils.toMillisSinceEpochInUTC("2024-11-11 15:32"),
        LocalDateTime.of(2024, 11, 11, 15, 32)
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
    assertEquals(TimestampUtils.toMillisSinceEpochInUTC("2024-11-11"),
        LocalDateTime.of(2024, 11, 11, 0, 0)
            .atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
    assertEquals(TimestampUtils.toMillisSinceEpochInUTC("1720798356111"), 1720798356111L);
  }

  @Test
  public void testValidDaysSinceEpoch() {
    assertEquals(
        TimestampUtils.toDaysSinceEpoch("2024-11-11"),
        LocalDate.of(2024, 11, 11).toEpochDay());
    assertEquals(TimestampUtils.toDaysSinceEpoch("10000"), 10000);
  }

  @Test
  public void testValidMillisOfDay() {
    assertEquals(
        TimestampUtils.toMillisOfDay("15:32:36"),
        LocalTime.of(15, 32, 36).toNanoOfDay() / 1000000L);
    assertEquals(
        TimestampUtils.toMillisOfDay("15:32:36.111"),
        LocalTime.of(15, 32, 36, 111000000).toNanoOfDay() / 1000000L);
    for (int i = 1; i < 7; i++) {
      int fraction = Integer.parseInt("1".repeat(i) + "0".repeat(9 - i));
      assertEquals(
          TimestampUtils.toMillisOfDay("15:32:36." + fraction),
          LocalTime.of(15, 32, 36, fraction).toNanoOfDay() / 1000000L);
    }

    assertEquals(TimestampUtils.toMillisOfDay("100000"), 100000);
  }

  @Test
  public void testValidLocalDateTimeFormatsWithZone() {
    // when cast to LocalDateTime, ignore time zone
    assertEquals(
        TimestampUtils.toLocalDateTime("2024-11-11T15:32:36+02:00"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36));
    assertEquals(
        TimestampUtils.toLocalDateTime("2024-11-11T15:32:36.111+02:00"),
        LocalDateTime.of(2024, 11, 11, 15, 32, 36, 111000000));
  }
}
