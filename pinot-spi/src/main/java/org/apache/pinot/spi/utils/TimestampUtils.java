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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;


public class TimestampUtils {
  private static final DateTimeFormatter UNIVERSAL_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
      // Date part
      .appendPattern("yyyy-MM-dd")
      // Optional time part starting with 'T'
      .optionalStart()
      .appendLiteral('T')
      .appendPattern("HH:mm")
      .optionalStart()
      .appendLiteral(':')
      .appendPattern("ss")
      .optionalEnd()
      .optionalStart()
      .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
      .optionalEnd()
      .optionalEnd()
      // Optional space-separated time part
      .optionalStart()
      .appendLiteral(' ')
      .appendPattern("HH:mm")
      .optionalStart()
      .appendLiteral(':')
      .appendPattern("ss")
      .optionalEnd()
      .optionalStart()
      .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
      .optionalEnd()
      .optionalEnd()
      // Time zone handling, allows parsing of 'Z', '+hh:mm', '-hh:mm'
      .appendOptional(DateTimeFormatter.ofPattern("XXX"))
      // Default values for missing time components
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter();

  private TimestampUtils() {
  }

  /**
   * Parses the given timestamp string into {@link Timestamp}.
   * <p>Below formats of timestamp are supported:
   * <ul>
   *   <li>'yyyy-mm-dd hh:mm:ss[.fffffffff]'</li>
   *   <li>'yyyy-MM-dd[ HH:mm[:ss]]'</li>
   *   <li>Millis since epoch</li>
   *   <li>ISO8601 format</li>
   * </ul>
   */
  public static Timestamp toTimestamp(String timestampString) {
    try {
      return Timestamp.valueOf(timestampString);
    } catch (Exception e) {
      // Try the next format
    }
    try {
      return new Timestamp(Long.parseLong(timestampString));
    } catch (Exception e) {
    }
    try {
      return Timestamp.from(ZonedDateTime.parse(timestampString, UNIVERSAL_DATE_TIME_FORMATTER).toInstant());
    } catch (Exception e) {
      // Try the next format
    }
    try {
      LocalDateTime dateTime = LocalDateTime.parse(timestampString, UNIVERSAL_DATE_TIME_FORMATTER);
      return Timestamp.valueOf(dateTime);
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
    }
  }

  public static LocalDateTime toLocalDateTime(String timestampString) {
    try {
      return LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(timestampString)), ZoneId.of("UTC"));
    } catch (Exception e) {
    }
    try {
      return LocalDateTime.parse(timestampString, UNIVERSAL_DATE_TIME_FORMATTER);
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
    }
  }

  /**
   * Parses the given timestamp string into millis since epoch.
   * <p>Below formats of timestamp are supported:
   * <ul>
   *   <li>'yyyy-mm-dd hh:mm:ss[.fffffffff]'</li>
   *   <li>'yyyy-MM-dd[ HH:mm[:ss]]'</li>
   *   <li>Millis since epoch</li>
   *   <li>ISO8601 format</li>
   * </ul>
   */
  public static long toMillisSinceEpoch(String timestampString) {
    try {
      return Timestamp.valueOf(timestampString).getTime();
    } catch (Exception e) {
      // Try the next format
    }
    try {
      return Long.parseLong(timestampString);
    } catch (Exception e) {
      // Try the next format
    }
    try {
      return ZonedDateTime.parse(timestampString, UNIVERSAL_DATE_TIME_FORMATTER).toInstant().toEpochMilli();
    } catch (Exception e) {
      // Try the next format
    }
    try {
      LocalDateTime dateTime = LocalDateTime.parse(timestampString, UNIVERSAL_DATE_TIME_FORMATTER);
      return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
    }
  }

  public static long toMillsWithoutTimeZone(String timestampString) {
    try {
      return Long.parseLong(timestampString);
    } catch (Exception e) {
      // Try the next format
    }
    try {
      LocalDateTime dateTime = LocalDateTime.parse(timestampString, UNIVERSAL_DATE_TIME_FORMATTER);
      return dateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
    }
  }
}
