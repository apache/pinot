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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;


public class TimestampUtils {

  /**
   * This format matches the JDBC time format default {@link Timestamp} but adds
   * timeZone information to it. Note that it will not work with milliseconds
   * (it requires the either seconds or nanoseconds)
   */
  private static final DateTimeFormatter JDBC_TIMESTAMP_WITH_ZONE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.nnnnnnnnn]VV");

  // these two formats below are used for printing, while the one above is used
  // for parsing
  private static final DateTimeFormatter DEFAULT_TIMESTAMP_WITH_TIME_ZONE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX");
  private static final DateTimeFormatter DEFAULT_TIMESTAMP_WITH_TIME_ZONE_NANO_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnnX");

  private TimestampUtils() {
  }

  /**
   * Parses the given timestamp string into {@link LocalDateTime}.
   * <p>Three formats of timestamp are supported:
   * <ul>
   *   <li>ISO-8601</li>
   *   <li>'yyyy-mm-dd hh:mm:ss[.fffffffff]'</li>
   *   <li>Millis since epoch</li>
   * </ul>
   */
  public static LocalDateTime toTimestamp(String timestampString) {
    try {
      return LocalDateTime.parse(timestampString);
    } catch (Exception e) {
      try {
        // this is a catch-all for the JDBC timestamp format
        return LocalDateTime.ofInstant(Timestamp.valueOf(timestampString).toInstant(), ZoneOffset.systemDefault());
      } catch (Exception e1) {
        try {
          return LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(timestampString)), ZoneOffset.UTC);
        } catch (Exception e2) {
          throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
        }
      }
    }
  }

  public static LocalDateTime toTimestamp(long epochMillis) {
    // if an epoch is passed in, we assume it's in UTC (unlike the toTimestamp(String) method,
    // which assumes that the date time is in the local time zone)
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
  }

  /**
   * Parses the given timestamp string into {@link java.time.OffsetDateTime}.
   * <p>Three formats of timestamp are supported:
   * <ul>
   *   <li>ISO-8601</li>
   *   <li>'yyyy-mm-ddThh:mm:ss[.fffffffff]±hh[:mm]'</li>
   *   <li>Millis since epoch</li>
   * </ul>
   */
  public static OffsetDateTime toTimestampWithTimeZone(String timestampString) {
    try {
      return OffsetDateTime.parse(timestampString);
    } catch (Exception e) {
      try {
        // attempt to parse non-standard JDBC-like format
        return OffsetDateTime.parse(timestampString, JDBC_TIMESTAMP_WITH_ZONE_FORMAT);
      } catch (Exception e1) {
        try {
          return OffsetDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(timestampString)), ZoneId.of("UTC"));
        } catch (Exception e2) {
          throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
        }
      }
    }
  }

  public static OffsetDateTime toTimestampWithTimeZone(long epochMillis) {
    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
  }

  /**
   * Parses the given timestamp string into millis since epoch.
   * <p>Two formats of timestamp are supported:
   * <ul>
   *   <li>'yyyy-mm-dd hh:mm:ss[.fffffffff]'</li>
   *   <li>Millis since epoch</li>
   * </ul>
   */
  public static long toMillisSinceEpoch(String timestampString) {
    return toTimestamp(timestampString).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public static long toMillisSinceEpoch(LocalDateTime timestamp) {
    return timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public static long toMillisSinceEpoch(OffsetDateTime timestamp) {
    return timestamp.toInstant().toEpochMilli();
  }

  /**
   * Parses the given timestamp with TZ string into millis since epoch.
   * <p>Two formats of timestamp are supported:
   * <ul>
   *   <li>'yyyy-mm-ddThh:mm:ss[.fffffffff]±hh[:mm]'</li>
   *   <li>Millis since epoch</li>
   * </ul>
   */
  public static long toMillisSinceEpochWithTimeZone(String timestampString) {
    return toTimestampWithTimeZone(timestampString).toInstant().toEpochMilli();
  }

  public static String format(LocalDateTime timestamp) {
    // for backwards compatibility, we use JDBC string format for
    // timestamps instead of ISO-8601
    return Timestamp.valueOf(timestamp).toString();
  }

  public static String format(OffsetDateTime timestamp) {
    if (timestamp.getNano() != 0) {
      return DEFAULT_TIMESTAMP_WITH_TIME_ZONE_NANO_FORMAT.format(timestamp);
    } else {
      return DEFAULT_TIMESTAMP_WITH_TIME_ZONE_FORMAT.format(timestamp);
    }
  }
}
