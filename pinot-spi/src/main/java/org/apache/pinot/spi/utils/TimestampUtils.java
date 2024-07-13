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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;


public class TimestampUtils {
  private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd[ HH:mm[:ss]]")
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter();

  private TimestampUtils() {
  }

  /**
   * Parses the given timestamp string into {@link Timestamp}.
   * <p>Two formats of timestamp are supported:
   * <ul>
   *   <li>'yyyy-mm-dd hh:mm:ss[.fffffffff]'</li>
   *   <li>'yyyy-MM-dd[ HH:mm[:ss]]'</li>
   *   <li>Millis since epoch</li>
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
    } catch (Exception e1) {
      // Try the next format
    }
    try {
      LocalDateTime dateTime = LocalDateTime.parse(timestampString, DATE_TIME_FORMATTER);
      return Timestamp.valueOf(dateTime);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
    }
  }

  /**
   * Parses the given timestamp string into millis since epoch.
   * <p>Two formats of timestamp are supported:
   * <ul>
   *   <li>'yyyy-mm-dd hh:mm:ss[.fffffffff]'</li>
   *   <li>'yyyy-MM-dd[ HH:mm[:ss]]'</li>
   *   <li>Millis since epoch</li>
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
    } catch (Exception e1) {
      // Try the next format
    }
    try {
      LocalDateTime dateTime = LocalDateTime.parse(timestampString, DATE_TIME_FORMATTER);
      return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
    }
  }
}
