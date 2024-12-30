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
package org.apache.pinot.client.utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;


public class DateTimeUtils {
  private DateTimeUtils() {
  }

  private static final String TIMESTAMP_FORMAT_STR = "yyyy-MM-dd HH:mm:ss";
  private static final String DATE_FORMAT_STR = "yyyy-MM-dd";
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_FORMAT_STR);
  private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT_STR);

  public static Date getDateFromString(String value, Calendar cal) {
    // Parse the input string to a LocalDate
    LocalDate localDate = LocalDate.parse(value, DATE_FORMATTER);

    // Convert LocalDate to a java.sql.Date, using the Calendar's time zone
    ZoneId zoneId = cal.getTimeZone().toZoneId();
    return new Date(localDate.atStartOfDay(zoneId).toInstant().toEpochMilli());
  }

  public static Time getTimeFromString(String value, Calendar cal) {
    // Parse the input string to a LocalTime
    LocalTime localTime = LocalTime.parse(value, TIMESTAMP_FORMATTER);

    // Convert LocalTime to java.sql.Time, considering the Calendar's time zone
    ZoneId zoneId = cal.getTimeZone().toZoneId();
    return new Time(localTime.atDate(LocalDate.ofEpochDay(0)).atZone(zoneId).toInstant().toEpochMilli());
  }

  public static Timestamp getTimestampFromString(String value, Calendar cal) {
    // Parse the input string to a LocalDateTime
    LocalDateTime localDateTime = LocalDateTime.parse(value, TIMESTAMP_FORMATTER);

    // Convert LocalDateTime to java.sql.Timestamp, considering the Calendar's time zone
    ZoneId zoneId = cal.getTimeZone().toZoneId();
    return new Timestamp(localDateTime.atZone(zoneId).toInstant().toEpochMilli());
  }

  public static Timestamp getTimestampFromLong(Long value) {
    return new Timestamp(value);
  }

  public static String dateToString(Date date) {
    return date.toLocalDate().format(DATE_FORMATTER);
  }

  public static String timeToString(Time time) {
    return TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(time.getTime()).atZone(ZoneId.systemDefault()));
  }

  public static String timeStampToString(Timestamp timestamp) {
    return TIMESTAMP_FORMATTER.format(Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneId.systemDefault()));
  }

  public static long timeStampToLong(Timestamp timestamp) {
    return timestamp.getTime();
  }
}
