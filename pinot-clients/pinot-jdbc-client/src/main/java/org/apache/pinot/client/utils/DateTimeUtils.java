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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class DateTimeUtils {
  private DateTimeUtils() {
  }

  private static final String TIMESTAMP_FORMAT_STR = "yyyy-MM-dd HH:mm:ss";
  private static final String DATE_FORMAT_STR = "yyyy-MM-dd";
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_STR);
  private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat(TIMESTAMP_FORMAT_STR);

  public static Date getDateFromString(String value, Calendar cal)
      throws ParseException {
    DATE_FORMAT.setTimeZone(cal.getTimeZone());
    java.util.Date date = DATE_FORMAT.parse(value);
    Date sqlDate = new Date(date.getTime());
    return sqlDate;
  }

  public static Time getTimeFromString(String value, Calendar cal)
      throws ParseException {
    TIMESTAMP_FORMAT.setTimeZone(cal.getTimeZone());
    java.util.Date date = TIMESTAMP_FORMAT.parse(value);
    Time sqlTime = new Time(date.getTime());
    return sqlTime;
  }

  public static Timestamp getTimestampFromString(String value, Calendar cal)
      throws ParseException {
    TIMESTAMP_FORMAT.setTimeZone(cal.getTimeZone());
    java.util.Date date = TIMESTAMP_FORMAT.parse(value);
    Timestamp sqlTime = new Timestamp(date.getTime());
    return sqlTime;
  }

  public static Timestamp getTimestampFromLong(Long value) {
    Timestamp sqlTime = new Timestamp(value);
    return sqlTime;
  }

  public static String dateToString(Date date) {
    return DATE_FORMAT.format(date.getTime());
  }

  public static String timeToString(Time time) {
    return TIMESTAMP_FORMAT.format(time.getTime());
  }

  public static String timeStampToString(Timestamp timestamp) {
    return TIMESTAMP_FORMAT.format(timestamp.getTime());
  }

  public static long timeStampToLong(Timestamp timestamp) {
    return timestamp.getTime();
  }
}
