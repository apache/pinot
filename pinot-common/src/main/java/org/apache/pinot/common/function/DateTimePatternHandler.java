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

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * Handles DateTime conversions from long to strings and strings to longs based on passed patterns
 */
public class DateTimePatternHandler {
  /**
   * Converts the dateTimeString of passed pattern into a long of the millis since epoch
   */
  public static long parseDateTimeStringToEpochMillis(String dateTimeString, String pattern) {
    DateTimeFormatter dateTimeFormatter = getDateTimeFormatter(pattern);
    return dateTimeFormatter.parseMillis(dateTimeString);
  }

  /**
   * Converts the millis representing seconds since epoch into a string of passed pattern
   */
  public static String parseEpochMillisToDateTimeString(long millis, String pattern) {
    DateTimeFormatter dateTimeFormatter = getDateTimeFormatter(pattern);
    return dateTimeFormatter.print(millis);
  }

  private static DateTimeFormatter getDateTimeFormatter(String pattern) {
    // Note: withZoneUTC is overwritten if the timezone is specified directly in the pattern
    // This also leverages an internal cache so it won't generate a new DateTimeFormatter for every row with
    // the same pattern
    return DateTimeFormat.forPattern(pattern).withZoneUTC();
  }
}
