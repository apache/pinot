/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


public class JodaDateTimeUtils {
  private static DateTimeFormatter ISO_DATETIME_FORMATTER = ISODateTimeFormat.dateTimeParser();

  /**
   * Parse the joda DateTime instance to ISO string, e.g. 2017-05-31T00:00:00-07:00
   * @param dateTime
   *      A joda DateTime instance
   * @return
   *      An ISO DateTime String
   */
  public static String toIsoDateTimeString(DateTime dateTime){
    return dateTime.toString(ISO_DATETIME_FORMATTER);
  }

  /**
   * Parse the ISO DateTime String to a joda DateTime instance
   * @param isoDateTimeString
   *      The ISO DateTime String, e.g. 2017-05-31T00:00:00-07:00
   * @return
   *      A joda DateTime instance
   */
  public static DateTime toDateTime(String isoDateTimeString) {
    return ISO_DATETIME_FORMATTER.parseDateTime(isoDateTimeString);
  }
}
