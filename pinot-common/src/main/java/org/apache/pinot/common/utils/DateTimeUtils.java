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
package org.apache.pinot.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;


public class DateTimeUtils {
  private DateTimeUtils() {
  }

  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss z";
  private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT, Locale.getDefault());

  static {
    SIMPLE_DATE_FORMAT.setTimeZone(TimeZone.getDefault());
  }

  /**
   * Utility function to convert epoch in millis to SDF of form "yyyy-MM-dd HH:mm:ss z".
   *
   * @param millisSinceEpoch Time in millis to convert
   * @return SDF equivalent
   */
  public static String epochToDefaultDateFormat(long millisSinceEpoch) {
    return SIMPLE_DATE_FORMAT.format(new Date(millisSinceEpoch));
  }
}
