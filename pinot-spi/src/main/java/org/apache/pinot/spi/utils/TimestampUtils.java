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
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class TimestampUtils {

  private static final String[] SDF_FORMATS = {
      "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ssZ",
      "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "yyyy-MM-dd HH:mm:ss",
      "MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy'T'HH:mm:ss.SSS'Z'",
      "MM/dd/yyyy'T'HH:mm:ss.SSSZ", "MM/dd/yyyy'T'HH:mm:ss.SSS",
      "MM/dd/yyyy'T'HH:mm:ssZ", "MM/dd/yyyy'T'HH:mm:ss",
      "yyyy:MM:dd HH:mm:ss", "yyyyMMdd", "MM/dd/yyyy"
  };

  private TimestampUtils() {
  }

  /**
   * Parses the given timestamp string into {@link Timestamp}.
   */
  public static Timestamp toTimestamp(String timestampString) {
    try {
      return Timestamp.valueOf(timestampString);
    } catch (Exception e) {
      try {
        return new Timestamp(Long.parseLong(timestampString));
      } catch (Exception e1) {
        try {
          return dateTimeToTimestamp(timestampString);
        } catch (Exception e2) {
          throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
        }
      }
    }
  }

  /**
   * Parses the given timestamp string into millis since epoch.
   */
  public static long toMillisSinceEpoch(String timestampString) {
    try {
      return Timestamp.valueOf(timestampString).getTime();
    } catch (Exception e) {
      try {
        return Long.parseLong(timestampString);
      } catch (Exception e1) {
        try {
          return dateTimeToTimestamp(timestampString).getTime();
        } catch (Exception e2) {
          throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
        }
      }
    }
  }

  /**
   * Infers a date time format from a valid {@link Timestamp} string.
   */
  private static Timestamp dateTimeToTimestamp(String timestampString) {
    Timestamp result = null;
    for (String parse : SDF_FORMATS) {
      SimpleDateFormat sdf = new SimpleDateFormat(parse);
      try {
        result = Timestamp.from(sdf.parse(timestampString).toInstant());
      } catch (ParseException e) {
      }
    }
    if (result == null) {
      throw new IllegalArgumentException(String.format("Date format not recognized: '%s'", timestampString));
    }
    return result;
  }
}