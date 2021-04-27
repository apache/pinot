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


public class TimestampUtils {

  /**
   * Parses the given timestamp string into {@link Timestamp}.
   * <p>Two formats of timestamp are supported:
   * <ul>
   *   <li>'yyyy-mm-dd hh:mm:ss[.fffffffff]'</li>
   *   <li>Millis since epoch</li>
   * </ul>
   */
  public static Timestamp toTimestamp(String timestampString) {
    try {
      return Timestamp.valueOf(timestampString);
    } catch (Exception e) {
      try {
        return new Timestamp(Long.parseLong(timestampString));
      } catch (Exception e1) {
        throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
      }
    }
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
    try {
      return Timestamp.valueOf(timestampString).getTime();
    } catch (Exception e) {
      try {
        return Long.parseLong(timestampString);
      } catch (Exception e1) {
        throw new IllegalArgumentException(String.format("Invalid timestamp: '%s'", timestampString));
      }
    }
  }
}
