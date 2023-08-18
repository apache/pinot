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

public class ExceptionUtils {

  private ExceptionUtils() {
  }

  public static String consolidateExceptionMessages(Throwable e) {
    StringBuilder sb = new StringBuilder();
    // No more than 10 causes
    int maxCauses = 10;
    while (e != null && maxCauses-- > 0) {
      sb.append(getStackTrace(e, 3));
      e = e.getCause();
    }
    return sb.toString();
  }

  public static String getStackTrace(Throwable e) {
    return getStackTrace(e, Integer.MAX_VALUE);
  }

  public static String getStackTrace(Throwable e, int maxLines) {
    return getStackTrace(e, maxLines, Integer.MAX_VALUE);
  }

  public static String getStackTrace(Throwable e, int maxLines, int maxLineLength) {
    StringBuilder sb = new StringBuilder();
    while (e != null) {
      sb.append(e.getMessage()).append("\n");
      for (StackTraceElement ste : e.getStackTrace()) {
        sb.append(ste.toString()).append("\n");
        if (maxLines-- <= 0) {
          return sb.toString();
        }
        if (sb.length() > maxLineLength) {
          return sb.toString();
        }
      }
      e = e.getCause();
    }
    return sb.toString();
  }
}
