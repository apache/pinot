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

import com.google.common.base.Preconditions;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Utility class to convert between human readable data size (e.g. '10.5G', '40B') and data size in bytes.
 */
public class DataSizeUtils {
  private DataSizeUtils() {
  }

  private static final Pattern DATA_SIZE_PATTERN =
      Pattern.compile("^(\\d+(\\.\\d+)?)([KMGTP])?(B)?$", Pattern.CASE_INSENSITIVE);
  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.##");

  private static double KB_IN_BYTES = 1024;
  private static double MB_IN_BYTES = KB_IN_BYTES * 1024;
  private static double GB_IN_BYTES = MB_IN_BYTES * 1024;
  private static double TB_IN_BYTES = GB_IN_BYTES * 1024;
  private static double PB_IN_BYTES = TB_IN_BYTES * 1024;

  /**
   * Converts human readable data size (e.g. '10.5G', '40B') to data size in bytes.
   */
  public static long toBytes(String dataSizeString) {
    Matcher matcher = DATA_SIZE_PATTERN.matcher(dataSizeString);
    Preconditions.checkArgument(matcher.matches(), "Illegal data size: %s", dataSizeString);

    double value = Double.parseDouble(matcher.group(1));
    String unit = matcher.group(3);
    if (unit != null) {
      switch (unit.toUpperCase()) {
        case "K":
          return (long) (value * KB_IN_BYTES);
        case "M":
          return (long) (value * MB_IN_BYTES);
        case "G":
          return (long) (value * GB_IN_BYTES);
        case "T":
          return (long) (value * TB_IN_BYTES);
        case "P":
          return (long) (value * PB_IN_BYTES);
        default:
          throw new IllegalStateException();
      }
    } else {
      return (long) value;
    }
  }

  /**
   * Converts data size in bytes to human readable data size (e.g. '5B', '1.24M', '3T').
   */
  public static String fromBytes(long dataSizeInBytes) {
    if (dataSizeInBytes < KB_IN_BYTES) {
      return dataSizeInBytes + "B";
    }
    if (dataSizeInBytes < MB_IN_BYTES) {
      return DECIMAL_FORMAT.format(dataSizeInBytes / KB_IN_BYTES) + "K";
    }
    if (dataSizeInBytes < GB_IN_BYTES) {
      return DECIMAL_FORMAT.format(dataSizeInBytes / MB_IN_BYTES) + "M";
    }
    if (dataSizeInBytes < TB_IN_BYTES) {
      return DECIMAL_FORMAT.format(dataSizeInBytes / GB_IN_BYTES) + "G";
    }
    if (dataSizeInBytes < PB_IN_BYTES) {
      return DECIMAL_FORMAT.format(dataSizeInBytes / TB_IN_BYTES) + "T";
    }
    return DECIMAL_FORMAT.format(dataSizeInBytes / PB_IN_BYTES) + "P";
  }
}
