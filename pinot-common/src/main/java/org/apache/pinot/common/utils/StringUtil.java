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

import org.apache.commons.lang.StringUtils;


// TODO: Use pinot-spi StringUtils instead
public class StringUtil {
  private StringUtil() {
  }

  // prefer string to character because String.indexOf(String) is a fast intrinsic on all JDK versions
  private static final String NULL_CHARACTER = "\0";

  /**
   * Joins the given keys with the separator.
   */
  public static String join(String separator, String... keys) {
    return StringUtils.join(keys, separator);
  }

  /**
   * Sanitizes a string value.
   * <ul>
   *   <li>Truncate characters after the first {@code null} character as it is reserved as the padding character</li>
   *   <li>Limit the length of the string</li>
   * </ul>
   *
   * @param value String value to sanitize
   * @param maxLength Max number of characters allowed
   * @return Modified value, or value itself if not modified
   */
  public static String sanitizeStringValue(String value, int maxLength) {
    int index = value.indexOf(NULL_CHARACTER);
    if (index < 0) {
      return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
    return value.substring(0, Math.min(index, maxLength));
  }
}
