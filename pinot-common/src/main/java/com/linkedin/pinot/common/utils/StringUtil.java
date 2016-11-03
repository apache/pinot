/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils;

import org.apache.commons.lang.StringUtils;

/**
 * Sep 30, 2014
 */

public class StringUtil {

  private static final String EMPTY_STRING = "";

  public static String join(String seperator, String...keys) {
    return StringUtils.join(keys, seperator);
  }

  /**
   * Trim trailing null characters from a string.
   * @param input Input to trim
   * @return Trimmed input
   */
  public static String trimTrailingNulls(String input) {
    if (input == null) {
      return input;
    }

    int origEnd = input.length() - 1;
    int end = origEnd;
    while (end >= 0 && input.charAt(end) == '\0') {
      end--;
    }

    if (end == origEnd) {
      return input;
    } else if (end < 0) {
      return EMPTY_STRING;
    } else {
      return input.substring(0, end+1);
    }
  }
}
