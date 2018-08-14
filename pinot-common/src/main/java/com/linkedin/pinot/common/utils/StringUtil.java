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
package com.linkedin.pinot.common.utils;

import java.io.UnsupportedEncodingException;
import javax.annotation.Nonnull;
import org.apache.commons.lang.StringUtils;


public class StringUtil {
  private static final char NULL_CHARACTER = '\0';
  private static String charSet = "UTF-8";

  /**
   * Joins the given keys with the separator.
   */
  public static String join(String separator, String... keys) {
    return StringUtils.join(keys, separator);
  }

  /**
   * Returns whether the string contains null character.
   */
  public static boolean containsNullCharacter(@Nonnull String input) {
    return input.indexOf(NULL_CHARACTER) >= 0;
  }

  /**
   * Removes the null characters from a string.
   */
  public static String removeNullCharacters(@Nonnull String input) {
    if (!containsNullCharacter(input)) {
      return input;
    }

    char[] chars = input.toCharArray();
    int length = chars.length;
    int index = 0;
    for (int i = 0; i < length; i++) {
      if (chars[i] != NULL_CHARACTER) {
        chars[index++] = chars[i];
      }
    }
    return new String(chars, 0, index);
  }

  public static byte[] encodeUtf8(String s) {
    try {
      return s.getBytes(charSet);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String decodeUtf8(byte[] bytes) {
    return decodeUtf8(bytes, 0, bytes.length);
  }

  public static String decodeUtf8(byte[] bytes, int startIndex, int endIndex) {
    try {
      return new String(bytes, startIndex, endIndex, charSet);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
