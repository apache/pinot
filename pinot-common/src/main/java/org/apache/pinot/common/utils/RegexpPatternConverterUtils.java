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


import com.google.common.primitives.Chars;

/**
 * Utility for converting regex patterns.
 */
public class RegexpPatternConverterUtils {
  private RegexpPatternConverterUtils() {
  }

  /*
   * Represents all metacharacters to be processed.
   * This excludes the \ (back slash) character as that doubles up as an escape character as well.
   * So it is handled separately in the conversion logic.
   */
  public static final char[] REGEXP_METACHARACTERS = new char[]{
          '^', '$', '.', '{', '}', '[', ']', '(', ')', '*', '+', '?', '|', '<', '>', '-', '&', '/'};
  public static final char BACK_SLASH = '\\';

  /**
   * Converts a LIKE pattern into REGEXP_LIKE pattern.
   */
  public static String likeToRegexpLike(String likePattern) {
    int start = 0;
    int end = likePattern.length();
    String prefix = "^";
    String suffix = "$";
    switch (likePattern.length()) {
      case 0:
        return "^$";
      case 1:
        if (likePattern.charAt(0) == '%') {
          return "";
        }
        break;
      default:
        if (likePattern.charAt(0) == '%') {
          start = indexOfFirstDifferent(likePattern, '%');
          if (start == -1) {
            return "";
          }
          prefix = "";
        }
        if (likePattern.charAt(likePattern.length() - 1) == '%') {
          end = indexOfLastDifferent(likePattern, '%');
          if (end == -1) { //this should never happen, but for clarity
            return "";
          }
          end++;
          suffix = "";
        }
        break;
    }

    likePattern = likePattern.substring(start, end);
    return escapeMetaCharsAndWildcards(likePattern, prefix, suffix);
  }

  /**
   * Escapes the provided pattern by considering the following constraints:
   * <ul>
   *     <li> SQL wildcards escaping is handled (_, %) </li>
   *     <li> Regex meta characters escaping is handled </li>
   * </ul>
   * @param input the provided input string
   * @param prefix the prefix to be added to the output string
   * @param suffix the suffix to be added to the output string
   * @return the final output string
   */
  private static String escapeMetaCharsAndWildcards(String input, String prefix, String suffix) {
    StringBuilder sb = new StringBuilder();
    sb.append(prefix);
    // handling SQL wildcards (_, %) by replacing them with corresponding regex equivalents
    // we ignore them if the SQL wildcards are escaped
    int i = 0;
    int len = input.length();
    boolean isPrevCharBackSlash = false;
    while (i < len) {
      char c = input.charAt(i);
      switch (c) {
        case '_':
          sb.append(isPrevCharBackSlash ? c : ".");
          break;
        case '%':
          sb.append(isPrevCharBackSlash ? c : ".*");
          break;
        default:
          // either the current character is a meta-character
          // OR
          // this means the previous character is a \
          // but it was not used for escaping SQL wildcards
          // so let's escape this \ in the output
          // this case is separately handled outside the meta characters list
          if (Chars.indexOf(REGEXP_METACHARACTERS, c) >= 0 || isPrevCharBackSlash) {
            sb.append(BACK_SLASH);
          }
          sb.append(c);
          break;
      }
      isPrevCharBackSlash = (c == BACK_SLASH);
      i++;
    }

    // handle trailing \
    if (isPrevCharBackSlash) {
      sb.append(BACK_SLASH);
    }

    sb.append(suffix);
    return sb.toString();
  }

  private static int indexOfFirstDifferent(String str, char character) {
    for (int i = 0; i < str.length(); i++) {
      if (str.charAt(i) != character) {
        return i;
      }
    }
    return -1;
  }

  private static int indexOfLastDifferent(String str, char character) {
    for (int i = str.length() - 1; i >= 0; i--) {
      if (str.charAt(i) != character) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Converts a REGEXP_LIKE pattern into Lucene REGEXP pattern.
   */
  public static String regexpLikeToLuceneRegExp(String regexpLikePattern) {
    if (regexpLikePattern.isEmpty()) {
      return regexpLikePattern;
    }
    if (regexpLikePattern.charAt(0) == '^') {
      regexpLikePattern = regexpLikePattern.substring(1);
    } else {
      regexpLikePattern = ".*" + regexpLikePattern;
    }
    int length = regexpLikePattern.length();
    if (regexpLikePattern.charAt(length - 1) == '$') {
      regexpLikePattern = regexpLikePattern.substring(0, length - 1);
    } else {
      regexpLikePattern = regexpLikePattern + ".*";
    }
    return regexpLikePattern;
  }
}
