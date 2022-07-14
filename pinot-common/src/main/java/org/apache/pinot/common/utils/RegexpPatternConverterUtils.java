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

/**
 * Utility for converting regex patterns.
 */
public class RegexpPatternConverterUtils {
  private RegexpPatternConverterUtils() {
  }

  /* Represents all metacharacters to be processed */
  public static final String[] REGEXP_METACHARACTERS =
      {"\\", "^", "$", ".", "{", "}", "[", "]", "(", ")", "*", "+", "?", "|", "<", ">", "-", "&", "/"};

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

    String escaped = escapeMetaCharacters(likePattern.substring(start, end));
    StringBuilder sb = new StringBuilder(escaped.length() + 2);
    sb.append(prefix);
    sb.append(escaped);
    sb.append(suffix);

    int i = 0;
    while (i < sb.length()) {
      char c = sb.charAt(i);
      if (c == '_') {
        sb.replace(i, i + 1, ".");
      } else if (c == '%') {
        sb.replace(i, i + 1, ".*");
        i++;
      }
      i++;
    }

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
   * Add escape characters before special characters
   */
  private static String escapeMetaCharacters(String pattern) {
    for (String metaCharacter : REGEXP_METACHARACTERS) {
      if (pattern.contains(metaCharacter)) {
        pattern = pattern.replace(metaCharacter, "\\" + metaCharacter);
      }
    }
    return pattern;
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
