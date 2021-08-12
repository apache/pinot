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
 * Utility for converting LIKE operator syntax to a regex
 */
public class LikeToRegexpLikePatternConverterUtils {
  /* Represents all metacharacters to be processed */
  public static final String[] REGEXP_METACHARACTERS =
      {"\\", "^", "$", ".", "{", "}", "[", "]", "(", ")", "*", "+", "?", "|", "<", ">", "-", "&", "/"};

  /**
   * Process an incoming LIKE string and make it regexp friendly
   * @param value LIKE operator styled predicate
   * @return Result regex
   */
  public static String processValue(String value) {
    return escapeMetaCharacters(value).replace('_', '.').replace("%", ".*");
  }

  /**
   * Add escape characters before special characters
   */
  private static String escapeMetaCharacters(String inputString) {
    for (String metaCharacter : REGEXP_METACHARACTERS) {
      if (inputString.contains(metaCharacter)) {
        inputString = inputString.replace(metaCharacter, "\\" + metaCharacter);
      }
    }
    return inputString;
  }
}
