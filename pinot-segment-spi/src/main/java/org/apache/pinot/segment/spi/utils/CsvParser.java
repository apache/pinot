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
package org.apache.pinot.segment.spi.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class CsvParser {
  private CsvParser() {
    // Hide utility class default constructor
  }

  /**
   * Parse the input csv string with customizable parsing behavior. Sometimes the individual values may contain comma
   * and other white space characters. These characters are sometimes expected to be part of the actual argument.
   *
   * @param input  string to split on comma
   * @param escapeComma if true, we don't split on escaped commas, and we replace "\," with "," after the split
   * @param trim   whether we should trim each tokenized terms
   * @return a list of values, empty list if input is empty or null
   */
  public static List<String> parse(@Nullable String input, boolean escapeComma, boolean trim) {
    if (null == input || input.isEmpty()) {
      return Collections.emptyList();
    }

    Stream<String> tokenStream;
    if (escapeComma) {
      // Use regular expression to split on "," unless it is "\,"
      // Use a non-positive limit to apply the replacement as many times as possible and to ensure trailing empty
      // strings shall not be discarded
      tokenStream = Arrays.stream(input.split("(?<!\\\\),", -1))
          .map(s -> s.replace("\\,", ","));
    } else {
      tokenStream = Arrays.stream(input.split(","));
    }

    if (trim) {
      tokenStream = tokenStream.map(String::trim);
    }

    return tokenStream.collect(Collectors.toList());
  }

  /**
   * Parse the input list of string with customized serialization behavior.
   * @param input containing a list of string to be serialized
   * @param escapeComma if true, escape commas by replacing "," with "\," before the join
   * @param trim whether we should trim each tokenized terms before serialization
   * @return serialized string representing the input list of string
   */
  public static String serialize(List<String> input, boolean escapeComma, boolean trim) {
    Stream<String> tokenStream = input.stream();
    if (escapeComma) {
      tokenStream = tokenStream.map(s -> s.replaceAll(",", Matcher.quoteReplacement("\\,")));
    }
    if (trim) {
      tokenStream = tokenStream.map(String::trim);
    }
    return tokenStream.collect(Collectors.joining(","));
  }
}
