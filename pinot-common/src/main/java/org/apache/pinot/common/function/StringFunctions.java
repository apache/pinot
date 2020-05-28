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
package org.apache.pinot.common.function;


import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.annotations.ScalarFunction;


/**
 * Inbuilt string related transform functions
 *
 */
public class StringFunctions {
  private final static Pattern LTRIM = Pattern.compile("^\\s+");
  private final static Pattern RTRIM = Pattern.compile("\\s+$");

  @ScalarFunction
  static String reverse(String input) {
    return StringUtils.reverse(input);
  }

  @ScalarFunction
  static String lower(String input) {
    return input.toLowerCase();
  }

  @ScalarFunction
  static String upper(String input) {
    return input.toUpperCase();
  }

  @ScalarFunction
  static String substr(String input, Integer beginIndex) {
    return input.substring(beginIndex);
  }

  @ScalarFunction
  static String substr(String input, Integer beginIndex, Integer endIndex) {
    if (endIndex == -1) {
      return substr(input, beginIndex);
    }
    return input.substring(beginIndex, endIndex);
  }

  @ScalarFunction
  static String concat(String input1, String input2, String seperator) {
    String result = input1;
    result = result + seperator + input2;
    return result;
  }

  @ScalarFunction
  static String trim(String input) {
    return input.trim();
  }

  @ScalarFunction
  static String ltrim(String input) {
    return LTRIM.matcher(input).replaceAll("");
  }

  @ScalarFunction
  static String rtrim(String input) {
    return RTRIM.matcher(input).replaceAll("");
  }

  @ScalarFunction
  static Integer length(String input) {
    return input.length();
  }

  @ScalarFunction
  static Integer strpos(String input, String find, Integer instance) {
    return StringUtils.ordinalIndexOf(input, find, instance);
  }

  @ScalarFunction
  static Boolean startsWith(String input, String prefix) {
    return input.startsWith(prefix);
  }

  @ScalarFunction
  static String replace(String input, String find, String substitute) {
    return input.replaceAll(find, substitute);
  }

  @ScalarFunction
  static String rpad(String input, Integer size, String pad) {
    return StringUtils.rightPad(input, size, pad);
  }

  @ScalarFunction
  static String lpad(String input, Integer size, String pad) {
    return StringUtils.leftPad(input, size, pad);
  }

  @ScalarFunction
  static Integer codepoint(String input) {
    return input.codePointAt(0);
  }

  @ScalarFunction
  static String chr(Integer codepoint) {
    char[] result = Character.toChars(codepoint);
    return new String(result);
  }
}
