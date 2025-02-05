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
package org.apache.pinot.common.function.scalar.regexp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Optimized regexp_replace implementations that assume pattern is constant.
 */
public class RegexpReplaceConstFunctions {

  private final StringBuilder _buffer = new StringBuilder();
  private Pattern _pattern;
  private Matcher _matcher;

  /**
   * Replace a regular expression pattern. If matchStr is not found, inputStr will be returned. By default, all
   * occurrences of match pattern in the input string will be replaced. Default matching pattern is case sensitive.
   *
   * @param inputStr      Input string to apply the regexpReplace
   * @param matchStr      Regexp or string to match against inputStr
   * @param replaceStr    Regexp or string to replace if matchStr is found
   * @param matchStartPos Index of inputStr from where matching should start. Default is 0.
   * @param occurrence    Controls which occurrence of the matched pattern must be replaced. Counting starts at 0.
   *                      Default is -1
   * @param flag          Single character flag that controls how the regex finds matches in inputStr. If an
   *                      incorrect flag
   *                      is specified, the function applies default case-sensitive match. Only one flag can be
   *                      specified.
   *                      Supported flags:
   *                      i -> Case insensitive
   * @return replaced input string
   */
  @ScalarFunction
  public String regexpReplace(String inputStr, String matchStr, String replaceStr, int matchStartPos,
      int occurrence, String flag) {
    if (_pattern == null) {
      int patternFlag = "i".equals(flag) ? Pattern.CASE_INSENSITIVE : 0;
      _pattern = Pattern.compile(matchStr, patternFlag);
      _matcher = _pattern.matcher("");
    }

    _matcher.reset(inputStr).region(matchStartPos, inputStr.length());

    if (occurrence >= 0) {
      _buffer.setLength(0);
      _buffer.append(inputStr);
      while (occurrence >= 0 && _matcher.find()) {
        if (occurrence == 0) {
          _buffer.replace(_matcher.start(), _matcher.end(), replaceStr);
          break;
        }
        occurrence--;
      }
    } else {
      _buffer.setLength(0);
      while (_matcher.find()) {
        _matcher.appendReplacement(_buffer, replaceStr);
      }
      _matcher.appendTail(_buffer);
    }

    return _buffer.toString();
  }

  /**
   * See #regexpReplace(String, String, String, int, int, String). Matches against entire inputStr and replaces all
   * occurrences. Match is performed in case-sensitive mode.
   *
   * @param inputStr   Input string to apply the regexpReplace
   * @param matchStr   Regexp or string to match against inputStr
   * @param replaceStr Regexp or string to replace if matchStr is found
   * @return replaced input string
   */
  @ScalarFunction
  public String regexpReplace(String inputStr, String matchStr, String replaceStr) {
    return regexpReplace(inputStr, matchStr, replaceStr, 0, -1, "");
  }

  /**
   * See #regexpReplace(String, String, String, int, int, String). Matches against entire inputStr and replaces all
   * occurences. Match is performed in case-sensitive mode.
   *
   * @param inputStr      Input string to apply the regexpReplace
   * @param matchStr      Regexp or string to match against inputStr
   * @param replaceStr    Regexp or string to replace if matchStr is found
   * @param matchStartPos Index of inputStr from where matching should start. Default is 0.
   * @return replaced input string
   */
  @ScalarFunction
  public String regexpReplace(String inputStr, String matchStr, String replaceStr, int matchStartPos) {
    return regexpReplace(inputStr, matchStr, replaceStr, matchStartPos, -1, "");
  }

  /**
   * See #regexpReplace(String, String, String, int, int, String). Match is performed in case-sensitive mode.
   *
   * @param inputStr      Input string to apply the regexpReplace
   * @param matchStr      Regexp or string to match against inputStr
   * @param replaceStr    Regexp or string to replace if matchStr is found
   * @param matchStartPos Index of inputStr from where matching should start. Default is 0.
   * @param occurrence    Controls which occurrence of the matched pattern must be replaced. Counting starts
   *                      at 0. Default is -1
   * @return replaced input string
   */
  @ScalarFunction
  public String regexpReplace(String inputStr, String matchStr, String replaceStr, int matchStartPos,
      int occurrence) {
    return regexpReplace(inputStr, matchStr, replaceStr, matchStartPos, occurrence, "");
  }
}
