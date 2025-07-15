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

import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
import org.apache.pinot.common.utils.regex.Matcher;
import org.apache.pinot.common.utils.regex.Pattern;
import org.apache.pinot.common.utils.regex.PatternFactory;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Optimized regexp_like implementation that assumes that pattern is constant.
 */
public class RegexpLikeConstFunctions {

  private Matcher _matcher;

  @ScalarFunction
  public boolean regexpLike(String inputStr, String regexPatternStr) {
    if (_matcher == null) {
      Pattern p = PatternFactory.compile(regexPatternStr);
      _matcher = p.matcher("");
    }

    return _matcher.reset(inputStr).find();
  }

  @ScalarFunction
  public boolean regexpLike(String inputStr, String regexPatternStr, String matchParameter) {
    if (_matcher == null) {
      Pattern p = buildPattern(regexPatternStr, matchParameter);
      _matcher = p.matcher("");
    }

    return _matcher.reset(inputStr).find();
  }

  private Pattern buildPattern(String pattern, String matchParameter) {
    // Validate that all characters in matchParameter are supported
    for (char c : matchParameter.toCharArray()) {
      if (c != 'i' && c != 'c') {
        throw new IllegalArgumentException(
            "Unsupported match parameter: '" + c + "'. Only 'i' (case-insensitive) and "
                + "'c' (case-sensitive) are supported.");
      }
    }

    // Validate that we don't have conflicting flags (both 'i' and 'c')
    if (matchParameter.contains("i") && matchParameter.contains("c")) {
      throw new IllegalArgumentException(
          "Invalid match parameter: '" + matchParameter + "'. Cannot specify both 'i' (case-insensitive) and "
              + "'c' (case-sensitive) flags.");
    }

    // Check for case-insensitive flag
    if (matchParameter.contains("i")) {
      return PatternFactory.compileCaseInsensitive(pattern);
    }
    // Default to case-sensitive
    return PatternFactory.compile(pattern);
  }

  @ScalarFunction
  public boolean like(String inputStr, String likePatternStr) {
    if (_matcher == null) {
      String regexPatternStr = RegexpPatternConverterUtils.likeToRegexpLike(likePatternStr);
      Pattern p = PatternFactory.compile(regexPatternStr);
      _matcher = p.matcher("");
    }

    return _matcher.reset(inputStr).find();
  }
}
