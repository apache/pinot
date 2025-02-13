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
      _matcher = PatternFactory.compile(regexPatternStr).matcher("");
    }

    return _matcher.reset(inputStr).find();
  }

  @ScalarFunction
  public boolean like(String inputStr, String likePatternStr) {
    if (_matcher == null) {
      String regexPatternStr = RegexpPatternConverterUtils.likeToRegexpLike(likePatternStr);
      _matcher = PatternFactory.compile(regexPatternStr).matcher("");
    }

    return _matcher.reset(inputStr).find();
  }
}
