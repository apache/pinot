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
 * Optimized implementation of regexp_extract that assumes pattern is constant.
 */
public class RegexpExtractConstFunctions {

  private Matcher _matcher;

  /**
   * @param value
   * @param regexp
   * @return the matched result.
   */
  @ScalarFunction
  public String regexpExtractConst(String value, String regexp) {
    return regexpExtractConst(value, regexp, 0, "");
  }

  /**
   * @param value
   * @param regexp
   * @param group
   * @return the matched result.
   */
  @ScalarFunction
  public String regexpExtractConst(String value, String regexp, int group) {
    return regexpExtractConst(value, regexp, group, "");
  }

  /**
   * Regular expression that extract first matched substring.
   *
   * @param value        input value
   * @param regexp       regular expression
   * @param group        the group number within the regular expression to extract.
   * @param defaultValue the default value if no match found
   * @return the matched result
   */
  @ScalarFunction
  public String regexpExtractConst(String value, String regexp, int group, String defaultValue) {
    if (_matcher == null) {
      Pattern p = Pattern.compile(regexp);
      _matcher = p.matcher("");
    }

    _matcher.reset(value);
    if (_matcher.find() && _matcher.groupCount() >= group) {
      return _matcher.group(group);
    } else {
      return defaultValue;
    }
  }
}
