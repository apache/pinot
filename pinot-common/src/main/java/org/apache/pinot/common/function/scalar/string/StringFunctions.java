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
package org.apache.pinot.common.function.scalar.string;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Class contains a number of non-static scalar functions optimized by re-using various buffers.
 */
public class StringFunctions {

  private final StringBuilder _buffer = new StringBuilder();

  /**
   * Joins two input strings with separator in between.
   */
  @ScalarFunction
  public String concat(String input1, String input2, String separator) {
    _buffer.setLength(0);
    return _buffer.append(input1)
        .append(separator)
        .append(input2)
        .toString();
  }

  /**
   * Joins two input strings with separator in between.
   */
  @ScalarFunction
  public String concatWS(String separator, String input1, String input2) {
    _buffer.setLength(0);
    return _buffer.append(input1)
        .append(separator)
        .append(input2)
        .toString();
  }

  /**
   * @see Strings#replace(String, String, String)
   */
  @ScalarFunction
  public String replace(String text, String searchString, String replacement) {
    if (text.isEmpty() || searchString.isEmpty()) {
      return text;
    }
    int start = 0;
    int end = Strings.CS.indexOf(text, searchString, start);
    if (end == StringUtils.INDEX_NOT_FOUND) {
      return text;
    }
    final int replLength = searchString.length();
    int increase = Math.max(replacement.length() - replLength, 0) * 16;
    _buffer.setLength(0);
    _buffer.ensureCapacity(text.length() + increase);
    while (end != StringUtils.INDEX_NOT_FOUND) {
      _buffer.append(text, start, end).append(replacement);
      start = end + replLength;
      end = Strings.CS.indexOf(text, searchString, start);
    }
    _buffer.append(text, start, text.length());
    return _buffer.toString();
  }
}
