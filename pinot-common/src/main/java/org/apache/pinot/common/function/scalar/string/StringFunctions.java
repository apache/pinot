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
   * @param input
   * @param searchString       target substring to replace
   * @param substitute new substring to be replaced with target
   * @see String#replaceAll(String, String)
   */
  @ScalarFunction
  public String replace(String input, String searchString, String substitute) {
    if (StringUtils.isEmpty(input) || StringUtils.isEmpty(searchString) || substitute == null) {
      return input;
    }
    int start = 0;
    int end = StringUtils.indexOf(input, searchString, start);
    if (end == StringUtils.INDEX_NOT_FOUND) {
      return input;
    }
    final int replLength = searchString.length();
    int increase = Math.max(substitute.length() - replLength, 0) * 16;
    _buffer.setLength(0);
    _buffer.ensureCapacity(input.length() + increase);
    int max = -1;
    while (end != StringUtils.INDEX_NOT_FOUND) {
      _buffer.append(input, start, end).append(substitute);
      start = end + replLength;
      if (--max == 0) {
        break;
      }
      end = StringUtils.indexOf(input, searchString, start);
    }
    _buffer.append(input, start, input.length());
    return _buffer.toString();
  }
}
