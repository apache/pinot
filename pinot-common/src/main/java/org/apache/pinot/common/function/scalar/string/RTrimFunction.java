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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.spi.annotations.ScalarFunction;


public class RTrimFunction {

  private final static Pattern RTRIM = Pattern.compile("\\s+$");
  private final Matcher _matcher = RTRIM.matcher("");

  /**
   * @param input input
   * @return trim spaces from right side of the string
   */
  @ScalarFunction
  public String rtrim(String input) {
    return _matcher.reset(input).replaceAll("");
  }
}
