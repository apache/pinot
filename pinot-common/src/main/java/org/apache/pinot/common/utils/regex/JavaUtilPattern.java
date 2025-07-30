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
package org.apache.pinot.common.utils.regex;

import java.util.regex.Pattern;

public class JavaUtilPattern implements org.apache.pinot.common.utils.regex.Pattern {
  Pattern _pattern;

  public JavaUtilPattern(String regex) {
    _pattern = Pattern.compile(regex, 0);
  }

  public JavaUtilPattern(String regex, boolean caseInsensitive) {
    _pattern = caseInsensitive
        ? Pattern.compile(regex, Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE)
        : Pattern.compile(regex, 0);
  }

  @Override
  public Matcher matcher(CharSequence input) {
    return MatcherFactory.matcher(this, input);
  }

  protected Pattern getPattern() {
    return _pattern;
  }
}
