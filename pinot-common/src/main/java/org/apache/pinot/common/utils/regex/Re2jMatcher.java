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

public class Re2jMatcher implements org.apache.pinot.common.utils.regex.Matcher {
  private final com.google.re2j.Matcher _matcher;

  public Re2jMatcher(Pattern pattern, CharSequence input) {
    _matcher = ((org.apache.pinot.common.utils.regex.Re2jPattern) pattern).getPattern().matcher(input);
  }

  @Override
  public boolean matches() {
    return _matcher.matches();
  }

  @Override
  public Matcher reset(CharSequence input) {
    _matcher.reset(input);
    return this;
  }

  @Override
  public boolean find() {
    return _matcher.find();
  }

  @Override
  public int groupCount() {
    return _matcher.groupCount();
  }

  @Override
  public String group(int group) {
    return _matcher.group(group);
  }
}
