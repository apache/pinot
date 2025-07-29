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

/**
 * Factory class to create Pattern objects based on a configurable regex class
 */
public class PatternFactory {
  private static RegexClass _regexClass;

  private PatternFactory() {
  }

  public static void init(String regexClass) {
    _regexClass = RegexClass.valueOf(regexClass);
  }

  /**
   * Returns a Pattern for the regex class specified in PatternFactory.init(). If pattern factory is not initialized,
   * then returns a Pattern using the java.util.regex class
   *
   * @param regex to compile the Pattern for
   * @param caseInsensitive whether the pattern should be case-insensitive
   * @return the compiled Pattern
   */
  public static Pattern compile(String regex, boolean caseInsensitive) {
    // un-initialized factory will use java.util.regex to avoid requiring initialization in tests
    if (_regexClass == null) {
      return new JavaUtilPattern(regex, caseInsensitive);
    }
    switch (_regexClass) {
      case RE2J:
        return new Re2jPattern(regex, caseInsensitive);
      case JAVA_UTIL:
      default:
        return new JavaUtilPattern(regex, caseInsensitive);
    }
  }

  /**
   * Returns a Pattern for the regex class specified in PatternFactory.init(). If pattern factory is not initialized,
   * then returns a Pattern using the java.util.regex class (case-sensitive by default)
   *
   * @param regex to compile the Pattern for
   * @return the compiled Pattern
   */
  public static Pattern compile(String regex) {
    return compile(regex, false);
  }
}
