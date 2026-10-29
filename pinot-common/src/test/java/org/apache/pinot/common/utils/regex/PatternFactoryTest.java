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

import java.lang.reflect.Field;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


public class PatternFactoryTest {
  @Test
  public void testNonInitializedPatternFactory()
      throws ReflectiveOperationException {
    // Other tests initialize the static factory, so explicitly restore the uninitialized state for this case.
    Field regexClass = PatternFactory.class.getDeclaredField("_regexClass");
    regexClass.setAccessible(true);
    Object previousRegexClass = regexClass.get(null);
    try {
      regexClass.set(null, null);
      Pattern pattern = PatternFactory.compile("pattern");
      assertTrue(pattern instanceof JavaUtilPattern);
      assertTrue(pattern.matcher("pattern").matches());
    } finally {
      regexClass.set(null, previousRegexClass);
    }
  }

  @Test
  public void testJavaUtilInitializedPatternFactory() {
    PatternFactory.init(RegexClass.JAVA_UTIL.name());
    Pattern pattern = PatternFactory.compile("pattern");
    assertTrue(pattern instanceof JavaUtilPattern);
  }

  @Test
  public void testRe2jInitializedPatternFactory() {
    PatternFactory.init(RegexClass.RE2J.name());
    Pattern pattern = PatternFactory.compile("pattern");
    assertTrue(pattern instanceof Re2jPattern);
  }
}
