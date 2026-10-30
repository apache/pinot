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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class RegexImplementationsTest {
  private static final String FIND_VALUE = "value to match against";
  private static final String MATCHES_VALUE = "to match";
  private static final String PATTERN = "(to match)";

  @Test
  public void testRe2jCaseSensitivity() {
    Pattern caseSensitive = new Re2jPattern("pinot", false);
    assertTrue(caseSensitive.matcher("pinot").matches());
    assertFalse(caseSensitive.matcher("pInOt").matches());

    Pattern caseInsensitive = new Re2jPattern("pinot", true);
    assertTrue(caseInsensitive.matcher("pinot").matches());
    assertTrue(caseInsensitive.matcher("pInOt").matches());
    assertFalse(caseInsensitive.matcher("other").matches());
  }

  @Test(dataProvider = "regexImplementationProvider")
  public void testRegexImplementations(RegexClass regexClass) {
    PatternFactory.init(regexClass.name());
    Pattern pattern = PatternFactory.compile(PATTERN);
    Matcher matcher = pattern.matcher("");

    // Test .find()
    Assert.assertTrue(matcher.reset(FIND_VALUE).find());

    // Test .reset()
    Assert.assertFalse(matcher.find());
    Assert.assertTrue(matcher.reset(FIND_VALUE).find());

    // Test .matches()
    Assert.assertTrue(matcher.reset(MATCHES_VALUE).matches());

    // Test .groupCount()
    Assert.assertEquals(matcher.reset(FIND_VALUE).groupCount(), 1);

    // Test .group()
    matcher.find();
    Assert.assertEquals(matcher.group(0), MATCHES_VALUE);
    Assert.assertEquals(matcher.group(1), MATCHES_VALUE);
    Assert.assertThrows(IndexOutOfBoundsException.class, () -> matcher.group(2));
  }

  @DataProvider(name = "regexImplementationProvider")
  public RegexClass[] regexImplementationProvider() {
    return new RegexClass[]{RegexClass.RE2J, RegexClass.JAVA_UTIL};
  }
}
