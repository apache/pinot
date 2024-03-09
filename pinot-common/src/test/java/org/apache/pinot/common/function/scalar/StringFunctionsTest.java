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
package org.apache.pinot.common.function.scalar;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class StringFunctionsTest {

  @DataProvider(name = "prefixAndSuffixTestCases")
  public static Object[][] prefixAndSuffixTestCases() {
    return new Object[][]{
        {"abcde", 3, new String[]{"a", "ab", "abc"}, new String[]{"e", "de", "cde"}, new String[]{"^a", "^ab",
            "^abc"}, new String[]{"e$", "de$", "cde$"}},
        {"abcde", 0, new String[]{}, new String[]{}, new String[]{}, new String[]{}},
        {"abcde", 9, new String[]{"a", "ab", "abc", "abcd", "abcde"}, new String[]{"e", "de", "cde", "bcde", "abcde"},
            new String[]{"^a", "^ab", "^abc", "^abcd", "^abcde"}, new String[]{"e$", "de$", "cde$", "bcde$", "abcde$"}},
        {"a", 3, new String[]{"a"}, new String[]{"a"}, new String[]{"^a"}, new String[]{"a$"}},
        {"a", 0, new String[]{}, new String[]{}, new String[]{}, new String[]{}},
        {"a", 9, new String[]{"a"}, new String[]{"a"}, new String[]{"^a"}, new String[]{"a$"}},
        {"", 3, new String[]{}, new String[]{}, new String[]{}, new String[]{}},
        {"", 0, new String[]{}, new String[]{}, new String[]{}, new String[]{}},
        {"", 9, new String[]{}, new String[]{}, new String[]{}, new String[]{}}
    };
  }

  @DataProvider(name = "ngramTestCases")
  public static Object[][] ngramTestCases() {
    return new Object[][]{
        {"abcd", 0, 3, new String[]{"abc", "bcd"}, new String[]{"a", "b", "c", "d", "ab", "bc", "cd", "abc", "bcd"}},
        {"abcd", 2, 2, new String[]{"ab", "bc", "cd"}, new String[]{"ab", "bc", "cd"}},
        {"abcd", 3, 0, new String[]{}, new String[]{}},
        {"abc", 0, 3, new String[]{"abc"}, new String[]{"a", "b", "c", "ab", "bc", "abc"}},
        {"abc", 3, 0, new String[]{}, new String[]{}},
        {"abc", 3, 3, new String[]{"abc"}, new String[]{"abc"}},
        {"a", 0, 3, new String[]{}, new String[]{"a"}},
        {"a", 2, 3, new String[]{}, new String[]{}},
        {"a", 3, 3, new String[]{}, new String[]{}},
        {"", 3, 0, new String[]{}, new String[]{}},
        {"", 3, 3, new String[]{}, new String[]{}},
        {"", 0, 3, new String[]{}, new String[]{}}
    };
  }

  @Test(dataProvider = "prefixAndSuffixTestCases")
  public void testPrefixAndSuffix(String input, int length, String[] expectedPrefix, String[] expectedSuffix,
      String[] expectedPrefixWithRegexChar, String[] expectedSuffixWithRegexChar) {
    assertEquals(StringFunctions.uniquePrefixes(input, length), expectedPrefix);
    assertEquals(StringFunctions.uniqueSuffixes(input, length), expectedSuffix);
    assertEquals(StringFunctions.uniquePrefixesWithPrefix(input, length, "^"), expectedPrefixWithRegexChar);
    assertEquals(StringFunctions.uniqueSuffixesWithSuffix(input, length, "$"), expectedSuffixWithRegexChar);
  }

  @Test(dataProvider = "ngramTestCases")
  public void testNGram(String input, int minGram, int maxGram, String[] expectedExactNGram, String[] expectedNGram) {
    assertEquals(StringFunctions.uniqueNgrams(input, maxGram), expectedExactNGram);
    assertEquals(StringFunctions.uniqueNgrams(input, minGram, maxGram), expectedNGram);
  }
}
