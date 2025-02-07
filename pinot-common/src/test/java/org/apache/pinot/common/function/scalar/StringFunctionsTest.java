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

import org.apache.pinot.common.function.scalar.string.NgramFunctions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class StringFunctionsTest {

  @DataProvider(name = "splitPartTestCases")
  public static Object[][] splitPartTestCases() {
    return new Object[][]{
        {"org.apache.pinot.common.function", ".", 0, 100, "org", "org"},
        {"org.apache.pinot.common.function", ".", 10, 100, "null", "null"},
        {"org.apache.pinot.common.function", ".", 1, 0, "apache", "apache"},
        {"org.apache.pinot.common.function", ".", 1, 1, "apache", "null"},
        {"org.apache.pinot.common.function", ".", 0, 1, "org", "org.apache.pinot.common.function"},
        {"org.apache.pinot.common.function", ".", 1, 2, "apache", "apache.pinot.common.function"},
        {"org.apache.pinot.common.function", ".", 2, 3, "pinot", "pinot.common.function"},
        {"org.apache.pinot.common.function", ".", 3, 4, "common", "common.function"},
        {"org.apache.pinot.common.function", ".", 4, 5, "function", "function"},
        {"org.apache.pinot.common.function", ".", 5, 6, "null", "null"},
        {"org.apache.pinot.common.function", ".", 3, 3, "common", "null"},
        {"+++++", "+", 0, 100, "", ""},
        {"+++++", "+", 1, 100, "null", "null"},
        // note that splitPart will split with limit first, then lookup by index from START or END.
        {"org.apache.pinot.common.function", ".", -1, 100, "function", "function"},
        {"org.apache.pinot.common.function", ".", -10, 100, "null", "null"},
        {"org.apache.pinot.common.function", ".", -2, 0, "common", "common"}, // Case: limit=0 is not taking effect.
        {"org.apache.pinot.common.function", ".", -1, 1, "function", "org.apache.pinot.common.function"},
        {"org.apache.pinot.common.function", ".", -2, 1, "common", "null"},
        {"org.apache.pinot.common.function", ".", -1, 2, "function", "apache.pinot.common.function"},
        {"org.apache.pinot.common.function", ".", -2, 2, "common", "org"},
        {"org.apache.pinot.common.function", ".", -1, 3, "function", "pinot.common.function"},
        {"org.apache.pinot.common.function", ".", -3, 3, "pinot", "org"},
        {"org.apache.pinot.common.function", ".", -4, 3, "apache", "null"},
        {"org.apache.pinot.common.function", ".", -1, 4, "function", "common.function"},
        {"org.apache.pinot.common.function", ".", -3, 4, "pinot", "apache"},
        {"org.apache.pinot.common.function", ".", -4, 4, "apache", "org"},
        {"org.apache.pinot.common.function", ".", -1, 5, "function", "function"},
        {"org.apache.pinot.common.function", ".", -5, 5, "org", "org"},
        {"org.apache.pinot.common.function", ".", -6, 5, "null", "null"},
        {"org.apache.pinot.common.function", ".", -1, 6, "function", "function"},
        {"org.apache.pinot.common.function", ".", -5, 6, "org", "org"},
        {"org.apache.pinot.common.function", ".", -6, 6, "null", "null"},
        {"+++++", "+", -1, 100, "", ""},
        {"+++++", "+", -2, 100, "null", "null"},
    };
  }

  @DataProvider(name = "isJson")
  public static Object[][] isJsonTestCases() {
    return new Object[][]{
        {"", true},
        {"{\"key\": \"value\"}", true},
        {"{\"key\": \"value\", }", false},
        {"{\"key\": \"va", false}
    };
  }

  @DataProvider(name = "prefixAndSuffixTestCases")
  public static Object[][] prefixAndSuffixTestCases() {
    return new Object[][]{
        {"abcde", 3, new String[]{"a", "ab", "abc"}, new String[]{"e", "de", "cde"}, new String[]{
            "^a", "^ab", "^abc"}, new String[]{"e$", "de$", "cde$"}},
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

  @Test(dataProvider = "isJson")
  public void testIsJson(String input, boolean expectedValue) {
    assertEquals(StringFunctions.isJson(input), expectedValue);
  }

  @Test(dataProvider = "splitPartTestCases")
  public void testSplitPart(String input, String delimiter, int index, int limit, String expectedToken,
      String expectedTokenWithLimitCounts) {
    assertEquals(StringFunctions.splitPart(input, delimiter, index), expectedToken);
    assertEquals(StringFunctions.splitPart(input, delimiter, limit, index), expectedTokenWithLimitCounts);
  }

  @Test(dataProvider = "prefixAndSuffixTestCases")
  public void testPrefixAndSuffix(String input, int length, String[] expectedPrefix, String[] expectedSuffix,
      String[] expectedPrefixWithRegexChar, String[] expectedSuffixWithRegexChar) {
    assertEquals(StringFunctions.prefixes(input, length), expectedPrefix);
    assertEquals(StringFunctions.suffixes(input, length), expectedSuffix);
    assertEquals(StringFunctions.prefixesWithPrefix(input, length, "^"), expectedPrefixWithRegexChar);
    assertEquals(StringFunctions.suffixesWithSuffix(input, length, "$"), expectedSuffixWithRegexChar);
  }

  @Test(dataProvider = "ngramTestCases")
  public void testNGram(String input, int minGram, int maxGram, String[] expectedExactNGram, String[] expectedNGram) {
    assertEquals(new NgramFunctions().uniqueNgrams(input, maxGram), expectedExactNGram);
    assertEquals(new NgramFunctions().uniqueNgrams(input, minGram, maxGram), expectedNGram);
  }

  @Test
  public void encodeUrl() {
    assertEquals(StringFunctions.encodeUrl(""), "");
    assertEquals(StringFunctions.encodeUrl("a"), "a");
    assertEquals(StringFunctions.encodeUrl("A"), "A");
    assertEquals(StringFunctions.encodeUrl(" "), "+");
    assertEquals(StringFunctions.encodeUrl("?"), "%3F");
    assertEquals(StringFunctions.encodeUrl("/"), "%2F");
    assertEquals(StringFunctions.encodeUrl("&"), "%26");
    assertEquals(StringFunctions.encodeUrl(":"), "%3A");
    assertEquals(StringFunctions.encodeUrl("="), "%3D");
    assertEquals(StringFunctions.encodeUrl("@"), "%40");

    assertEquals(StringFunctions.encodeUrl(
        "http://localhost:8080/hello?a=b"), "http%3A%2F%2Flocalhost%3A8080%2Fhello%3Fa%3Db");

    // CHECKSTYLE:OFF
    assertEquals(StringFunctions.encodeUrl(
        "http://localhost:8080/hello?paramWithSpace=a b"), "http%3A%2F%2Flocalhost%3A8080%2Fhello%3FparamWithSpace%3Da+b");
    // CHECKSTYLE:ON

    assertEquals(StringFunctions.encodeUrl(
        "https://localhost:8080/hello?a=b"), "https%3A%2F%2Flocalhost%3A8080%2Fhello%3Fa%3Db");
  }
}
