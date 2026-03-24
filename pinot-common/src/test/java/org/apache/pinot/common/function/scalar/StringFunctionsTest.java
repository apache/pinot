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

  @DataProvider(name = "initcapTestCases")
  public static Object[][] initcapTestCases() {
    return new Object[][]{
        // Basic test cases
        {"hello world", "Hello World"},
        {"HELLO WORLD", "Hello World"},
        {"hello WORLD", "Hello World"},
        {"HeLLo WoRLd", "Hello World"},

        // Single word
        {"hello", "Hello"},
        {"HELLO", "Hello"},
        {"hELLO", "Hello"},

        // Multiple spaces
        {"hello  world", "Hello  World"},
        {"hello   world   test", "Hello   World   Test"},

        // Leading and trailing spaces
        {" hello world", " Hello World"},
        {"hello world ", "Hello World "},
        {" hello world ", " Hello World "},

        // Special characters and numbers
        {"hello-world", "Hello-world"},
        {"hello_world", "Hello_world"},
        {"hello123world", "Hello123world"},
        {"123hello world", "123hello World"},

        // Mixed whitespace characters
        {"hello\tworld", "Hello\tWorld"},
        {"hello\nworld", "Hello\nWorld"},
        {"hello\rworld", "Hello\rWorld"},

        // Edge cases
        {"", ""},
        {" ", " "},
        {"a", "A"},
        {"A", "A"},

        // Real-world examples
        {"apache pinot", "Apache Pinot"},
        {"the quick brown fox", "The Quick Brown Fox"},
        {"SQL is AWESOME", "Sql Is Awesome"},
        {"new york city", "New York City"},

        // Unicode and special characters
        {"café résumé", "Café Résumé"},
        {"hello@world.com", "Hello@world.com"},
        {"one,two,three", "One,two,three"}
    };
  }

  @DataProvider(name = "levenshteinDistanceTestCases")
  public static Object[][] levenshteinDistanceTestCases() {
    return new Object[][]{
        // Basic test cases
        {"", "", 0},
        {"a", "", 1},
        {"", "a", 1},
        {"a", "a", 0},

        // Classic examples
        {"kitten", "sitting", 3},
        {"saturday", "sunday", 3},
        {"intention", "execution", 5},

        // Single character operations
        {"cat", "bat", 1}, // substitution
        {"cat", "cats", 1}, // insertion
        {"cats", "cat", 1}, // deletion

        // More complex cases
        {"book", "back", 2},
        {"hello", "world", 4},
        {"algorithm", "altruistic", 6},

        // Edge cases with repeated characters
        {"aaa", "aa", 1},
        {"aa", "aaa", 1},
        {"abc", "def", 3},

        // Longer strings
        {"abcdefghijklmnop", "1234567890123456", 16},
        {"programming", "grammar", 6},

        // Case sensitivity
        {"Hello", "hello", 1},
        {"WORLD", "world", 5},

        // Special characters and numbers
        {"test123", "test456", 3},
        {"hello!", "hello?", 1},
        {"a@b.com", "a@c.com", 1}
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

  @Test(dataProvider = "initcapTestCases")
  public void testInitcap(String input, String expected) {
    assertEquals(StringFunctions.initcap(input), expected);
  }

  @Test(dataProvider = "levenshteinDistanceTestCases")
  public void testLevenshteinDistance(String input1, String input2, int expectedDistance) {
    assertEquals(StringFunctions.levenshteinDistance(input1, input2), expectedDistance);
  }


  @Test
  public void testHammingDistance() {
    // Test existing hammingDistance function for comparison
    assertEquals(StringFunctions.hammingDistance("abc", "abc"), 0);
    assertEquals(StringFunctions.hammingDistance("abc", "def"), 3);
    assertEquals(StringFunctions.hammingDistance("abc", "aef"), 2);
    assertEquals(StringFunctions.hammingDistance("abc", "abcd"), -1); // Different lengths

    // Demonstrate the difference between hammingDistance and levenshteinDistance
    assertEquals(StringFunctions.hammingDistance("cat", "cats"), -1); // Hamming can't handle different lengths
    assertEquals(StringFunctions.levenshteinDistance("cat", "cats"), 1); // Levenshtein can handle different lengths
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
