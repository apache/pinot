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

import java.util.Random;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


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
        {"+++++org++apache++", "", 1, 100, "null", "null"},
        {"+++++org++apache++", "", 0, 100, "+++++org++apache++", "+++++org++apache++"},
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

        // Empty delimiter: index=-1 returns input, other negative indices return "null"
        {"hello", "", -1, 100, "hello", "hello"},
        {"hello", "", -2, 100, "null", "null"},

        // Single field with no delimiter present
        {"abc", ".", 0, 100, "abc", "abc"},
        {"abc", ".", 1, 100, "null", "null"},
        {"abc", ".", -1, 100, "abc", "abc"},
        {"abc", ".", -2, 100, "null", "null"},

        // Input equals delimiter
        {".", ".", 0, 100, "", ""},
        {".", ".", 1, 100, "null", "null"},
        {".", ".", -1, 100, "", ""},

        // Trailing delimiters with content (single-char): exercises splitPartNegativeIdxSingleCharDelim
        // trailing-delimiter handling (resultIdx decrement, empty trailing field)
        {"org++apache++", "+", 0, 100, "org", "org"},
        {"org++apache++", "+", 1, 100, "apache", "apache"},
        {"org++apache++", "+", 2, 100, "", ""},
        {"org++apache++", "+", 3, 100, "null", "null"},
        {"org++apache++", "+", -1, 100, "", ""},
        {"org++apache++", "+", -2, 100, "apache", "apache"},
        {"org++apache++", "+", -3, 100, "org", "org"},
        {"org++apache++", "+", -4, 100, "null", "null"},

        // Leading AND trailing delimiters (single-char): exercises backward scan with
        // both leading delimiter skip and trailing delimiter adjustment
        {"++org++apache++", "+", 0, 100, "org", "org"},
        {"++org++apache++", "+", 1, 100, "apache", "apache"},
        {"++org++apache++", "+", -1, 100, "", ""},
        {"++org++apache++", "+", -2, 100, "apache", "apache"},
        {"++org++apache++", "+", -3, 100, "org", "org"},
        {"++org++apache++", "+", -4, 100, "null", "null"},

        // Single field surrounded by delimiters
        {"++abc++", "+", 0, 100, "abc", "abc"},
        {"++abc++", "+", -1, 100, "", ""},
        {"++abc++", "+", -2, 100, "abc", "abc"},
        {"++abc++", "+", -3, 100, "null", "null"},

        // Multi-char delimiter: exercises forward scan and multi-char negative index
        // path (totalFields counting + adjustedIndex conversion) which is separate from
        // the single-char optimized path
        {"org::apache::pinot", "::", 0, 100, "org", "org"},
        {"org::apache::pinot", "::", 1, 100, "apache", "apache"},
        {"org::apache::pinot", "::", 2, 100, "pinot", "pinot"},
        {"org::apache::pinot", "::", 3, 100, "null", "null"},
        {"org::apache::pinot", "::", -1, 100, "pinot", "pinot"},
        {"org::apache::pinot", "::", -2, 100, "apache", "apache"},
        {"org::apache::pinot", "::", -3, 100, "org", "org"},
        {"org::apache::pinot", "::", -4, 100, "null", "null"},

        // Multi-char delimiter with consecutive delimiters: exercises the consecutive
        // delimiter skip in both leading-skip loop and the totalFields counting loop
        {"::::org::::apache", "::", 0, 100, "org", "org"},
        {"::::org::::apache", "::", 1, 100, "apache", "apache"},
        {"::::org::::apache", "::", 2, 100, "null", "null"},
        {"::::org::::apache", "::", -1, 100, "apache", "apache"},
        {"::::org::::apache", "::", -2, 100, "org", "org"},
        {"::::org::::apache", "::", -3, 100, "null", "null"},

        // Multi-char delimiter with leading AND trailing delimiters: exercises the
        // trailing empty field in the multi-char totalFields counting path
        {"::org::apache::", "::", 0, 100, "org", "org"},
        {"::org::apache::", "::", 1, 100, "apache", "apache"},
        {"::org::apache::", "::", 2, 100, "", ""},
        {"::org::apache::", "::", -1, 100, "", ""},
        {"::org::apache::", "::", -2, 100, "apache", "apache"},
        {"::org::apache::", "::", -3, 100, "org", "org"},
        {"::org::apache::", "::", -4, 100, "null", "null"},

        // Empty input with non-empty delimiter
        {"", ".", 0, 100, "null", "null"},
        {"", ".", -1, 100, "null", "null"},
        {"", ".", -2, 100, "null", "null"},

        // Empty input with multi-char delimiter and negative index
        {"", "::", -1, 100, "null", "null"},

        // Integer.MIN_VALUE: negating it overflows (remains negative), guard must return "null"
        {"org.apache.pinot", ".", Integer.MIN_VALUE, 100, "null", "null"},
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

  @Test
  public void testSplitPartRandomized() {
    String chars = "abcdefg.,:;+-_/";
    String[] delimiters = {".", ",", ":", "::", "++", "ab", "///"};
    Random random = new Random();
    int numIterations = 10_000;

    for (int iter = 0; iter < numIterations; iter++) {
      int len = random.nextInt(50);
      StringBuilder sb = new StringBuilder(len);
      for (int i = 0; i < len; i++) {
        sb.append(chars.charAt(random.nextInt(chars.length())));
      }
      String input = sb.toString();
      String delimiter = delimiters[random.nextInt(delimiters.length)];
      int index = random.nextInt(21) - 10; // range [-10, 10]

      String expected = StringFunctions.splitPartArrayBased(
          StringUtils.splitByWholeSeparator(input, delimiter), index);
      String actual = StringFunctions.splitPart(input, delimiter, index);
      assertEquals(actual, expected,
          String.format("Mismatch for input='%s', delimiter='%s', index=%d", input, delimiter, index));
    }
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
  public void testSoundex() {
    assertEquals(StringFunctions.soundex("Robert"), "R163");
    assertEquals(StringFunctions.soundex("Rupert"), "R163");
    assertEquals(StringFunctions.soundex("Ashcraft"), "A261");
    // Empty string returns SQL-standard fallback code
    assertEquals(StringFunctions.soundex(""), "0000");
    assertNull(StringFunctions.soundex(null));
  }

  @Test
  public void testDifference() {
    assertEquals(StringFunctions.difference("Robert", "Rupert"), 4);
    assertEquals(StringFunctions.difference("Smith", "Johnson"), 1);
    assertEquals(StringFunctions.difference("Ann", "Ann"), 4);
    // "0000" vs "0000" — both encode to the standard empty fallback, all 4 positions match
    assertEquals(StringFunctions.difference("", ""), 4);
    // "R163" vs "0000" — first characters differ, no positions match
    assertEquals(StringFunctions.difference("Robert", ""), 0);
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

  // ==================== Tests for ascii ====================

  @Test
  public void testAscii() {
    assertEquals(StringFunctions.ascii("A"), 65);
    assertEquals(StringFunctions.ascii("a"), 97);
    assertEquals(StringFunctions.ascii("0"), 48);
    assertEquals(StringFunctions.ascii("hello"), 104);
    assertEquals(StringFunctions.ascii(" "), 32);
    assertEquals(StringFunctions.ascii(""), 0);
  }

  // ==================== Tests for space ====================

  @Test
  public void testSpace() {
    assertEquals(StringFunctions.space(0), "");
    assertEquals(StringFunctions.space(1), " ");
    assertEquals(StringFunctions.space(5), "     ");
    assertEquals(StringFunctions.space(-1), "");
  }

  // ==================== Tests for substringIndex ====================

  @Test
  public void testSubstringIndex() {
    // Positive count — substring before nth delimiter
    assertEquals(StringFunctions.substringIndex("a.b.c.d", ".", 1), "a");
    assertEquals(StringFunctions.substringIndex("a.b.c.d", ".", 2), "a.b");
    assertEquals(StringFunctions.substringIndex("a.b.c.d", ".", 3), "a.b.c");
    assertEquals(StringFunctions.substringIndex("a.b.c.d", ".", 10), "a.b.c.d");

    // Negative count — substring after nth delimiter from right
    assertEquals(StringFunctions.substringIndex("a.b.c.d", ".", -1), "d");
    assertEquals(StringFunctions.substringIndex("a.b.c.d", ".", -2), "c.d");
    assertEquals(StringFunctions.substringIndex("a.b.c.d", ".", -3), "b.c.d");
    assertEquals(StringFunctions.substringIndex("a.b.c.d", ".", -10), "a.b.c.d");

    // Zero count
    assertEquals(StringFunctions.substringIndex("a.b.c", ".", 0), "");

    // Empty delimiter
    assertEquals(StringFunctions.substringIndex("a.b.c", "", 1), "");

    // No delimiter found
    assertEquals(StringFunctions.substringIndex("abc", ".", 1), "abc");
    assertEquals(StringFunctions.substringIndex("abc", ".", -1), "abc");

    // Multi-char delimiter
    assertEquals(StringFunctions.substringIndex("a::b::c", "::", 1), "a");
    assertEquals(StringFunctions.substringIndex("a::b::c", "::", -1), "c");
  }

  // ==================== Tests for firstLine ====================

  @Test
  public void testFirstLine() {
    assertEquals(StringFunctions.firstLine("hello\nworld"), "hello");
    assertEquals(StringFunctions.firstLine("single line"), "single line");
    assertEquals(StringFunctions.firstLine(""), "");
    assertEquals(StringFunctions.firstLine("\nstart"), "");
    assertEquals(StringFunctions.firstLine("line1\nline2\nline3"), "line1");
    // Windows line endings
    assertEquals(StringFunctions.firstLine("hello\r\nworld"), "hello");
    // Old Mac line endings
    assertEquals(StringFunctions.firstLine("hello\rworld"), "hello");
    // Mixed
    assertEquals(StringFunctions.firstLine("first\r\nsecond\nthird"), "first");
  }

  // ==================== Tests for startsWithCaseInsensitive ====================

  @Test
  public void testStartsWithCaseInsensitive() {
    assertTrue(StringFunctions.startsWithCaseInsensitive("Hello World", "hello"));
    assertTrue(StringFunctions.startsWithCaseInsensitive("Hello World", "HELLO"));
    assertTrue(StringFunctions.startsWithCaseInsensitive("Hello World", "Hello"));
    assertTrue(StringFunctions.startsWithCaseInsensitive("Hello World", ""));
    assertFalse(StringFunctions.startsWithCaseInsensitive("Hello World", "world"));
  }

  // ==================== Tests for endsWithCaseInsensitive ====================

  @Test
  public void testEndsWithCaseInsensitive() {
    assertTrue(StringFunctions.endsWithCaseInsensitive("Hello World", "world"));
    assertTrue(StringFunctions.endsWithCaseInsensitive("Hello World", "WORLD"));
    assertTrue(StringFunctions.endsWithCaseInsensitive("Hello World", "World"));
    assertTrue(StringFunctions.endsWithCaseInsensitive("Hello World", ""));
    assertFalse(StringFunctions.endsWithCaseInsensitive("Hello World", "hello"));
  }

  // ==================== Tests for isValidASCII ====================

  @Test
  public void testIsValidASCII() {
    assertTrue(StringFunctions.isValidASCII("hello"));
    assertTrue(StringFunctions.isValidASCII("Hello World 123!@#"));
    assertTrue(StringFunctions.isValidASCII(""));
    assertFalse(StringFunctions.isValidASCII("héllo"));
    assertFalse(StringFunctions.isValidASCII("日本語"));
    assertFalse(StringFunctions.isValidASCII("café"));
  }

  // ==================== Tests for bitLength ====================

  @Test
  public void testBitLength() {
    assertEquals(StringFunctions.bitLength(""), 0);
    assertEquals(StringFunctions.bitLength("a"), 8);
    assertEquals(StringFunctions.bitLength("hello"), 40);
    // Multi-byte UTF-8: é is 2 bytes in UTF-8
    assertEquals(StringFunctions.bitLength("é"), 16);
    // 3-byte UTF-8 character
    assertEquals(StringFunctions.bitLength("日"), 24);
  }

  // ==================== Tests for octetLength ====================

  @Test
  public void testOctetLength() {
    assertEquals(StringFunctions.octetLength(""), 0);
    assertEquals(StringFunctions.octetLength("a"), 1);
    assertEquals(StringFunctions.octetLength("hello"), 5);
    // Multi-byte UTF-8
    assertEquals(StringFunctions.octetLength("é"), 2);
    assertEquals(StringFunctions.octetLength("日"), 3);
  }

  // ==================== Tests for charLength ====================

  @Test
  public void testCharLength() {
    assertEquals(StringFunctions.charLength(""), 0);
    assertEquals(StringFunctions.charLength("hello"), 5);
    // Multi-byte characters still count as 1 codepoint each
    assertEquals(StringFunctions.charLength("é"), 1);
    assertEquals(StringFunctions.charLength("日本語"), 3);
    assertEquals(StringFunctions.charLength("café"), 4);
    // Supplementary character (emoji) — 1 codepoint but 2 UTF-16 code units
    assertEquals(StringFunctions.charLength("\uD83D\uDE00"), 1); // U+1F600 grinning face
    // Compare with length() which would return 2 for the emoji
    assertEquals(StringFunctions.length("\uD83D\uDE00"), 2);
  }

  // ==================== Tests for regexpCount ====================

  @Test
  public void testRegexpCount() {
    assertEquals(StringFunctions.regexpCount("hello world hello", "hello"), 2);
    assertEquals(StringFunctions.regexpCount("aaa", "a"), 3);
    assertEquals(StringFunctions.regexpCount("abc", "x"), 0);
    assertEquals(StringFunctions.regexpCount("", "a"), 0);
    // Non-overlapping: "aa" in "aaaa" matches at positions 0 and 2
    assertEquals(StringFunctions.regexpCount("aaaa", "aa"), 2);
    // Regex patterns
    assertEquals(StringFunctions.regexpCount("abc123def456", "\\d+"), 2);
    assertEquals(StringFunctions.regexpCount("a1b2c3", "[0-9]"), 3);
  }

  // ==================== Tests for regexpSubstr ====================

  @Test
  public void testRegexpSubstr() {
    assertEquals(StringFunctions.regexpSubstr("hello world", "w\\w+"), "world");
    assertEquals(StringFunctions.regexpSubstr("abc123def456", "\\d+"), "123");
    assertNull(StringFunctions.regexpSubstr("hello", "\\d+"));
    assertEquals(StringFunctions.regexpSubstr("", "a"), null);
    assertEquals(StringFunctions.regexpSubstr("Hello World", "[A-Z][a-z]+"), "Hello");
  }
}
