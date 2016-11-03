/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for StringUtil class
 */
public class StringUtilTest {

  private static final int NUM_TRAILING_NULLS = 10;
  private static final String TEST_STRING = "test_string";

  /**
   * Test for trimTrailingNulls.
   */
  @Test
  public void testTrimTrailingNulls() {
    // Input is null
    String expected = null;
    String actual = StringUtil.trimTrailingNulls(expected);
    Assert.assertEquals(actual, expected);

    // Input has no trailing nulls
    expected = TEST_STRING;
    actual = StringUtil.trimTrailingNulls(expected);
    Assert.assertEquals(actual, expected);

    // Input has trailing nulls
    expected = "abc";
    String stringToTrim = appendTrailingNulls(expected, NUM_TRAILING_NULLS);
    actual = StringUtil.trimTrailingNulls(stringToTrim);
    Assert.assertEquals(actual, expected);

    // Input is empty
    expected = "";
    actual = StringUtil.trimTrailingNulls(expected);
    Assert.assertEquals(actual, expected);

    // Input only has nulls
    expected = "";
    stringToTrim = appendTrailingNulls(expected, NUM_TRAILING_NULLS);
    actual = StringUtil.trimTrailingNulls(stringToTrim);
    Assert.assertEquals(actual, expected);

    // Input has non-trailing nulls only
    expected = new String(new byte[] {0, 0, 97, 98, 99, 100, 0, 0, 0, 101, 102, 103});
    actual = StringUtil.trimTrailingNulls(expected);
    Assert.assertEquals(actual, expected);

    // Input has non-trailing as well as trailing nulls.
    expected = new String(new byte[] {0, 0, 97, 98, 99, 100, 0, 0, 0, 101, 102, 103});
    stringToTrim = appendTrailingNulls(expected, NUM_TRAILING_NULLS);
    actual = StringUtil.trimTrailingNulls(stringToTrim);
    Assert.assertEquals(actual, expected);

  }

  /**
   * Helper method that appends trailing nulls to the given string
   *
   * @param input Input string to pad
   * @param N Number of nulls to append
   * @return String with nulls appended
   */
  private String appendTrailingNulls(String input, int N) {
    return input + new String(new byte[N]);
  }
}
