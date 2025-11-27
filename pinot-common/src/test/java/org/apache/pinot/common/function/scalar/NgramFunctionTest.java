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


public class NgramFunctionTest {

  @DataProvider(name = "ngramTestCases")
  public static Object[][] ngramTestCases() {
    return new Object[][]{
        {"abcd", 0, 3, new String[]{"abc", "bcd"}, new String[]{"a", "b", "c", "d", "ab", "bc", "cd", "abc", "bcd"}},
        {"abcd", 2, 2, new String[]{"ab", "bc", "cd"}, new String[]{"ab", "bc", "cd"}}, {
        "abcd", 3, 0, new String[]{}, new String[]{}
    }, {"abc", 0, 3, new String[]{"abc"}, new String[]{"a", "b", "c", "ab", "bc", "abc"}}, {
        "abc", 3, 0, new String[]{}, new String[]{}
    }, {"abc", 3, 3, new String[]{"abc"}, new String[]{"abc"}}, {
        "a", 0, 3, new String[]{}, new String[]{"a"}
    }, {"a", 2, 3, new String[]{}, new String[]{}}, {
        "a", 3, 3, new String[]{}, new String[]{}
    }, {"", 3, 0, new String[]{}, new String[]{}}, {"", 3, 3, new String[]{}, new String[]{}}, {
        "", 0, 3, new String[]{}, new String[]{}
    }
    };
  }

  @DataProvider(name = "ngramMVExactTestCases")
  public static Object[][] ngramMVExactTestCases() {
    return new Object[][]{
        {new String[]{"ab", "bc"}, 2, new String[]{"ab", "bc"}}, {
        new String[]{"abcd", "bcd"}, 3, new String[]{
        "abc", "bcd"
    }
    }, {new String[]{"a", ""}, 1, new String[]{"a"}}, {new String[]{"a", "b"}, 3, new String[]{}}, {new String[]{"aba"
    }, 2, new String[]{"ab", "ba"}}, {new String[]{}, 2, new String[]{}}, {
        new String[]{"abc"}, 0, new String[]{}
    }
    };
  }

  @DataProvider(name = "ngramMVRangeTestCases")
  public static Object[][] ngramMVRangeTestCases() {
    return new Object[][]{
        {new String[]{"abcd"}, 1, 2, new String[]{"a", "b", "c", "d", "ab", "bc", "cd"}}, {
        new String[]{"abc", "bc"}, 2, 3, new String[]{"ab", "bc", "abc"}
    }, {new String[]{"", "a"}, 0, 3, new String[]{"a"}}, {
        new String[]{"a"}, 3, 3, new String[]{}
    }, {new String[]{}, 1, 2, new String[]{}}
    };
  }

  @Test(dataProvider = "ngramTestCases")
  public void testNGram(String input, int minGram, int maxGram, String[] expectedExactNGram, String[] expectedNGram) {
    assertEquals(new NgramFunctions().uniqueNgrams(input, maxGram), expectedExactNGram);
    assertEquals(new NgramFunctions().uniqueNgrams(input, minGram, maxGram), expectedNGram);
  }

  @Test(dataProvider = "ngramMVExactTestCases")
  public void testNGramMVExact(String[] inputs, int length, String[] expected) {
    assertEquals(new NgramFunctions().generateUniqueNgramsMV(inputs, length), expected);
  }

  @Test(dataProvider = "ngramMVRangeTestCases")
  public void testNGramMVRange(String[] inputs, int minGram, int maxGram, String[] expected) {
    assertEquals(new NgramFunctions().generateUniqueNgramsMV(inputs, minGram, maxGram), expected);
  }
}
