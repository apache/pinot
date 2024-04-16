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

  @Test(dataProvider = "isJson")
  public void testIsJson(String input, boolean expectedValue) {
    assertEquals(StringFunctions.isJson(input), expectedValue);
  }

  @Test(dataProvider = "splitPartTestCases")
  public void testSplitPart(String input, String delimiter, int index, int limit, String expectedToken,
      String expectedTokenWithLimitCounts) {
    assertEquals(StringFunctions.splitPart(input, delimiter, index), expectedToken);
    assertEquals(StringFunctions.splitPart(input, delimiter, index, limit), expectedTokenWithLimitCounts);
  }
}
