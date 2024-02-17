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
        {"org.apache.pinot.common.function", ".", 0, 100, "org", "org", "function"},
        {"org.apache.pinot.common.function", ".", 10, 100, "null", "null", "null"},
        {"org.apache.pinot.common.function", ".", 1, 0, "apache", "apache", "common"},
        {"org.apache.pinot.common.function", ".", 1, 1, "apache", "null", "common"},
        {"org.apache.pinot.common.function", ".", 0, 1, "org", "org.apache.pinot.common.function", "function"},
        {"org.apache.pinot.common.function", ".", 1, 2, "apache", "apache.pinot.common.function", "common"},
        {"org.apache.pinot.common.function", ".", 2, 3, "pinot", "pinot.common.function", "pinot"},
        {"org.apache.pinot.common.function", ".", 3, 4, "common", "common.function", "apache"},
        {"org.apache.pinot.common.function", ".", 4, 5, "function", "function", "org"},
        {"org.apache.pinot.common.function", ".", 5, 6, "null", "null", "null"},
        {"org.apache.pinot.common.function", ".", 3, 3, "common", "null", "apache"},
        {"+++++", "+", 0, 100, "", "", ""},
        {"+++++", "+", 1, 100, "null", "null", "null"},
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
  public void testSplitPark(String input, String delimiter, int index, int max, String expectedToken,
      String expectedTokenWithLimitCounts, String expectedTokenFromEnd) {
    assertEquals(StringFunctions.splitPart(input, delimiter, index), expectedToken);
    assertEquals(StringFunctions.splitPart(input, delimiter, index, max), expectedTokenWithLimitCounts);
    assertEquals(StringFunctions.splitPartFromEnd(input, delimiter, index), expectedTokenFromEnd);
  }
}
