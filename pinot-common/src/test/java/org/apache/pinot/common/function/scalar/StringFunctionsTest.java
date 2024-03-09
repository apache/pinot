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

  @DataProvider(name = "isJson")
  public static Object[][] isJsonTestCases() {
    return new Object[][]{
        {null, true, true},
        {null, false, false},
        {"", true, true},
        {"{\"key\": \"value\"}", true, true},
        {"{\"key\": \"value\", }", true, false},
        {"{\"key\": \"va", true, false},
        {"", false, true},
        {"{\"key\": \"value\"}", false, true},
        {"{\"key\": \"value\", }", false, false},
        {"{\"key\": \"va", false, false},
    };
  }

  @Test(dataProvider = "isJson")
  public void testIsJson(String input, boolean acceptNull, boolean expectedValue) {
    assertEquals(StringFunctions.isJson(input, acceptNull), expectedValue);
  }
}
