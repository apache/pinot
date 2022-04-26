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


public class DataTypeConversionFunctionsTest {

  @DataProvider(name = "testCases")
  public static Object[][] testCases() {
    return new Object[][]{
        {"a", "string", "a"},
        {"10", "int", 10},
        {"10", "long", 10L},
        {"10", "float", 10F},
        {"10", "double", 10D},
        {"10.0", "int", 10},
        {"10.0", "long", 10L},
        {"10.0", "float", 10F},
        {"10.0", "double", 10D},
        {10, "string", "10"},
        {10L, "string", "10"},
        {10F, "string", "10.0"},
        {10D, "string", "10.0"},
        {"a", "string", "a"},
        {10, "int", 10},
        {10L, "long", 10L},
        {10F, "float", 10F},
        {10D, "double", 10D},
        {10L, "int", 10},
        {10, "long", 10L},
        {10D, "float", 10F},
        {10F, "double", 10D},
        {"abc1", "bytes", new byte[]{(byte) 0xab, (byte) 0xc1}},
        {new byte[]{(byte) 0xab, (byte) 0xc1}, "string", "abc1"}
    };
  }

  @Test(dataProvider = "testCases")
  public void test(Object value, String type, Object expected) {
    assertEquals(DataTypeConversionFunctions.cast(value, type), expected);
  }
}
