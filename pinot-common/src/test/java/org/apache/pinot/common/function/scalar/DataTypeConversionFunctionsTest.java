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

import static org.apache.pinot.common.function.scalar.DataTypeConversionFunctions.hexDecimalToLong;
import static org.apache.pinot.common.function.scalar.DataTypeConversionFunctions.longToHexDecimal;
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

  @Test
  public void testHexDecimalToLong() {
    assertEquals(hexDecimalToLong("0"), 0L);
    assertEquals(hexDecimalToLong("1"), 1L);
    assertEquals(hexDecimalToLong("10"), 16L);
    assertEquals(hexDecimalToLong("100"), 256L);
    assertEquals(hexDecimalToLong("1000"), 4096L);
    assertEquals(hexDecimalToLong("10000"), 65536L);
    assertEquals(hexDecimalToLong("100000"), 1048576L);
    assertEquals(hexDecimalToLong("1000000"), 16777216L);
    assertEquals(hexDecimalToLong("10000000"), 268435456L);
    assertEquals(hexDecimalToLong("100000000"), 4294967296L);
    assertEquals(hexDecimalToLong("1000000000"), 68719476736L);
    assertEquals(hexDecimalToLong("10000000000"), 1099511627776L);
    assertEquals(hexDecimalToLong("100000000000"), 17592186044416L);
    assertEquals(hexDecimalToLong("1000000000000"), 281474976710656L);
    assertEquals(hexDecimalToLong("10000000000000"), 4503599627370496L);
    assertEquals(hexDecimalToLong("100000000000000"), 72057594037927936L);
    assertEquals(hexDecimalToLong("1000000000000000"), 1152921504606846976L);
    assertEquals(hexDecimalToLong("0x0"), 0L);
    assertEquals(hexDecimalToLong("0x1"), 1L);
    assertEquals(hexDecimalToLong("0x10"), 16L);
    assertEquals(hexDecimalToLong("0x100"), 256L);
    assertEquals(hexDecimalToLong("0x1000"), 4096L);
    assertEquals(hexDecimalToLong("0x10000"), 65536L);
    assertEquals(hexDecimalToLong("0x100000"), 1048576L);
    assertEquals(hexDecimalToLong("0x1000000"), 16777216L);
    assertEquals(hexDecimalToLong("0x10000000"), 268435456L);
    assertEquals(hexDecimalToLong("0x100000000"), 4294967296L);
    assertEquals(hexDecimalToLong("0x1000000000"), 68719476736L);
    assertEquals(hexDecimalToLong("0x10000000000"), 1099511627776L);
    assertEquals(hexDecimalToLong("0x100000000000"), 17592186044416L);
    assertEquals(hexDecimalToLong("0x1000000000000"), 281474976710656L);
    assertEquals(hexDecimalToLong("0x10000000000000"), 4503599627370496L);
    assertEquals(hexDecimalToLong("0x100000000000000"), 72057594037927936L);
    assertEquals(hexDecimalToLong("0x1000000000000000"), 1152921504606846976L);
  }

  @Test
  public void testLongToHexDecimal() {
    assertEquals(longToHexDecimal(0L), "0");
    assertEquals(longToHexDecimal(1L), "1");
    assertEquals(longToHexDecimal(16L), "10");
    assertEquals(longToHexDecimal(256L), "100");
    assertEquals(longToHexDecimal(4096L), "1000");
    assertEquals(longToHexDecimal(65536L), "10000");
    assertEquals(longToHexDecimal(1048576L), "100000");
    assertEquals(longToHexDecimal(16777216L), "1000000");
    assertEquals(longToHexDecimal(268435456L), "10000000");
    assertEquals(longToHexDecimal(4294967296L), "100000000");
    assertEquals(longToHexDecimal(68719476736L), "1000000000");
    assertEquals(longToHexDecimal(1099511627776L), "10000000000");
    assertEquals(longToHexDecimal(17592186044416L), "100000000000");
    assertEquals(longToHexDecimal(281474976710656L), "1000000000000");
    assertEquals(longToHexDecimal(4503599627370496L), "10000000000000");
    assertEquals(longToHexDecimal(72057594037927936L), "100000000000000");
    assertEquals(longToHexDecimal(1152921504606846976L), "1000000000000000");
  }
}
