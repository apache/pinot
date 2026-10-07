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

import java.math.BigDecimal;
import java.sql.Timestamp;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ObjectFunctionsTest {

  @DataProvider
  public static Object[][] distinctFromCases() {
    return new Object[][]{
        // Nulls
        {null, null, false},
        {null, 1, true},
        // Same types
        {5, 5, false},
        {5, 6, true},
        {5L, 5L, false},
        {1.5, 1.5, false},
        {1.5f, 1.5f, false},
        {"a", "a", false},
        {"a", "b", true},
        {true, true, false},
        {new Timestamp(1000L), new Timestamp(1000L), false},
        // Integers of different types
        {5, 5L, false},
        {5, 6L, true},
        {Integer.MAX_VALUE, (long) Integer.MAX_VALUE, false},
        {-1, 0xFFFFFFFFL, true},
        // Integers and floating point numbers
        {5, 5.0, false},
        {5, 5.0f, false},
        {5L, 5.5, true},
        {16777217, 16777216f, true},
        {9007199254740993L, 9007199254740992.0, true},
        {1L << 60, 0x1p60, false},
        {Long.MAX_VALUE, (double) Long.MAX_VALUE, true},
        {Long.MIN_VALUE, -0x1p63, false},
        // A float is widened to double, so 1.1f (1.100000023841858) is distinct from 1.1
        {1.5f, 1.5, false},
        {1.1f, 1.1, true},
        {1.1f, (double) 1.1f, false},
        // BigDecimal ignores the scale, and compares a floating point number by its decimal string
        {new BigDecimal("1.5"), new BigDecimal("1.50"), false},
        {new BigDecimal("1.5"), new BigDecimal("1.51"), true},
        {new BigDecimal("5.00"), 5, false},
        {new BigDecimal("5.00"), 5L, false},
        {new BigDecimal("5.01"), 5L, true},
        {new BigDecimal("1.50"), 1.5, false},
        {new BigDecimal("0.1"), 0.1, false},
        {new BigDecimal("0.10000000000000000001"), 0.1, true},
        {new BigDecimal("1.50"), 1.5f, false},
        {new BigDecimal("1.1"), 1.1f, true},
        // Like Double.equals(), -0.0 is distinct from 0.0. BigDecimal has no -0.0.
        {-0.0, 0.0, true},
        {-0.0f, 0.0f, true},
        {-0.0f, 0.0, true},
        {-0.0, -0.0f, false},
        {-0.0, 0, true},
        {-0.0, 0L, true},
        {-0.0, BigDecimal.ZERO, false},
        // NaN is not distinct from NaN, and is distinct from any other number
        {Double.NaN, Double.NaN, false},
        {Float.NaN, Double.NaN, false},
        {Double.NaN, 0.0, true},
        {Double.NaN, 0, true},
        {Double.NaN, Long.MAX_VALUE, true},
        {Double.NaN, BigDecimal.ZERO, true},
        {Float.NaN, BigDecimal.ONE, true},
        // Infinity
        {Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, false},
        {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, true},
        {Double.POSITIVE_INFINITY, Long.MAX_VALUE, true},
        {Double.POSITIVE_INFINITY, new BigDecimal("1E+400"), true},
        // Non-numeric values are compared with equals()
        {"5", 5, true},
        {true, 1, true},
        {new Timestamp(1000L), 1000L, true}
    };
  }

  @Test(dataProvider = "distinctFromCases")
  public void testIsDistinctFrom(Object value1, Object value2, boolean expected) {
    assertEquals(ObjectFunctions.isDistinctFrom(value1, value2), expected);
    assertEquals(ObjectFunctions.isDistinctFrom(value2, value1), expected);
    assertEquals(ObjectFunctions.isNotDistinctFrom(value1, value2), !expected);
    assertEquals(ObjectFunctions.isNotDistinctFrom(value2, value1), !expected);
  }
}
