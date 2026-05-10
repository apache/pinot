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
package org.apache.pinot.common.function.scalar.bitwise;

import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests for bitwise scalar function edge cases.
public class BitwiseScalarFunctionTest {
  private static final int[] INT_VALUES = {
      0, 10, -10, Integer.MIN_VALUE, Integer.MAX_VALUE
  };
  private static final long[] LONG_VALUES = {
      0L, 10L, -10L, Long.MIN_VALUE, Long.MAX_VALUE
  };

  @Test
  public void testMaskScalarFunctionStripsSignBit() {
    for (int value : INT_VALUES) {
      assertEquals(MaskScalarFunction.intMask(value), value & Integer.MAX_VALUE);
    }
    for (long value : LONG_VALUES) {
      assertEquals(MaskScalarFunction.longMask(value), value & Long.MAX_VALUE);
    }
  }

  @Test
  public void testPolymorphicMaskScalarFunction() {
    assertEquals(MaskScalarFunction.intMask(Integer.MIN_VALUE), 0);
    assertEquals(MaskScalarFunction.longMask(Long.MIN_VALUE), 0L);

    MaskScalarFunction mask = new MaskScalarFunction();
    assertEquals(mask.getFunctionInfo(new ColumnDataType[]{ColumnDataType.INT}).getMethod().getName(), "intMask");
    assertEquals(mask.getFunctionInfo(new ColumnDataType[]{ColumnDataType.LONG}).getMethod().getName(), "longMask");
    assertEquals(mask.getFunctionInfo(1).getMethod().getName(), "longMask");
    assertNull(mask.getFunctionInfo(new ColumnDataType[]{ColumnDataType.FLOAT}));
  }
}
