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
package org.apache.pinot.query.runtime.operator.utils;

import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


public class TypeUtilsTest {

  @Test
  public void testNumericArrayKeepsItsType() {
    int[] intArray = {1, 2};
    assertSame(TypeUtils.convert(intArray, ColumnDataType.INT_ARRAY), intArray);
    double[] doubleArray = {1.5, 2.5};
    assertSame(TypeUtils.convert(doubleArray, ColumnDataType.DOUBLE_ARRAY), doubleArray);
  }

  /// The single-stage engine can compute another element type than the one the multi-stage planner inferred, for
  /// example a `double[]` from `generateArray(intCol, intCol + 1)`, whose `plus` returns DOUBLE.
  @Test
  public void testNumericArrayConvertsToTheStoredType() {
    assertEquals(TypeUtils.convert(new double[]{1, 2}, ColumnDataType.INT_ARRAY), new int[]{1, 2});
    assertEquals(TypeUtils.convert(new double[]{1, 2}, ColumnDataType.LONG_ARRAY), new long[]{1, 2});
    assertEquals(TypeUtils.convert(new int[]{1, 2}, ColumnDataType.LONG_ARRAY), new long[]{1, 2});
    assertEquals(TypeUtils.convert(new long[]{1, 2}, ColumnDataType.INT_ARRAY), new int[]{1, 2});
    assertEquals(TypeUtils.convert(new int[]{1, 2}, ColumnDataType.FLOAT_ARRAY), new float[]{1, 2});
    assertEquals(TypeUtils.convert(new float[]{1.5f}, ColumnDataType.DOUBLE_ARRAY), new double[]{1.5});
    assertEquals(TypeUtils.convert(new long[]{1L << 40}, ColumnDataType.DOUBLE_ARRAY), new double[]{1L << 40});
  }
}
