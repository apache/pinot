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

import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests array construction behavior that depends on runtime Java element types.
public class ArrayFunctionsTest {

  @Test
  public void testBytesArrayValueConstructor() {
    byte[][] expected = {{0}, {1, 2}};

    Assert.assertEquals(ArrayFunctions.arrayValueConstructor(expected[0], expected[1]), expected);
  }

  @Test
  public void testDoubleArrayElementAtUsesOneBasedBounds() {
    double[] values = {1.25d, -0.0d, Double.POSITIVE_INFINITY};
    assertEquals(ArrayFunctions.arrayElementAtDouble(values, 1), 1.25d);
    assertEquals(Double.doubleToLongBits(ArrayFunctions.arrayElementAtDouble(values, 2)),
        Double.doubleToLongBits(-0.0d));
    assertEquals(ArrayFunctions.arrayElementAtDouble(values, 3), Double.POSITIVE_INFINITY);
    assertEquals(ArrayFunctions.arrayElementAtDouble(values, 0), NullValuePlaceHolder.DOUBLE);
    assertEquals(ArrayFunctions.arrayElementAtDouble(values, -1), NullValuePlaceHolder.DOUBLE);
    assertEquals(ArrayFunctions.arrayElementAtDouble(values, 4), NullValuePlaceHolder.DOUBLE);
    assertEquals(ArrayFunctions.arrayElementAtDouble(new double[0], 1), NullValuePlaceHolder.DOUBLE);
  }
}
