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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for inverse hyperbolic trigonometric functions in {@link TrigonometricFunctions}.
 */
public class TrigonometricFunctionsTest {

  private static final double DELTA = 1e-10;

  @Test
  public void testAsinh() {
    assertEquals(TrigonometricFunctions.asinh(0.0), 0.0, DELTA);
    // asinh(sinh(x)) == x
    assertEquals(TrigonometricFunctions.asinh(Math.sinh(1.0)), 1.0, DELTA);
    assertEquals(TrigonometricFunctions.asinh(Math.sinh(5.0)), 5.0, DELTA);
    assertEquals(TrigonometricFunctions.asinh(Math.sinh(-3.0)), -3.0, DELTA);
    // Negative input
    assertEquals(TrigonometricFunctions.asinh(-1.0), -TrigonometricFunctions.asinh(1.0), DELTA);
    // Special values
    assertTrue(Double.isNaN(TrigonometricFunctions.asinh(Double.NaN)));
    assertEquals(TrigonometricFunctions.asinh(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    assertEquals(TrigonometricFunctions.asinh(Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testAcosh() {
    assertEquals(TrigonometricFunctions.acosh(1.0), 0.0, DELTA);
    // acosh(cosh(x)) == x for x >= 0
    assertEquals(TrigonometricFunctions.acosh(Math.cosh(2.0)), 2.0, DELTA);
    assertEquals(TrigonometricFunctions.acosh(Math.cosh(5.0)), 5.0, DELTA);
    // Domain: x < 1 produces NaN
    assertTrue(Double.isNaN(TrigonometricFunctions.acosh(0.5)));
    assertTrue(Double.isNaN(TrigonometricFunctions.acosh(0.0)));
    // Special values
    assertTrue(Double.isNaN(TrigonometricFunctions.acosh(Double.NaN)));
    assertEquals(TrigonometricFunctions.acosh(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
  }

  @Test
  public void testAtanh() {
    assertEquals(TrigonometricFunctions.atanh(0.0), 0.0, DELTA);
    // atanh(tanh(x)) == x for small x
    assertEquals(TrigonometricFunctions.atanh(Math.tanh(0.5)), 0.5, DELTA);
    assertEquals(TrigonometricFunctions.atanh(Math.tanh(-0.5)), -0.5, DELTA);
    // Symmetry
    assertEquals(TrigonometricFunctions.atanh(0.5), -TrigonometricFunctions.atanh(-0.5), DELTA);
    // Boundary: atanh(1) == +Inf, atanh(-1) == -Inf
    assertEquals(TrigonometricFunctions.atanh(1.0), Double.POSITIVE_INFINITY);
    assertEquals(TrigonometricFunctions.atanh(-1.0), Double.NEGATIVE_INFINITY);
    // Domain: |x| > 1 produces NaN
    assertTrue(Double.isNaN(TrigonometricFunctions.atanh(1.5)));
    assertTrue(Double.isNaN(TrigonometricFunctions.atanh(-1.5)));
    // Special values
    assertTrue(Double.isNaN(TrigonometricFunctions.atanh(Double.NaN)));
  }
}
