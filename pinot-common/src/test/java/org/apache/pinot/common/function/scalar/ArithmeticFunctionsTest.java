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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Tests for non-polymorphic arithmetic scalar functions in {@link ArithmeticFunctions}.
 */
public class ArithmeticFunctionsTest {

  private static final double DELTA = 1e-10;

  @Test
  public void testCbrt() {
    assertEquals(ArithmeticFunctions.cbrt(27.0), 3.0, DELTA);
    assertEquals(ArithmeticFunctions.cbrt(-8.0), -2.0, DELTA);
    assertEquals(ArithmeticFunctions.cbrt(0.0), 0.0, DELTA);
    assertEquals(ArithmeticFunctions.cbrt(1.0), 1.0, DELTA);
    assertTrue(Double.isNaN(ArithmeticFunctions.cbrt(Double.NaN)));
    assertEquals(ArithmeticFunctions.cbrt(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    assertEquals(ArithmeticFunctions.cbrt(Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testExp2() {
    assertEquals(ArithmeticFunctions.exp2(0.0), 1.0, DELTA);
    assertEquals(ArithmeticFunctions.exp2(1.0), 2.0, DELTA);
    assertEquals(ArithmeticFunctions.exp2(3.0), 8.0, DELTA);
    assertEquals(ArithmeticFunctions.exp2(10.0), 1024.0, DELTA);
    assertEquals(ArithmeticFunctions.exp2(-1.0), 0.5, DELTA);
    assertTrue(Double.isNaN(ArithmeticFunctions.exp2(Double.NaN)));
    assertEquals(ArithmeticFunctions.exp2(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    assertEquals(ArithmeticFunctions.exp2(Double.NEGATIVE_INFINITY), 0.0, DELTA);
  }

  @Test
  public void testExp10() {
    assertEquals(ArithmeticFunctions.exp10(0.0), 1.0, DELTA);
    assertEquals(ArithmeticFunctions.exp10(1.0), 10.0, DELTA);
    assertEquals(ArithmeticFunctions.exp10(2.0), 100.0, DELTA);
    assertEquals(ArithmeticFunctions.exp10(3.0), 1000.0, DELTA);
    assertEquals(ArithmeticFunctions.exp10(-1.0), 0.1, DELTA);
    assertTrue(Double.isNaN(ArithmeticFunctions.exp10(Double.NaN)));
    assertEquals(ArithmeticFunctions.exp10(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    assertEquals(ArithmeticFunctions.exp10(Double.NEGATIVE_INFINITY), 0.0, DELTA);
  }

  @Test
  public void testLog1p() {
    assertEquals(ArithmeticFunctions.log1p(0.0), 0.0, DELTA);
    assertEquals(ArithmeticFunctions.log1p(Math.E - 1), 1.0, DELTA);
    assertEquals(ArithmeticFunctions.log1p(-1.0), Double.NEGATIVE_INFINITY);
    assertTrue(Double.isNaN(ArithmeticFunctions.log1p(-2.0)));
    assertTrue(Double.isNaN(ArithmeticFunctions.log1p(Double.NaN)));
    assertEquals(ArithmeticFunctions.log1p(Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    // Verify numerical stability for small values
    double smallValue = 1e-15;
    assertEquals(ArithmeticFunctions.log1p(smallValue), smallValue, smallValue * 1e-5);
  }

  @Test
  public void testSigmoid() {
    assertEquals(ArithmeticFunctions.sigmoid(0.0), 0.5, DELTA);
    assertTrue(ArithmeticFunctions.sigmoid(100.0) > 0.999);
    assertTrue(ArithmeticFunctions.sigmoid(-100.0) < 0.001);
    assertEquals(ArithmeticFunctions.sigmoid(Double.POSITIVE_INFINITY), 1.0, DELTA);
    assertEquals(ArithmeticFunctions.sigmoid(Double.NEGATIVE_INFINITY), 0.0, DELTA);
    assertTrue(Double.isNaN(ArithmeticFunctions.sigmoid(Double.NaN)));
    // Symmetry: sigmoid(x) + sigmoid(-x) == 1
    assertEquals(ArithmeticFunctions.sigmoid(2.0) + ArithmeticFunctions.sigmoid(-2.0), 1.0, DELTA);
  }

  @Test
  public void testPi() {
    assertEquals(ArithmeticFunctions.pi(), Math.PI, DELTA);
  }

  @Test
  public void testE() {
    assertEquals(ArithmeticFunctions.e(), Math.E, DELTA);
  }

  @Test
  public void testBitCount() {
    assertEquals(ArithmeticFunctions.bitCount(0L), 0);
    assertEquals(ArithmeticFunctions.bitCount(1L), 1);
    assertEquals(ArithmeticFunctions.bitCount(7L), 3);
    assertEquals(ArithmeticFunctions.bitCount(255L), 8);
    assertEquals(ArithmeticFunctions.bitCount(-1L), 64);
    assertEquals(ArithmeticFunctions.bitCount(Long.MIN_VALUE), 1);
    assertEquals(ArithmeticFunctions.bitCount(Long.MAX_VALUE), 63);
    // int values promoted to long
    assertEquals(ArithmeticFunctions.bitCount((long) Integer.MAX_VALUE), 31);
    assertEquals(ArithmeticFunctions.bitCount((long) Integer.MIN_VALUE), 33);
  }

  @Test
  public void testFactorial() {
    assertEquals(ArithmeticFunctions.factorial(0), 1L);
    assertEquals(ArithmeticFunctions.factorial(1), 1L);
    assertEquals(ArithmeticFunctions.factorial(2), 2L);
    assertEquals(ArithmeticFunctions.factorial(5), 120L);
    assertEquals(ArithmeticFunctions.factorial(10), 3628800L);
    assertEquals(ArithmeticFunctions.factorial(20), 2432902008176640000L);
  }

  @Test
  public void testFactorialInvalid() {
    assertThrows(IllegalArgumentException.class, () -> ArithmeticFunctions.factorial(-1));
    assertThrows(IllegalArgumentException.class, () -> ArithmeticFunctions.factorial(21));
  }

  @Test
  public void testWidthBucket() {
    // Values within range
    assertEquals(ArithmeticFunctions.widthBucket(5.0, 0.0, 10.0, 5), 3);
    assertEquals(ArithmeticFunctions.widthBucket(0.0, 0.0, 10.0, 5), 1);
    assertEquals(ArithmeticFunctions.widthBucket(1.99, 0.0, 10.0, 5), 1);
    assertEquals(ArithmeticFunctions.widthBucket(2.0, 0.0, 10.0, 5), 2);
    assertEquals(ArithmeticFunctions.widthBucket(9.99, 0.0, 10.0, 5), 5);

    // Below range
    assertEquals(ArithmeticFunctions.widthBucket(-1.0, 0.0, 10.0, 5), 0);

    // At or above upper bound
    assertEquals(ArithmeticFunctions.widthBucket(10.0, 0.0, 10.0, 5), 6);
    assertEquals(ArithmeticFunctions.widthBucket(100.0, 0.0, 10.0, 5), 6);

    // Infinite value (valid — not a bound)
    assertEquals(ArithmeticFunctions.widthBucket(Double.NEGATIVE_INFINITY, 0.0, 10.0, 5), 0);
    assertEquals(ArithmeticFunctions.widthBucket(Double.POSITIVE_INFINITY, 0.0, 10.0, 5), 6);

    // Single bucket
    assertEquals(ArithmeticFunctions.widthBucket(5.0, 0.0, 10.0, 1), 1);
    assertEquals(ArithmeticFunctions.widthBucket(-1.0, 0.0, 10.0, 1), 0);
    assertEquals(ArithmeticFunctions.widthBucket(10.0, 0.0, 10.0, 1), 2);
  }

  @Test
  public void testWidthBucketInvalid() {
    // lo >= hi
    assertThrows(IllegalArgumentException.class, () -> ArithmeticFunctions.widthBucket(5.0, 10.0, 0.0, 5));
    assertThrows(IllegalArgumentException.class, () -> ArithmeticFunctions.widthBucket(5.0, 5.0, 5.0, 5));
    // numBuckets <= 0
    assertThrows(IllegalArgumentException.class, () -> ArithmeticFunctions.widthBucket(5.0, 0.0, 10.0, 0));
    assertThrows(IllegalArgumentException.class, () -> ArithmeticFunctions.widthBucket(5.0, 0.0, 10.0, -1));
    // NaN values
    assertThrows(IllegalArgumentException.class,
        () -> ArithmeticFunctions.widthBucket(Double.NaN, 0.0, 10.0, 5));
    assertThrows(IllegalArgumentException.class,
        () -> ArithmeticFunctions.widthBucket(5.0, Double.NaN, 10.0, 5));
    assertThrows(IllegalArgumentException.class,
        () -> ArithmeticFunctions.widthBucket(5.0, 0.0, Double.NaN, 5));
    // Infinite bounds
    assertThrows(IllegalArgumentException.class,
        () -> ArithmeticFunctions.widthBucket(5.0, Double.NEGATIVE_INFINITY, 10.0, 5));
    assertThrows(IllegalArgumentException.class,
        () -> ArithmeticFunctions.widthBucket(5.0, 0.0, Double.POSITIVE_INFINITY, 5));
  }
}
