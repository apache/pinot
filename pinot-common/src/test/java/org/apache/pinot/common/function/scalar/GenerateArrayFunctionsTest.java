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


/// Tests the `generate*Array` sequence generators, in particular the bounds that keep a literal call from producing
/// an unusable array on the broker.
public class GenerateArrayFunctionsTest {

  @Test
  public void testAscendingSequence() {
    assertEquals(ArrayFunctions.generateLongArray(0, 10, 2), new long[]{0, 2, 4, 6, 8, 10});
    assertEquals(ArrayFunctions.generateLongArray(0, 4), new long[]{0, 1, 2, 3, 4});
  }

  @Test
  public void testDescendingSequence() {
    assertEquals(ArrayFunctions.generateLongArray(10, 0, -5), new long[]{10, 5, 0});
    assertEquals(ArrayFunctions.generateLongArray(2, -2), new long[]{2, 1, 0, -1, -2});
  }

  @Test
  public void testEndNotOnAStepIsNotPassed() {
    assertEquals(ArrayFunctions.generateLongArray(0, 9, 4), new long[]{0, 4, 8});
    assertEquals(ArrayFunctions.generateLongArray(0, -9, -4), new long[]{0, -4, -8});
  }

  @Test
  public void testSingleElementSequence() {
    // A zero span is a legitimate one bucket grid rather than an error.
    assertEquals(ArrayFunctions.generateLongArray(7, 7, 1), new long[]{7});
    assertEquals(ArrayFunctions.generateLongArray(7, 7, -1), new long[]{7});
    assertEquals(ArrayFunctions.generateLongArray(7, 7), new long[]{7});
  }

  @Test
  public void testTimeGrid() {
    // The motivating case: a 30 minute grid over 3 hours, as used to gap fill a sparse time series.
    long[] grid = ArrayFunctions.generateLongArray(1633078800000L, 1633089600000L, 1800000L);
    assertEquals(grid.length, 7);
    assertEquals(grid[0], 1633078800000L);
    assertEquals(grid[6], 1633089600000L);
  }

  @Test
  public void testTimestampsBeyondDoublePrecision() {
    // Nanosecond timestamps exceed 2^53, so the length must not be computed in floating point.
    long start = 1633078800000000000L;
    assertEquals(ArrayFunctions.generateLongArray(start, start + 4, 2), new long[]{start, start + 2, start + 4});
  }

  @Test
  public void testZeroIncrementRejected() {
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateLongArray(0, 10, 0));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateIntArray(0, 10, 0));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateFloatArray(0, 10, 0));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateDoubleArray(0, 10, 0));
  }

  @Test
  public void testIncrementPointingAwayFromEndRejected() {
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateLongArray(10, 0, 1));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateLongArray(0, 10, -1));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateIntArray(10, 0, 1));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateFloatArray(10, 0, 1));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateDoubleArray(10, 0, 1));
  }

  @Test
  public void testOversizedSequenceRejected() {
    assertEquals(ArrayFunctions.generateLongArray(1, 100000, 1).length, 100000);
    // One element past the limit, so the rejection is the limit and not an incidental overflow.
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateLongArray(0, 100000, 1));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateDoubleArray(0, 1, 0.000001));
  }

  @Test
  public void testRangeWiderThanLongRejected() {
    // The span overflows a long. It used to be truncated to an int, silently yielding an empty array.
    assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.generateLongArray(Long.MIN_VALUE, Long.MAX_VALUE, 1));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateLongArray(0, Long.MAX_VALUE, 1));
  }

  @Test
  public void testNonFiniteBoundsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.generateDoubleArray(0, Double.POSITIVE_INFINITY, 1));
    assertThrows(IllegalArgumentException.class, () -> ArrayFunctions.generateDoubleArray(0, 10, Double.NaN));
  }

  @Test
  public void testIntAndFloatSequences() {
    assertEquals(ArrayFunctions.generateIntArray(0, 10, 5), new int[]{0, 5, 10});
    assertEquals(ArrayFunctions.generateFloatArray(0f, 1f, 0.5f), new float[]{0f, 0.5f, 1f});
  }

  @Test
  public void testFloatingPointStepDoesNotDrift() {
    // Accumulating the step would leave the last element short of 1.0.
    double[] values = ArrayFunctions.generateDoubleArray(0, 1, 0.1);
    assertTrue(values.length >= 10, "Expected at least 10 elements, got: " + values.length);
    assertEquals(values[10], 1.0, 1e-12);
  }
}
