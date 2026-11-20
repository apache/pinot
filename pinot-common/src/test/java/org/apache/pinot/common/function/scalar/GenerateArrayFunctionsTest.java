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
  public void testInfiniteIncrementRejected() {
    // The first element is start + 0 * inc, which is NaN for an infinite increment.
    assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.generateDoubleArray(0, 1, Double.POSITIVE_INFINITY));
    assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.generateDoubleArray(1, 0, Double.NEGATIVE_INFINITY));
    assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.generateFloatArray(0f, 1f, Float.POSITIVE_INFINITY));
    assertThrows(IllegalArgumentException.class,
        () -> ArrayFunctions.generateFloatArray(1f, 0f, Float.NEGATIVE_INFINITY));
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
    assertEquals(values.length, 11);
    assertEquals(values[10], 1.0, 1e-12);
  }

  @Test
  public void testFloatingPointStepKeepsInexactEnd() {
    // (0.3 - 0.1) / 0.1 is 1.9999999999999998, so flooring the step count would drop the end. The single-stage
    // engine has always kept it, and so does this.
    assertEquals(ArrayFunctions.generateDoubleArray(0.1, 0.3, 0.1), new double[]{0.1, 0.1 + 0.1, 0.1 + 0.1 * 2});
    assertEquals(ArrayFunctions.generateDoubleArray(0.3, 0.1, -0.1).length, 3);
    assertEquals(ArrayFunctions.generateFloatArray(0.1f, 0.3f, 0.1f).length, 3);
    assertEquals(ArrayFunctions.generateDoubleArray(1e6 + 0.1, 1e6 + 0.3, 0.1).length, 3);
  }

  @Test
  public void testFloatingPointEndBetweenStepsIsExcluded() {
    assertEquals(ArrayFunctions.generateDoubleArray(0, 0.95, 0.1).length, 10);
    assertEquals(ArrayFunctions.generateDoubleArray(0, 0.2999, 0.1).length, 3);
    assertEquals(ArrayFunctions.generateFloatArray(0f, 0.95f, 0.1f).length, 10);
  }

  @Test
  public void testDecimalStepsKeepEndDespiteRounding() {
    assertEquals(ArrayFunctions.generateDoubleArray(0.1, 0.7, 0.1).length, 7);
    assertEquals(ArrayFunctions.generateDoubleArray(0.7, 0.1, -0.1).length, 7);
    assertEquals(ArrayFunctions.generateDoubleArray(0, 0.3, 0.1).length, 4);
    assertEquals(ArrayFunctions.generateDoubleArray(0, 0.6, 0.1).length, 7);
    assertEquals(ArrayFunctions.generateDoubleArray(0, 0.7, 0.1).length, 8);
    assertEquals(ArrayFunctions.generateDoubleArray(0, 0.9, 0.3).length, 4);

    assertEquals(ArrayFunctions.generateFloatArray(0.1f, 0.7f, 0.1f).length, 7);
    assertEquals(ArrayFunctions.generateFloatArray(0.7f, 0.1f, -0.1f).length, 7);
    assertEquals(ArrayFunctions.generateFloatArray(0, 0.3f, 0.1f).length, 4);
    assertEquals(ArrayFunctions.generateFloatArray(0, 0.6f, 0.1f).length, 7);
    assertEquals(ArrayFunctions.generateFloatArray(0, 0.7f, 0.1f).length, 8);
    assertEquals(ArrayFunctions.generateFloatArray(0, 0.9f, 0.3f).length, 4);

    // The tolerance scales with the magnitude of the input values.
    assertEquals(ArrayFunctions.generateDoubleArray(1e6 + 0.1, 1e6 + 0.3, 0.1).length, 3);
  }

  @Test
  public void testFractionalStepIsNotPromotedToWholeStep() {
    // The step count is exactly 0.75 with no rounding error, and the bounds can tell 1000000.75 from 1000000, so the
    // range holds its start only.
    assertEquals(ArrayFunctions.generateFloatArray(1000000f, 1000000.75f, 1f), new float[]{1000000f});

    // Exactly 1.5 steps at a magnitude where one ulp of the bounds is about an eighth of the step. The step at 1.5
    // is genuinely beyond the end, so only the two values up to 1e15 + 1 may be generated.
    assertEquals(ArrayFunctions.generateDoubleArray(1e15, 1e15 + 1.5, 1d), new double[]{1e15, 1e15 + 1});

    // Ordinary magnitudes: the tolerance used to grow with the bounds until it swallowed a quarter of a step.
    assertEquals(ArrayFunctions.generateDoubleArray(1.0, 1.5, 1d), new double[]{1.0});
    assertEquals(ArrayFunctions.generateFloatArray(0.5f, 2.0f, 0.25f).length, 7);
  }

  @Test
  public void testFloatDecimalStepDoesNotOvershootEnd() {
    // In float, (91322.45 - 91321.76) / 0.09 is 7.7257, and no step lands on the end: the last one below it is
    // 91322.39. Snapping the step count to 8 added 91322.48, three float ulps past the end.
    float[] values = ArrayFunctions.generateFloatArray(91321.76f, 91322.45f, 0.09f);
    assertEquals(values.length, 8);
    assertEquals(values[7], 91322.39f);
    assertTrue(values[7] < 91322.45f);

    // An ordinary fractional step: 6 steps, ending exactly on the last bound.
    assertEquals(ArrayFunctions.generateFloatArray(0.5f, 2.0f, 0.25f),
        new float[]{0.5f, 0.75f, 1.0f, 1.25f, 1.5f, 1.75f, 2.0f});

    // A step far below the spacing of the bounds cannot be represented, so the sequence is just the start.
    assertEquals(ArrayFunctions.generateFloatArray(200000f, 200000.7f, 1f), new float[]{200000f});
  }

  @Test
  public void testEndExactlyOnStepIsKept() {
    // (163909.03 - 163443.44) / 9.7 is 47.9994, so flooring the step count drops the end. In float the 48th element
    // is 163909.03, exactly the bound, so the sequence has to keep it.
    float[] values = ArrayFunctions.generateFloatArray(163443.44f, 163909.03f, 9.7f);
    assertEquals(values.length, 49);
    assertEquals(values[48], 163909.03f);
  }

  @Test
  public void testFloatStepCountAtLargeMagnitude() {
    // At 1e6 a float holds about an eighth of a step, so the quotient is nearly a whole step away from its exact
    // count in a way the bounds cannot settle. Twelve steps of 6.9 is 82.8, and 82.8 is the width of the range.
    float[] values = ArrayFunctions.generateFloatArray(1048558.1f, 1048889.3f, 6.9f);
    assertEquals(values.length, 48);
    assertEquals(values[47], 1048882.4f);
    assertTrue(values[47] < 1048889.3f);
  }

  @Test
  public void testDoubleDecimalStepAtLargeMagnitude() {
    // A tenth at 1e15 is smaller than the spacing of the bounds, so the step count has to come from the values the
    // arguments denote rather than from their binary expansion.
    double[] values = ArrayFunctions.generateDoubleArray(1e15, 1e15 + 1.0, 0.1);
    assertEquals(values.length, 11);
    assertEquals(values[10], 1e15 + 1.0);

    assertEquals(ArrayFunctions.generateDoubleArray(1e9, 1e9 + 0.3, 0.1).length, 4);
    assertEquals(ArrayFunctions.generateDoubleArray(0, 1e6, 1e3).length, 1001);
  }
}
