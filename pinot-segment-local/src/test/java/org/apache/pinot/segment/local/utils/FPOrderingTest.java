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
package org.apache.pinot.segment.local.utils;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FPOrderingTest {

  @Test
  public void testNaN() {
    assertEquals(0xFFFFFFFFFFFFFFFFL, FPOrdering.ordinalOf(Double.NaN));
    assertEquals(0xFFFFFFFF, FPOrdering.ordinalOf(Float.NaN));
  }

  @Test
  public void testInfinities() {
    assertEquals(0, FPOrdering.ordinalOf(Double.NEGATIVE_INFINITY));
    assertEquals(0, FPOrdering.ordinalOf(Float.NEGATIVE_INFINITY));
    assertEquals(0xFFFFFFFFFFFFFFFFL, FPOrdering.ordinalOf(Double.POSITIVE_INFINITY));
    assertEquals(0xFFFFFFFF, FPOrdering.ordinalOf(Float.POSITIVE_INFINITY));
  }

  @Test
  public void testZeroes() {
    assertEquals(0x8000000000000000L, FPOrdering.ordinalOf(0D));
    assertEquals(0x80000000L, FPOrdering.ordinalOf(0F));
    assertEquals(0x8000000000000000L, FPOrdering.ordinalOf(-0D));
    assertEquals(0x80000000L, FPOrdering.ordinalOf(-0F));
    assertTrue(Long.compareUnsigned(FPOrdering.ordinalOf(0D),
        FPOrdering.ordinalOf(-1D)) > 0);
    assertTrue(Long.compareUnsigned(FPOrdering.ordinalOf(0F),
        FPOrdering.ordinalOf(-1F)) > 0);
    assertTrue(Long.compareUnsigned(FPOrdering.ordinalOf(0D),
        FPOrdering.ordinalOf(-1e-200D)) > 0);
    assertTrue(Long.compareUnsigned(FPOrdering.ordinalOf(0F),
        FPOrdering.ordinalOf(-1e-20F)) > 0);
  }

  @Test
  public void testOrderingPositive() {
    testDouble(createDoubles(i -> i, 1));
    testDouble(createDoubles(i -> 1, 1));
    testFloat(createFloats(i -> i, 1));
    testFloat(createFloats(i -> 1, 1));
  }

  @Test
  public void testOrderingNegative() {
    testDouble(createDoubles(i -> -i, 1));
    testDouble(createDoubles(i -> -1, 1));
    testFloat(createFloats(i -> -i, 1));
    testFloat(createFloats(i -> -1, 1));
  }

  @Test
  public void testOrderingMixed() {
    testDouble(createDoubles(i -> 1, -1));
    testDouble(createDoubles(i -> i, -1));
    testFloat(createFloats(i -> 1, -1));
    testFloat(createFloats(i -> i, -1));
  }

  private float[] createFloats(IntUnaryOperator multiplier, int signumMultiplier) {
    float[] floats = new float[100_000];
    int signum = 1;
    for (int i = 0; i < floats.length; i++) {
      floats[i] = signum * multiplier.applyAsInt(i) * ThreadLocalRandom.current().nextFloat();
      signum *= signumMultiplier;
    }
    Arrays.sort(floats);
    return floats;
  }

  private double[] createDoubles(IntUnaryOperator multiplier, int signumMultiplier) {
    double[] doubles = new double[100_000];
    int signum = 1;
    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = signum * multiplier.applyAsInt(i) * ThreadLocalRandom.current().nextDouble();
      signum *= signumMultiplier;
    }
    Arrays.sort(doubles);
    return doubles;
  }

  private void testDouble(double[] values) {
    assertOrdered(Arrays.stream(values).mapToLong(FPOrdering::ordinalOf).toArray(), values);
  }

  private void testFloat(float[] values) {
    assertOrdered(IntStream.range(0, values.length).mapToLong(i -> FPOrdering.ordinalOf(values[i])).toArray(), values);
  }

  private void assertOrdered(long[] ordinals, double[] values) {
    for (int i = 1; i < ordinals.length; i++) {
      if (values[i] > values[i - 1]) {
        assertTrue(Long.compareUnsigned(ordinals[i], ordinals[i - 1]) >= 0,
            ordinals[i] + "<=" + ordinals[i - 1] + " (" + values[i] + "<=" + values[i - 1] + ")");
      }
    }
  }

  private void assertOrdered(long[] ordinals, float[] values) {
    for (int i = 1; i < ordinals.length; i++) {
      if (values[i] > values[i - 1]) {
        assertTrue(Long.compareUnsigned(ordinals[i], ordinals[i - 1]) > 0,
            ordinals[i] + "<=" + ordinals[i - 1] + " (" + values[i] + "<=" + values[i - 1] + ")");
      }
    }
  }
}
