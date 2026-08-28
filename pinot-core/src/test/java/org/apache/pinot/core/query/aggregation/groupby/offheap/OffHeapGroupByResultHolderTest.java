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
package org.apache.pinot.core.query.aggregation.groupby.offheap;

import java.util.Random;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.IntGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.LongGroupByResultHolder;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests for the off-heap fixed-width `GroupByResultHolder` implementations, asserting semantic equivalence
/// with their on-heap twins.
public class OffHeapGroupByResultHolderTest {
  private static final long RANDOM_SEED = 42;
  private static final int NUM_SLOTS = 50_000;
  private static final int NUM_OPERATIONS = 100_000;
  private static final int INITIAL_CAPACITY = 100;
  private static final int MAX_GROWTH_STEP = 5000;

  @Test
  public void testDifferentialDouble() {
    Random random = new Random(RANDOM_SEED);
    DoubleGroupByResultHolder onHeap = new DoubleGroupByResultHolder(INITIAL_CAPACITY, NUM_SLOTS, -1.0);
    try (OffHeapDoubleGroupByResultHolder offHeap =
        new OffHeapDoubleGroupByResultHolder(INITIAL_CAPACITY, NUM_SLOTS, -1.0)) {
      int capacity = INITIAL_CAPACITY;
      for (int i = 0; i < NUM_OPERATIONS; i++) {
        int op = random.nextInt(10);
        if (op == 0 && capacity < NUM_SLOTS) {
          int newCapacity = Math.min(capacity + 1 + random.nextInt(MAX_GROWTH_STEP), NUM_SLOTS);
          onHeap.ensureCapacity(newCapacity);
          offHeap.ensureCapacity(newCapacity);
          capacity = newCapacity;
        } else if (op < 6) {
          int groupKey = nextGroupKey(random, capacity);
          double value = random.nextDouble();
          onHeap.setValueForKey(groupKey, value);
          offHeap.setValueForKey(groupKey, value);
        } else {
          int groupKey = nextGroupKey(random, capacity);
          assertDoubleEquals(offHeap.getDoubleResult(groupKey), onHeap.getDoubleResult(groupKey));
        }
      }
      onHeap.ensureCapacity(NUM_SLOTS);
      offHeap.ensureCapacity(NUM_SLOTS);
      for (int groupKey = 0; groupKey < NUM_SLOTS; groupKey++) {
        assertDoubleEquals(offHeap.getDoubleResult(groupKey), onHeap.getDoubleResult(groupKey));
      }
    }
  }

  @Test
  public void testDifferentialLong() {
    Random random = new Random(RANDOM_SEED);
    LongGroupByResultHolder onHeap = new LongGroupByResultHolder(INITIAL_CAPACITY, NUM_SLOTS, -1L);
    try (OffHeapLongGroupByResultHolder offHeap =
        new OffHeapLongGroupByResultHolder(INITIAL_CAPACITY, NUM_SLOTS, -1L)) {
      int capacity = INITIAL_CAPACITY;
      for (int i = 0; i < NUM_OPERATIONS; i++) {
        int op = random.nextInt(10);
        if (op == 0 && capacity < NUM_SLOTS) {
          int newCapacity = Math.min(capacity + 1 + random.nextInt(MAX_GROWTH_STEP), NUM_SLOTS);
          onHeap.ensureCapacity(newCapacity);
          offHeap.ensureCapacity(newCapacity);
          capacity = newCapacity;
        } else if (op < 6) {
          int groupKey = nextGroupKey(random, capacity);
          long value = random.nextLong();
          onHeap.setValueForKey(groupKey, value);
          offHeap.setValueForKey(groupKey, value);
        } else {
          int groupKey = nextGroupKey(random, capacity);
          Assert.assertEquals(offHeap.getLongResult(groupKey), onHeap.getLongResult(groupKey));
        }
      }
      onHeap.ensureCapacity(NUM_SLOTS);
      offHeap.ensureCapacity(NUM_SLOTS);
      for (int groupKey = 0; groupKey < NUM_SLOTS; groupKey++) {
        Assert.assertEquals(offHeap.getLongResult(groupKey), onHeap.getLongResult(groupKey));
      }
    }
  }

  @Test
  public void testDifferentialInt() {
    Random random = new Random(RANDOM_SEED);
    IntGroupByResultHolder onHeap = new IntGroupByResultHolder(INITIAL_CAPACITY, NUM_SLOTS, -1);
    try (OffHeapIntGroupByResultHolder offHeap =
        new OffHeapIntGroupByResultHolder(INITIAL_CAPACITY, NUM_SLOTS, -1)) {
      int capacity = INITIAL_CAPACITY;
      for (int i = 0; i < NUM_OPERATIONS; i++) {
        int op = random.nextInt(10);
        if (op == 0 && capacity < NUM_SLOTS) {
          int newCapacity = Math.min(capacity + 1 + random.nextInt(MAX_GROWTH_STEP), NUM_SLOTS);
          onHeap.ensureCapacity(newCapacity);
          offHeap.ensureCapacity(newCapacity);
          capacity = newCapacity;
        } else if (op < 6) {
          int groupKey = nextGroupKey(random, capacity);
          int value = random.nextInt();
          onHeap.setValueForKey(groupKey, value);
          offHeap.setValueForKey(groupKey, value);
        } else {
          int groupKey = nextGroupKey(random, capacity);
          Assert.assertEquals(offHeap.getIntResult(groupKey), onHeap.getIntResult(groupKey));
        }
      }
      onHeap.ensureCapacity(NUM_SLOTS);
      offHeap.ensureCapacity(NUM_SLOTS);
      for (int groupKey = 0; groupKey < NUM_SLOTS; groupKey++) {
        Assert.assertEquals(offHeap.getIntResult(groupKey), onHeap.getIntResult(groupKey));
      }
    }
  }

  @Test
  public void testDefaultValueVisibilityDouble() {
    double[] defaultValues = {0.0, 3.25, Double.NEGATIVE_INFINITY, Double.NaN};
    for (double defaultValue : defaultValues) {
      try (OffHeapDoubleGroupByResultHolder holder = new OffHeapDoubleGroupByResultHolder(8, 1000, defaultValue)) {
        assertDoubleEquals(holder.getDefaultValue(), defaultValue);
        for (int i = 0; i < 8; i += 2) {
          holder.setValueForKey(i, (double) i);
        }
        // Multiple growths; the extended tail must be visible as the default value after each one
        holder.ensureCapacity(20);
        holder.ensureCapacity(100);
        holder.ensureCapacity(1000);
        for (int i = 0; i < 8; i += 2) {
          assertDoubleEquals(holder.getDoubleResult(i), i);
        }
        for (int i = 1; i < 8; i += 2) {
          assertDoubleEquals(holder.getDoubleResult(i), defaultValue);
        }
        for (int i = 8; i < 1000; i++) {
          assertDoubleEquals(holder.getDoubleResult(i), defaultValue);
        }
        assertDoubleEquals(holder.getDoubleResult(GroupKeyGenerator.INVALID_ID), defaultValue);
      }
    }
  }

  @Test
  public void testDefaultValueVisibilityLong() {
    long[] defaultValues = {0L, -42L, Long.MIN_VALUE};
    for (long defaultValue : defaultValues) {
      try (OffHeapLongGroupByResultHolder holder = new OffHeapLongGroupByResultHolder(8, 1000, defaultValue)) {
        Assert.assertEquals(holder.getDefaultValue(), defaultValue);
        for (int i = 0; i < 8; i += 2) {
          holder.setValueForKey(i, (long) i);
        }
        holder.ensureCapacity(20);
        holder.ensureCapacity(100);
        holder.ensureCapacity(1000);
        for (int i = 0; i < 8; i += 2) {
          Assert.assertEquals(holder.getLongResult(i), i);
        }
        for (int i = 1; i < 8; i += 2) {
          Assert.assertEquals(holder.getLongResult(i), defaultValue);
        }
        for (int i = 8; i < 1000; i++) {
          Assert.assertEquals(holder.getLongResult(i), defaultValue);
        }
        Assert.assertEquals(holder.getLongResult(GroupKeyGenerator.INVALID_ID), defaultValue);
      }
    }
  }

  @Test
  public void testDefaultValueVisibilityInt() {
    int[] defaultValues = {0, -42, Integer.MIN_VALUE};
    for (int defaultValue : defaultValues) {
      try (OffHeapIntGroupByResultHolder holder = new OffHeapIntGroupByResultHolder(8, 1000, defaultValue)) {
        Assert.assertEquals(holder.getDefaultValue(), defaultValue);
        for (int i = 0; i < 8; i += 2) {
          holder.setValueForKey(i, i + 1000);
        }
        holder.ensureCapacity(20);
        holder.ensureCapacity(100);
        holder.ensureCapacity(1000);
        for (int i = 0; i < 8; i += 2) {
          Assert.assertEquals(holder.getIntResult(i), i + 1000);
        }
        for (int i = 1; i < 8; i += 2) {
          Assert.assertEquals(holder.getIntResult(i), defaultValue);
        }
        for (int i = 8; i < 1000; i++) {
          Assert.assertEquals(holder.getIntResult(i), defaultValue);
        }
        Assert.assertEquals(holder.getIntResult(GroupKeyGenerator.INVALID_ID), defaultValue);
      }
    }
  }

  @Test
  public void testEnsureCapacityBeyondMaxCapacityThrows() {
    try (OffHeapDoubleGroupByResultHolder doubleHolder = new OffHeapDoubleGroupByResultHolder(10, 100, 0.0);
        OffHeapLongGroupByResultHolder longHolder = new OffHeapLongGroupByResultHolder(10, 100, 0L);
        OffHeapIntGroupByResultHolder intHolder = new OffHeapIntGroupByResultHolder(10, 100, 0)) {
      Assert.assertThrows(IllegalArgumentException.class, () -> doubleHolder.ensureCapacity(101));
      Assert.assertThrows(IllegalArgumentException.class, () -> longHolder.ensureCapacity(101));
      Assert.assertThrows(IllegalArgumentException.class, () -> intHolder.ensureCapacity(101));
    }

    // On-heap twins must behave identically
    DoubleGroupByResultHolder onHeapDouble = new DoubleGroupByResultHolder(10, 100, 0.0);
    LongGroupByResultHolder onHeapLong = new LongGroupByResultHolder(10, 100, 0L);
    IntGroupByResultHolder onHeapInt = new IntGroupByResultHolder(10, 100, 0);
    Assert.assertThrows(IllegalArgumentException.class, () -> onHeapDouble.ensureCapacity(101));
    Assert.assertThrows(IllegalArgumentException.class, () -> onHeapLong.ensureCapacity(101));
    Assert.assertThrows(IllegalArgumentException.class, () -> onHeapInt.ensureCapacity(101));
  }

  @Test
  public void testGrowthClampsToMaxCapacity() {
    // Doubling 10 -> 20 must clamp to maxCapacity 15; the clamped tail must be default-initialized and writable
    try (OffHeapDoubleGroupByResultHolder holder = new OffHeapDoubleGroupByResultHolder(10, 15, -1.0)) {
      holder.ensureCapacity(12);
      for (int i = 10; i < 15; i++) {
        assertDoubleEquals(holder.getDoubleResult(i), -1.0);
      }
      holder.setValueForKey(14, 42.0);
      assertDoubleEquals(holder.getDoubleResult(14), 42.0);
      holder.ensureCapacity(15);
      assertDoubleEquals(holder.getDoubleResult(14), 42.0);
      Assert.assertThrows(IllegalArgumentException.class, () -> holder.ensureCapacity(16));
    }
    try (OffHeapLongGroupByResultHolder holder = new OffHeapLongGroupByResultHolder(10, 15, -1L)) {
      holder.ensureCapacity(12);
      for (int i = 10; i < 15; i++) {
        Assert.assertEquals(holder.getLongResult(i), -1L);
      }
      holder.setValueForKey(14, 42L);
      Assert.assertEquals(holder.getLongResult(14), 42L);
      holder.ensureCapacity(15);
      Assert.assertEquals(holder.getLongResult(14), 42L);
      Assert.assertThrows(IllegalArgumentException.class, () -> holder.ensureCapacity(16));
    }
    try (OffHeapIntGroupByResultHolder holder = new OffHeapIntGroupByResultHolder(10, 15, -1)) {
      holder.ensureCapacity(12);
      for (int i = 10; i < 15; i++) {
        Assert.assertEquals(holder.getIntResult(i), -1);
      }
      holder.setValueForKey(14, 42);
      Assert.assertEquals(holder.getIntResult(14), 42);
      holder.ensureCapacity(15);
      Assert.assertEquals(holder.getIntResult(14), 42);
      Assert.assertThrows(IllegalArgumentException.class, () -> holder.ensureCapacity(16));
    }
  }

  @Test
  public void testUnsupportedTypedMethodsThrow() {
    try (OffHeapDoubleGroupByResultHolder doubleHolder = new OffHeapDoubleGroupByResultHolder(10, 100, 0.0);
        OffHeapLongGroupByResultHolder longHolder = new OffHeapLongGroupByResultHolder(10, 100, 0L);
        OffHeapIntGroupByResultHolder intHolder = new OffHeapIntGroupByResultHolder(10, 100, 0)) {
      Assert.assertThrows(UnsupportedOperationException.class, () -> doubleHolder.getIntResult(0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> doubleHolder.getLongResult(0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> doubleHolder.getResult(0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> doubleHolder.setValueForKey(0, 1));
      Assert.assertThrows(UnsupportedOperationException.class, () -> doubleHolder.setValueForKey(0, 1L));
      Assert.assertThrows(UnsupportedOperationException.class, () -> doubleHolder.setValueForKey(0, (Object) "v"));

      Assert.assertThrows(UnsupportedOperationException.class, () -> longHolder.getDoubleResult(0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> longHolder.getIntResult(0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> longHolder.getResult(0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> longHolder.setValueForKey(0, 1.0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> longHolder.setValueForKey(0, 1));
      Assert.assertThrows(UnsupportedOperationException.class, () -> longHolder.setValueForKey(0, (Object) "v"));

      Assert.assertThrows(UnsupportedOperationException.class, () -> intHolder.getDoubleResult(0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> intHolder.getLongResult(0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> intHolder.getResult(0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> intHolder.setValueForKey(0, 1.0));
      Assert.assertThrows(UnsupportedOperationException.class, () -> intHolder.setValueForKey(0, 1L));
      Assert.assertThrows(UnsupportedOperationException.class, () -> intHolder.setValueForKey(0, (Object) "v"));
    }
  }

  @Test
  public void testZeroInitialCapacity() {
    try (OffHeapDoubleGroupByResultHolder holder = new OffHeapDoubleGroupByResultHolder(0, 100, -1.0)) {
      holder.ensureCapacity(10);
      assertDoubleEquals(holder.getDoubleResult(5), -1.0);
      holder.setValueForKey(5, 3.5);
      assertDoubleEquals(holder.getDoubleResult(5), 3.5);
    }
    try (OffHeapLongGroupByResultHolder holder = new OffHeapLongGroupByResultHolder(0, 100, -1L)) {
      holder.ensureCapacity(10);
      Assert.assertEquals(holder.getLongResult(5), -1L);
      holder.setValueForKey(5, 3L);
      Assert.assertEquals(holder.getLongResult(5), 3L);
    }
    try (OffHeapIntGroupByResultHolder holder = new OffHeapIntGroupByResultHolder(0, 100, -1)) {
      holder.ensureCapacity(10);
      Assert.assertEquals(holder.getIntResult(5), -1);
      holder.setValueForKey(5, 3);
      Assert.assertEquals(holder.getIntResult(5), 3);
    }
  }

  /// Exercises the wrapper-based fallback arms of the holder accessors and fills that normally only run for
  /// buffers beyond the 2GB view limit.
  @Test
  public void testDifferentialWithoutViews() {
    OffHeapGroupByUtils.setViewSizeLimitBytes(0);
    try {
      testDifferentialDouble();
      testDifferentialLong();
      testDifferentialInt();
      testDefaultValueVisibilityDouble();
    } finally {
      OffHeapGroupByUtils.setViewSizeLimitBytes(Integer.MAX_VALUE);
    }
  }

  @Test
  public void testCloseIsIdempotent() {
    OffHeapDoubleGroupByResultHolder doubleHolder = new OffHeapDoubleGroupByResultHolder(10, 100, 0.0);
    doubleHolder.close();
    doubleHolder.close();

    OffHeapLongGroupByResultHolder longHolder = new OffHeapLongGroupByResultHolder(10, 100, 0L);
    longHolder.close();
    longHolder.close();

    OffHeapIntGroupByResultHolder intHolder = new OffHeapIntGroupByResultHolder(10, 100, 0);
    intHolder.close();
    intHolder.close();
  }

  @Test
  public void testNoDirectMemoryLeak() {
    long baseline = PinotDataBuffer.getDirectBufferUsage();
    try (OffHeapDoubleGroupByResultHolder doubleHolder = new OffHeapDoubleGroupByResultHolder(128, 4096, -1.0);
        OffHeapLongGroupByResultHolder longHolder = new OffHeapLongGroupByResultHolder(128, 4096, -1L);
        OffHeapIntGroupByResultHolder intHolder = new OffHeapIntGroupByResultHolder(128, 4096, -1)) {
      // Grow several times so intermediate buffers are allocated and released along the way
      for (int capacity : new int[]{256, 1000, 4096}) {
        doubleHolder.ensureCapacity(capacity);
        longHolder.ensureCapacity(capacity);
        intHolder.ensureCapacity(capacity);
      }
      for (int i = 0; i < 4096; i++) {
        doubleHolder.setValueForKey(i, (double) i);
        longHolder.setValueForKey(i, (long) i);
        intHolder.setValueForKey(i, i);
      }
      Assert.assertTrue(PinotDataBuffer.getDirectBufferUsage() > baseline);
    }
    Assert.assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline);
  }

  private static int nextGroupKey(Random random, int capacity) {
    // Roughly 5% of the accesses target INVALID_ID to exercise the guard paths
    return random.nextInt(20) == 0 ? GroupKeyGenerator.INVALID_ID : random.nextInt(capacity);
  }

  private static void assertDoubleEquals(double actual, double expected) {
    // Bit-wise comparison so that NaN default values are asserted correctly
    Assert.assertEquals(Double.doubleToLongBits(actual), Double.doubleToLongBits(expected));
  }
}
