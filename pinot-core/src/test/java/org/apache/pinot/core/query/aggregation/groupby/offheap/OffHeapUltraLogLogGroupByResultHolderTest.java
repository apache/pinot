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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests for [OffHeapUltraLogLogGroupByResultHolder], pinning the vendored register-update math byte-identical
/// to hash4j's [UltraLogLog] across precisions and asserting on-heap-equivalent holder semantics (untouched
/// groups, growth, delegate mode, close).
public class OffHeapUltraLogLogGroupByResultHolderTest {
  private static final long RANDOM_SEED = 42;
  private static final int NUM_SLOTS = 2000;
  private static final int INITIAL_CAPACITY = 16;
  private static final int NUM_OPERATIONS = 200_000;
  // Register-update edge cases on top of random hashes
  private static final long[] EDGE_HASHES = {0L, -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE, 0x8000000000000001L};

  @Test
  public void testDifferentialAddAcrossPrecisions() {
    // p=3 is the library minimum (8B slots), 12 is the Pinot default (4KB), 18 fills a whole chunk, 19 exceeds
    // the target chunk size (one slot per chunk)
    for (int p : new int[]{3, 8, 12, 18, 19}) {
      runDifferential(p, p >= 18 ? 40 : NUM_SLOTS, p >= 18 ? 20_000 : NUM_OPERATIONS);
    }
  }

  @Test
  public void testDifferentialWithoutViews() {
    // Force the PinotDataBuffer wrapper fallback arm of every view fast path
    OffHeapGroupByUtils.setViewSizeLimitBytes(0);
    try {
      runDifferential(12, 500, 50_000);
    } finally {
      OffHeapGroupByUtils.setViewSizeLimitBytes(Integer.MAX_VALUE);
    }
  }

  private void runDifferential(int p, int numSlots, int numOperations) {
    Random random = new Random(RANDOM_SEED + p);
    Map<Integer, UltraLogLog> reference = new HashMap<>();
    try (OffHeapUltraLogLogGroupByResultHolder offHeap =
        new OffHeapUltraLogLogGroupByResultHolder(p, INITIAL_CAPACITY, numSlots)) {
      int capacity = INITIAL_CAPACITY;
      for (int i = 0; i < numOperations; i++) {
        int op = random.nextInt(20);
        if (op == 0 && capacity < numSlots) {
          int newCapacity = Math.min(capacity + 1 + random.nextInt(numSlots / 4), numSlots);
          offHeap.ensureCapacity(newCapacity);
          capacity = newCapacity;
        } else if (op == 1) {
          int groupKey = random.nextInt(capacity);
          offHeap.touch(groupKey);
          reference.computeIfAbsent(groupKey, k -> UltraLogLog.create(p));
        } else {
          int groupKey = random.nextInt(capacity);
          long hashValue = op < 5 ? EDGE_HASHES[random.nextInt(EDGE_HASHES.length)] : random.nextLong();
          offHeap.add(groupKey, hashValue);
          reference.computeIfAbsent(groupKey, k -> UltraLogLog.create(p)).add(hashValue);
        }
      }
      offHeap.ensureCapacity(numSlots);
      for (int groupKey = 0; groupKey < numSlots; groupKey++) {
        UltraLogLog expected = reference.get(groupKey);
        UltraLogLog actual = offHeap.getResult(groupKey);
        if (expected == null) {
          Assert.assertNull(actual, "untouched group " + groupKey + " must materialize as null");
        } else {
          Assert.assertNotNull(actual, "touched group " + groupKey + " must not materialize as null");
          Assert.assertEquals(actual.getState(), expected.getState(),
              "state bytes diverged from hash4j for group " + groupKey + " at p=" + p);
          Assert.assertEquals(actual.getDistinctCountEstimate(), expected.getDistinctCountEstimate());
        }
      }
    }
  }

  @Test
  public void testTouchCreatesEmptyState() {
    try (OffHeapUltraLogLogGroupByResultHolder holder =
        new OffHeapUltraLogLogGroupByResultHolder(12, INITIAL_CAPACITY, NUM_SLOTS)) {
      Assert.assertNull(holder.getResult(0));
      holder.touch(0);
      UltraLogLog materialized = holder.getResult(0);
      Assert.assertNotNull(materialized);
      Assert.assertEquals(materialized.getState(), UltraLogLog.create(12).getState());
      // Materialization returns a copy: mutating it must not touch the slot
      materialized.add(12345L);
      Assert.assertEquals(((UltraLogLog) holder.getResult(0)).getState(), UltraLogLog.create(12).getState());
    }
  }

  @Test
  public void testDelegateMode() {
    try (OffHeapUltraLogLogGroupByResultHolder holder =
        new OffHeapUltraLogLogGroupByResultHolder(12, INITIAL_CAPACITY, NUM_SLOTS)) {
      Assert.assertNull(holder.getResult(3));
      Object dictWrapper = new Object();
      holder.setValueForKey(3, dictWrapper);
      Assert.assertSame(holder.getResult(3), dictWrapper);
      Assert.assertNull(holder.getResult(4));
      // Growth must apply to the delegate as well
      holder.ensureCapacity(NUM_SLOTS);
      holder.setValueForKey(NUM_SLOTS - 1, dictWrapper);
      Assert.assertSame(holder.getResult(NUM_SLOTS - 1), dictWrapper);
    }
  }

  @Test
  public void testInvalidId() {
    try (OffHeapUltraLogLogGroupByResultHolder holder =
        new OffHeapUltraLogLogGroupByResultHolder(12, INITIAL_CAPACITY, NUM_SLOTS)) {
      holder.touch(GroupKeyGenerator.INVALID_ID);
      holder.add(GroupKeyGenerator.INVALID_ID, 123L);
      holder.setValueForKey(GroupKeyGenerator.INVALID_ID, new Object());
      Assert.assertNull(holder.getResult(GroupKeyGenerator.INVALID_ID));
      // None of the INVALID_ID calls may have created state or a delegate
      Assert.assertNull(holder.getResult(0));
    }
  }

  @Test
  public void testCloseReleasesDirectMemoryAndIsIdempotent() {
    long baseline = PinotDataBuffer.getDirectBufferUsage();
    OffHeapUltraLogLogGroupByResultHolder holder =
        new OffHeapUltraLogLogGroupByResultHolder(12, INITIAL_CAPACITY, NUM_SLOTS);
    holder.add(0, 42L);
    Assert.assertTrue(PinotDataBuffer.getDirectBufferUsage() > baseline,
        "adding a value must allocate a direct-memory chunk");
    holder.close();
    Assert.assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline,
        "close must release all direct memory");
    holder.close();
    Assert.assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline);
  }

  @Test
  public void testOutOfRangePrecisionRejected() {
    // p sizes direct memory as 1 << p without going through UltraLogLog.create's own bound check, so the holder
    // must reject out-of-range p itself (p in [27, 30] would allocate up to 1GB per group; p > 30 would let
    // register indexes walk outside the slot after int-shift wrap)
    for (int p : new int[]{Integer.MIN_VALUE, -1, 0, 2, 27, 30, 32, 40, Integer.MAX_VALUE}) {
      Assert.assertThrows(IllegalArgumentException.class,
          () -> new OffHeapUltraLogLogGroupByResultHolder(p, INITIAL_CAPACITY, NUM_SLOTS));
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEnsureCapacityBeyondMaxThrows() {
    try (OffHeapUltraLogLogGroupByResultHolder holder =
        new OffHeapUltraLogLogGroupByResultHolder(12, INITIAL_CAPACITY, NUM_SLOTS)) {
      holder.ensureCapacity(NUM_SLOTS + 1);
    }
  }
}
