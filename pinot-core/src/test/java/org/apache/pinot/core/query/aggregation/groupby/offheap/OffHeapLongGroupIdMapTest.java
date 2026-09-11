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

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Unit test for [OffHeapLongGroupIdMap], using [Long2IntOpenHashMap] as the on-heap reference
/// implementation of the same dense group-id assignment contract.
public class OffHeapLongGroupIdMapTest {
  private static final int INVALID_ID = OffHeapLongGroupIdMap.INVALID_ID;

  private static Long2IntOpenHashMap newReference() {
    Long2IntOpenHashMap reference = new Long2IntOpenHashMap();
    reference.defaultReturnValue(INVALID_ID);
    return reference;
  }

  /// Reference implementation of the dense-id contract: existing keys always resolve; new keys get id = size() when
  /// under the upper bound, otherwise INVALID_ID without insertion.
  private static int referenceGetGroupId(Long2IntOpenHashMap reference, long rawKey, int groupIdUpperBound) {
    int groupId = reference.get(rawKey);
    if (groupId != INVALID_ID) {
      return groupId;
    }
    if (reference.size() < groupIdUpperBound) {
      groupId = reference.size();
      reference.put(rawKey, groupId);
      return groupId;
    }
    return INVALID_ID;
  }

  private static void verifyIteratorMatchesReference(OffHeapLongGroupIdMap map, Long2IntOpenHashMap reference) {
    Map<Long, Integer> actual = new HashMap<>();
    long lastRawKey = Long.MIN_VALUE;
    Iterator<OffHeapLongGroupIdMap.Entry> iterator = map.iterator();
    while (iterator.hasNext()) {
      OffHeapLongGroupIdMap.Entry entry = iterator.next();
      // The Entry is a reused flyweight, so copy the values out
      assertNull(actual.put(entry._rawKey, entry._groupId), "Iterator yielded duplicate key: " + entry._rawKey);
      lastRawKey = entry._rawKey;
    }
    assertEquals(actual.size(), reference.size(), "Iterator must yield exactly size() entries");
    for (Map.Entry<Long, Integer> entry : actual.entrySet()) {
      assertEquals(entry.getValue().intValue(), reference.get(entry.getKey().longValue()),
          "Mismatch for key: " + entry.getKey());
    }
    if (reference.containsKey(0L)) {
      assertEquals(lastRawKey, 0L, "Zero-key entry must be yielded last");
    }
  }

  @Test
  public void testDifferentialAgainstReference() {
    Random random = new Random(42);
    long[] keys = new long[200_000];
    for (int i = 0; i < keys.length; i++) {
      if (i > 0 && random.nextBoolean()) {
        // Duplicate of an earlier key
        keys[i] = keys[random.nextInt(i)];
      } else {
        keys[i] = random.nextLong();
      }
    }
    // Force the zero key into the stream (random longs will essentially never produce it)
    keys[1_000] = 0;
    keys[2_000] = 0;
    Long2IntOpenHashMap reference = newReference();
    try (OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(1024)) {
      for (long key : keys) {
        assertEquals(map.getGroupId(key, Integer.MAX_VALUE), referenceGetGroupId(reference, key, Integer.MAX_VALUE),
            "Mismatch for key: " + key);
      }
      assertEquals(map.size(), reference.size());
      verifyIteratorMatchesReference(map, reference);
    }
  }

  @Test
  public void testZeroKeyFirst() {
    try (OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(16)) {
      assertEquals(map.getGroupId(0, 10), 0);
      assertEquals(map.getGroupId(0, 10), 0);
      assertEquals(map.getGroupId(42, 10), 1);
      assertEquals(map.getGroupId(0, 10), 0);
      assertEquals(map.size(), 2);
    }
  }

  @Test
  public void testZeroKeyMid() {
    try (OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(16)) {
      for (int i = 0; i < 10; i++) {
        assertEquals(map.getGroupId(i + 1, 100), i);
      }
      assertEquals(map.getGroupId(0, 100), 10);
      assertEquals(map.getGroupId(0, 100), 10);
      assertEquals(map.getGroupId(11, 100), 11);
      assertEquals(map.size(), 12);
    }
  }

  @Test
  public void testZeroKeyAtCap() {
    try (OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(16)) {
      for (int i = 0; i < 5; i++) {
        assertEquals(map.getGroupId(i + 1, 5), i);
      }
      // At the cap: the zero key must be rejected and not inserted
      assertEquals(map.getGroupId(0, 5), INVALID_ID);
      assertEquals(map.size(), 5);
      // With a larger bound it gets the next dense id, proving the rejection did not insert it
      assertEquals(map.getGroupId(0, 6), 5);
      // Present keys always resolve, even when size() >= upper bound
      assertEquals(map.getGroupId(0, 5), 5);
      assertEquals(map.size(), 6);
    }
  }

  @Test
  public void testNegativeKeys() {
    long[] keys = {Long.MIN_VALUE, -1, Long.MAX_VALUE, -123_456_789L};
    Long2IntOpenHashMap reference = newReference();
    try (OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(16)) {
      for (int i = 0; i < keys.length; i++) {
        assertEquals(map.getGroupId(keys[i], 100), i);
        referenceGetGroupId(reference, keys[i], 100);
      }
      for (int i = 0; i < keys.length; i++) {
        assertEquals(map.getGroupId(keys[i], 100), i);
      }
      assertEquals(map.size(), keys.length);
      verifyIteratorMatchesReference(map, reference);
    }
  }

  @Test
  public void testCapSemantics() {
    try (OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(256)) {
      for (int i = 0; i < 150; i++) {
        assertEquals(map.getGroupId(i + 1, 100), i < 100 ? i : INVALID_ID);
      }
      assertEquals(map.size(), 100);
      // Existing keys still resolve at the cap; rejected keys were never inserted
      for (int i = 0; i < 150; i++) {
        assertEquals(map.getGroupId(i + 1, 100), i < 100 ? i : INVALID_ID);
      }
      assertEquals(map.size(), 100);
      Iterator<OffHeapLongGroupIdMap.Entry> iterator = map.iterator();
      int numEntries = 0;
      while (iterator.hasNext()) {
        OffHeapLongGroupIdMap.Entry entry = iterator.next();
        assertEquals(entry._groupId, (int) entry._rawKey - 1);
        numEntries++;
      }
      assertEquals(numEntries, 100);
    }
  }

  @Test
  public void testUpperBoundSmallerThanCurrentSize() {
    try (OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(64)) {
      for (int i = 0; i < 50; i++) {
        assertEquals(map.getGroupId(i + 1, 1000), i);
      }
      assertEquals(map.getGroupId(0, 1000), 50);
      // An upper bound smaller than the current size never breaks existing lookups
      for (int i = 0; i < 50; i++) {
        assertEquals(map.getGroupId(i + 1, 1), i);
      }
      assertEquals(map.getGroupId(0, 1), 50);
      // But it rejects new keys
      assertEquals(map.getGroupId(9999, 1), INVALID_ID);
      assertEquals(map.size(), 51);
    }
  }

  @Test
  public void testGrowthAcrossMultipleResizes() {
    int numKeys = 100_000;
    try (OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(10)) {
      // Initial capacity is max(512, ceilPow2(expected * 2)) = 512 slots of 16 bytes
      assertEquals(map.getOffHeapMemoryBytes(), 512L * 16);
      for (int i = 0; i < numKeys; i++) {
        assertEquals(map.getGroupId(i + 1, Integer.MAX_VALUE), i);
      }
      assertEquals(map.size(), numKeys);
      // Load factor 0.5: the smallest power-of-two capacity with capacity / 2 >= 100_000 is 262144
      assertEquals(map.getOffHeapMemoryBytes(), 262_144L * 16);
      // Ids are untouched by resize: every key still resolves to its original id
      for (int i = 0; i < numKeys; i++) {
        assertEquals(map.getGroupId(i + 1, Integer.MAX_VALUE), i);
      }
      assertEquals(map.size(), numKeys);
    }
  }

  /// Exercises the wrapper-based fallback arms (probe, expand, zero-fill) that normally only run for buffers
  /// beyond the 2GB view limit.
  @Test
  public void testDifferentialWithoutViews() {
    OffHeapGroupByUtils.setViewSizeLimitBytes(0);
    try {
      testDifferentialAgainstReference();
      testGrowthAcrossMultipleResizes();
    } finally {
      OffHeapGroupByUtils.setViewSizeLimitBytes(Integer.MAX_VALUE);
    }
  }

  @Test
  public void testCloseTwiceIsSafe() {
    OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(16);
    assertEquals(map.getGroupId(123, 10), 0);
    map.close();
    map.close();
  }

  @Test
  public void testNoDirectMemoryLeak() {
    long baseline = PinotDataBuffer.getDirectBufferUsage();
    OffHeapLongGroupIdMap map = new OffHeapLongGroupIdMap(10);
    try {
      // Force multiple resizes; each resize must close the old buffer eagerly
      for (int i = 0; i < 10_000; i++) {
        map.getGroupId(i + 1, Integer.MAX_VALUE);
      }
      assertTrue(PinotDataBuffer.getDirectBufferUsage() > baseline);
      assertEquals(PinotDataBuffer.getDirectBufferUsage() - baseline, map.getOffHeapMemoryBytes(),
          "Only the current hash table buffer should be alive after resizes");
    } finally {
      map.close();
    }
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline, "Direct buffer usage must return to baseline");
  }
}
