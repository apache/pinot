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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.core.query.aggregation.groupby.DictionaryBasedGroupKeyGenerator.IntGroupIdMap;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class OffHeapIntGroupIdMapTest {
  private static final int GROUP_ID_UPPER_BOUND = Integer.MAX_VALUE;

  @Test
  public void testDifferentialAgainstOnHeapIntGroupIdMap() {
    try (OffHeapIntGroupIdMap offHeapMap = new OffHeapIntGroupIdMap(0)) {
      IntGroupIdMap onHeapMap = new IntGroupIdMap();
      Random random = new Random(42);
      for (int i = 0; i < 200_000; i++) {
        // ~50% duplicates; keys include 0 and Integer.MAX_VALUE
        int rawKey = random.nextBoolean() ? random.nextInt(50_000) : switch (random.nextInt(3)) {
          case 0 -> 0;
          case 1 -> Integer.MAX_VALUE;
          default -> random.nextInt(Integer.MAX_VALUE);
        };
        assertEquals(offHeapMap.getGroupId(rawKey, GROUP_ID_UPPER_BOUND),
            onHeapMap.getGroupId(rawKey, GROUP_ID_UPPER_BOUND), "Mismatch for key: " + rawKey + " at op " + i);
      }
      assertEquals(offHeapMap.size(), onHeapMap.size());

      // Iterator parity as sets of (rawKey -> groupId)
      Map<Integer, Integer> offHeapEntries = new HashMap<>();
      Iterator<OffHeapIntGroupIdMap.Entry> offHeapIterator = offHeapMap.iterator();
      while (offHeapIterator.hasNext()) {
        OffHeapIntGroupIdMap.Entry entry = offHeapIterator.next();
        offHeapEntries.put(entry._rawKey, entry._groupId);
      }
      Map<Integer, Integer> onHeapEntries = new HashMap<>();
      Iterator<IntGroupIdMap.Entry> onHeapIterator = onHeapMap.iterator();
      while (onHeapIterator.hasNext()) {
        IntGroupIdMap.Entry entry = onHeapIterator.next();
        onHeapEntries.put(entry._rawKey, entry._groupId);
      }
      assertEquals(offHeapEntries, onHeapEntries);
    }
  }

  @Test
  public void testCapSemantics() {
    try (OffHeapIntGroupIdMap map = new OffHeapIntGroupIdMap(0)) {
      Int2IntOpenHashMap reference = new Int2IntOpenHashMap();
      for (int key = 0; key < 150; key++) {
        int groupId = map.getGroupId(key * 31, 100);
        if (key < 100) {
          assertEquals(groupId, key);
          reference.put(key * 31, groupId);
        } else {
          assertEquals(groupId, OffHeapIntGroupIdMap.INVALID_ID);
        }
      }
      assertEquals(map.size(), 100);
      // Existing keys still resolve at cap; rejected keys were not inserted
      for (int key = 0; key < 100; key++) {
        assertEquals(map.getGroupId(key * 31, 100), reference.get(key * 31));
      }
      assertEquals(map.getGroupId(149 * 31, 100), OffHeapIntGroupIdMap.INVALID_ID);
      // Raising the bound assigns the next dense id
      assertEquals(map.getGroupId(149 * 31, 101), 100);
    }
  }

  @Test
  public void testGrowthAcrossResizes() {
    try (OffHeapIntGroupIdMap map = new OffHeapIntGroupIdMap(0)) {
      for (int key = 0; key < 100_000; key++) {
        assertEquals(map.getGroupId(key, GROUP_ID_UPPER_BOUND), key);
      }
      // All ids stable after many resizes
      for (int key = 0; key < 100_000; key++) {
        assertEquals(map.getGroupId(key, GROUP_ID_UPPER_BOUND), key);
      }
      // 100K entries at load factor 0.5 over 8-byte slots: 262144 slots * 8 bytes
      assertEquals(map.getOffHeapMemoryBytes(), 262_144L * 8);
    }
  }

  @Test
  public void testMinusOneKeyOutOfBand() {
    // -1 first
    try (OffHeapIntGroupIdMap map = new OffHeapIntGroupIdMap(0)) {
      assertEquals(map.getGroupId(-1, GROUP_ID_UPPER_BOUND), 0);
      assertEquals(map.getGroupId(7, GROUP_ID_UPPER_BOUND), 1);
      assertEquals(map.getGroupId(-1, GROUP_ID_UPPER_BOUND), 0);
      assertEquals(map.size(), 2);
      Map<Integer, Integer> entries = new HashMap<>();
      Iterator<OffHeapIntGroupIdMap.Entry> iterator = map.iterator();
      while (iterator.hasNext()) {
        OffHeapIntGroupIdMap.Entry entry = iterator.next();
        entries.put(entry._rawKey, entry._groupId);
      }
      assertEquals(entries, Map.of(-1, 0, 7, 1));
    }
    // -1 mid-stream
    try (OffHeapIntGroupIdMap map = new OffHeapIntGroupIdMap(0)) {
      assertEquals(map.getGroupId(10, GROUP_ID_UPPER_BOUND), 0);
      assertEquals(map.getGroupId(-1, GROUP_ID_UPPER_BOUND), 1);
      assertEquals(map.getGroupId(20, GROUP_ID_UPPER_BOUND), 2);
      assertEquals(map.size(), 3);
    }
    // -1 rejected at the cap while existing keys still resolve
    try (OffHeapIntGroupIdMap map = new OffHeapIntGroupIdMap(0)) {
      assertEquals(map.getGroupId(10, 1), 0);
      assertEquals(map.getGroupId(-1, 1), OffHeapIntGroupIdMap.INVALID_ID);
      assertEquals(map.getGroupId(10, 1), 0);
      assertEquals(map.size(), 1);
      // Raising the bound assigns the next dense id
      assertEquals(map.getGroupId(-1, 2), 1);
    }
  }

  /// Exercises the wrapper-based fallback arms (probe, expand, zero-fill) that normally only run for buffers
  /// beyond the 2GB view limit.
  @Test
  public void testDifferentialWithoutViews() {
    OffHeapGroupByUtils.setViewSizeLimitBytes(0);
    try {
      testDifferentialAgainstOnHeapIntGroupIdMap();
      testGrowthAcrossResizes();
      testMinusOneKeyOutOfBand();
    } finally {
      OffHeapGroupByUtils.setViewSizeLimitBytes(Integer.MAX_VALUE);
    }
  }

  @Test
  public void testCloseTwiceAndNoLeak() {
    long baseline = PinotDataBuffer.getDirectBufferUsage();
    OffHeapIntGroupIdMap map = new OffHeapIntGroupIdMap(0);
    for (int key = 0; key < 10_000; key++) {
      map.getGroupId(key, GROUP_ID_UPPER_BOUND);
    }
    assertTrue(PinotDataBuffer.getDirectBufferUsage() > baseline);
    map.close();
    map.close();
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline);
  }
}
