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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class OffHeapBytesGroupIdMapTest {
  private static final int NO_BOUND = Integer.MAX_VALUE;

  @Test
  public void testDifferentialAgainstReferenceMap() {
    Random random = new Random(42);
    Map<String, Integer> referenceMap = new HashMap<>();
    List<byte[]> generatedKeys = new ArrayList<>();
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(1024)) {
      for (int i = 0; i < 200_000; i++) {
        byte[] key;
        if (generatedKeys.isEmpty() || random.nextBoolean()) {
          key = new byte[random.nextInt(65)];
          random.nextBytes(key);
          generatedKeys.add(key);
        } else {
          // Re-submit a previously generated key to exercise the duplicate path
          key = generatedKeys.get(random.nextInt(generatedKeys.size()));
        }
        // ISO-8859-1 maps each byte to a unique char, so the String is a faithful reference key
        String referenceKey = new String(key, StandardCharsets.ISO_8859_1);
        int expectedId = referenceMap.computeIfAbsent(referenceKey, k -> referenceMap.size());
        assertEquals(map.getGroupId(key, NO_BOUND), expectedId);
      }
      assertEquals(map.size(), referenceMap.size());

      // getKey/readKey/getKeyLength round-trip for every 1000th id
      Map<Integer, String> idToKey = new HashMap<>();
      referenceMap.forEach((key, id) -> idToKey.put(id, key));
      for (int groupId = 0; groupId < map.size(); groupId += 1000) {
        byte[] expectedKey = idToKey.get(groupId).getBytes(StandardCharsets.ISO_8859_1);
        assertEquals(map.getKeyLength(groupId), expectedKey.length);
        assertEquals(map.getKey(groupId), expectedKey);
        int destOffset = 3;
        byte[] dest = new byte[expectedKey.length + destOffset + 4];
        map.readKey(groupId, dest, destOffset);
        assertEquals(Arrays.copyOfRange(dest, destOffset, destOffset + expectedKey.length), expectedKey);
      }
    }
  }

  @Test
  public void testEmptyKey() {
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(16)) {
      byte[] emptyKey = new byte[0];
      assertEquals(map.getGroupId(emptyKey, NO_BOUND), 0);
      assertEquals(map.getGroupId(new byte[0], NO_BOUND), 0);
      assertEquals(map.size(), 1);
      assertEquals(map.getKeyLength(0), 0);
      assertEquals(map.getKey(0), emptyKey);
      // The empty key is distinct from a single 0x00 byte
      assertEquals(map.getGroupId(new byte[1], NO_BOUND), 1);
      assertEquals(map.size(), 2);
    }
  }

  @Test
  public void testPrefixKeys() {
    byte[] fullKey = "abcdefgh".getBytes(StandardCharsets.UTF_8);
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(16)) {
      for (int length = 0; length <= fullKey.length; length++) {
        assertEquals(map.getGroupId(fullKey, 0, length, NO_BOUND), length);
      }
      assertEquals(map.size(), fullKey.length + 1);
      for (int length = 0; length <= fullKey.length; length++) {
        assertEquals(map.getGroupId(fullKey, 0, length, NO_BOUND), length);
        assertEquals(map.getKey(length), Arrays.copyOfRange(fullKey, 0, length));
      }
    }
  }

  @Test
  public void testAllZeroKeys() {
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(16)) {
      // All-zero keys live in the payload and must not be confused with empty directory slots
      for (int length = 1; length <= 16; length++) {
        assertEquals(map.getGroupId(new byte[length], NO_BOUND), length - 1);
      }
      assertEquals(map.size(), 16);
      for (int length = 1; length <= 16; length++) {
        assertEquals(map.getGroupId(new byte[length], NO_BOUND), length - 1);
        assertEquals(map.getKey(length - 1), new byte[length]);
      }
    }
  }

  @Test
  public void testGroupIdUpperBound() {
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(16)) {
      int upperBound = 10;
      for (int i = 0; i < upperBound; i++) {
        assertEquals(map.getGroupId(key(i), upperBound), i);
      }
      assertEquals(map.size(), upperBound);
      // Existing keys always resolve, even at the cap
      for (int i = 0; i < upperBound; i++) {
        assertEquals(map.getGroupId(key(i), upperBound), i);
      }
      // New keys are rejected at the cap, and the size stays frozen
      assertEquals(map.getGroupId(key(10), upperBound), GroupKeyGenerator.INVALID_ID);
      assertEquals(map.getGroupId(key(11), upperBound), GroupKeyGenerator.INVALID_ID);
      assertEquals(map.getGroupId(key(10), upperBound), GroupKeyGenerator.INVALID_ID);
      assertEquals(map.size(), upperBound);
      // A rejected key was not inserted: raising the bound assigns it the next dense id
      assertEquals(map.getGroupId(key(10), upperBound + 1), upperBound);
      assertEquals(map.size(), upperBound + 1);
    }
  }

  @Test
  public void testResizePreservesIds() {
    int numKeys = 100_000;
    // Start with the minimum directory (1024 slots) to force many directory resizes
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(0)) {
      for (int i = 0; i < numKeys; i++) {
        assertEquals(map.getGroupId(key(i), NO_BOUND), i);
      }
      assertEquals(map.size(), numKeys);
      for (int i = 0; i < numKeys; i += 1000) {
        assertEquals(map.getGroupId(key(i), NO_BOUND), i);
        assertEquals(map.getKey(i), key(i));
      }
      assertEquals(map.size(), numKeys);
    }
  }

  @Test
  public void testOversizedKey() {
    Random random = new Random(42);
    byte[] oversizedKey = new byte[300_000];
    random.nextBytes(oversizedKey);
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(16)) {
      for (int i = 0; i < 100; i++) {
        assertEquals(map.getGroupId(key(i), NO_BOUND), i);
      }
      int oversizedId = map.getGroupId(oversizedKey, NO_BOUND);
      assertEquals(oversizedId, 100);
      // Normal inserts continue after the oversized record
      for (int i = 100; i < 200; i++) {
        assertEquals(map.getGroupId(key(i), NO_BOUND), i + 1);
      }
      assertEquals(map.getGroupId(oversizedKey, NO_BOUND), oversizedId);
      assertEquals(map.getKeyLength(oversizedId), oversizedKey.length);
      assertEquals(map.getKey(oversizedId), oversizedKey);
      // The oversized record sits at offset 0 of its dedicated chunk
      assertEquals(map.getPayloadGlobalOffset(oversizedId) % OffHeapBytesGroupIdMap.CHUNK_SIZE, 0);
      verifyRecordOffsets(map);
    }
  }

  @Test
  public void testChunkBoundaryKey() {
    // A record of exactly CHUNK_SIZE bytes (16-byte header + key) exactly fills one normal chunk
    byte[] boundaryKey = new byte[OffHeapBytesGroupIdMap.CHUNK_SIZE - 16];
    new Random(42).nextBytes(boundaryKey);
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(16)) {
      assertEquals(map.getGroupId(boundaryKey, NO_BOUND), 0);
      assertEquals(map.getGroupId(boundaryKey, NO_BOUND), 0);
      assertEquals(map.getKeyLength(0), boundaryKey.length);
      assertEquals(map.getKey(0), boundaryKey);
      assertEquals(map.getPayloadGlobalOffset(0) % OffHeapBytesGroupIdMap.CHUNK_SIZE, 0);
      // The next record starts a new chunk
      assertEquals(map.getGroupId(key(1), NO_BOUND), 1);
      assertEquals(map.getKey(1), key(1));
      verifyRecordOffsets(map);
    }
  }

  @Test
  public void testOffsetLengthVariant() {
    Random random = new Random(42);
    byte[] outerArray = new byte[64];
    random.nextBytes(outerArray);
    int offset = 13;
    int length = 21;
    byte[] slice = Arrays.copyOfRange(outerArray, offset, offset + length);
    try (OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(16)) {
      int groupId = map.getGroupId(outerArray, offset, length, NO_BOUND);
      assertEquals(groupId, 0);
      // The same bytes submitted as a standalone array resolve to the same id
      assertEquals(map.getGroupId(slice, NO_BOUND), groupId);
      assertEquals(map.size(), 1);
      assertEquals(map.getKey(groupId), slice);
    }
  }

  /// Exercises the wrapper-based fallback arms (directory probe, matchRecordSlow, id-index reads, resize,
  /// zero-fill) that normally only run for buffers beyond the 2GB view limit.
  @Test
  public void testDifferentialWithoutViews() {
    OffHeapGroupByUtils.setViewSizeLimitBytes(0);
    try {
      testDifferentialAgainstReferenceMap();
      testResizePreservesIds();
      testOversizedKey();
    } finally {
      OffHeapGroupByUtils.setViewSizeLimitBytes(Integer.MAX_VALUE);
    }
  }

  @Test
  public void testCloseIsIdempotent() {
    OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(16);
    assertEquals(map.getGroupId(key(0), NO_BOUND), 0);
    map.close();
    // Second close is a no-op
    map.close();
  }

  @Test
  public void testNoDirectMemoryLeak() {
    long baseline = PinotDataBuffer.getDirectBufferUsage();
    OffHeapBytesGroupIdMap map = new OffHeapBytesGroupIdMap(16);
    try {
      Random random = new Random(42);
      // Enough keys to force directory resizes, id index growth and multiple payload chunks
      for (int i = 0; i < 50_000; i++) {
        byte[] key = new byte[random.nextInt(65)];
        random.nextBytes(key);
        map.getGroupId(key, NO_BOUND);
      }
      byte[] oversizedKey = new byte[300_000];
      random.nextBytes(oversizedKey);
      map.getGroupId(oversizedKey, NO_BOUND);
      assertTrue(map.getOffHeapMemoryBytes() > 0);
      assertTrue(PinotDataBuffer.getDirectBufferUsage() > baseline);
    } finally {
      map.close();
    }
    assertEquals(PinotDataBuffer.getDirectBufferUsage(), baseline);
  }

  /// Verifies the global offset encoding invariants: every record start offset within a chunk is smaller than
  /// CHUNK_SIZE, normal records fit entirely within their chunk, and oversized records start at offset 0.
  private static void verifyRecordOffsets(OffHeapBytesGroupIdMap map) {
    for (int groupId = 0; groupId < map.size(); groupId++) {
      long offsetInChunk = map.getPayloadGlobalOffset(groupId) % OffHeapBytesGroupIdMap.CHUNK_SIZE;
      assertTrue(offsetInChunk < OffHeapBytesGroupIdMap.CHUNK_SIZE);
      long recordSize = 16L + map.getKeyLength(groupId);
      if (recordSize <= OffHeapBytesGroupIdMap.CHUNK_SIZE) {
        assertTrue(offsetInChunk + recordSize <= OffHeapBytesGroupIdMap.CHUNK_SIZE);
      } else {
        assertEquals(offsetInChunk, 0);
      }
    }
  }

  private static byte[] key(int i) {
    return ("key-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
