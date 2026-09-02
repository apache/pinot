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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.io.IOException;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Pins the forward-index chunk sizing of [MutableKeyColumn]. The chunk size governs both the
/// off-heap a single OPEN_STRUCT key reserves merely by being observed (the first chunk is
/// allocated eagerly) and how many buffers a key that spans the segment allocates, which drives
/// allocation churn on the consuming path.
///
/// The dictionaries [MutableKeyColumn] builds are on-heap, so every byte the memory manager reports
/// here belongs to the forward index.
public class MutableKeyColumnChunkSizingTest {
  /// 512000 / MAX_CHUNKS_PER_KEY (256) = 2000 rows per chunk: above the 1000-row floor and below
  /// the 4000-row ceiling, so this capacity exercises the derived size rather than either bound.
  private static final int CAPACITY = 512_000;
  private static final int ROWS_PER_CHUNK_AT_CAPACITY = 2000;
  /// Well past MAX_ROWS_PER_CHUNK * MAX_CHUNKS_PER_KEY (1_024_000), so the ceiling binds.
  private static final int LARGE_CAPACITY = 5_000_000;
  private static final int MAX_ROWS_PER_CHUNK = 4000;
  private static final int BYTES_PER_DICT_ID = Integer.BYTES;

  private PinotDataBufferMemoryManager _memMgr;

  @BeforeMethod
  public void setUp() {
    _memMgr = new DirectMemoryManager(MutableKeyColumnChunkSizingTest.class.getName());
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    _memMgr.close();
  }

  private MutableKeyColumn newColumn(int capacity) {
    Object defaultNullValue = FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, DataType.LONG, null);
    return new MutableKeyColumn("k", DataType.LONG, defaultNullValue, _memMgr, capacity);
  }

  @Test
  public void testChunkSizeDerivedFromCapacity()
      throws IOException {
    try (MutableKeyColumn col = newColumn(CAPACITY)) {
      col.setValue(0, 1L);
      // One chunk of 2000 rows, not the old hardcoded 1000.
      assertEquals(_memMgr.getTotalAllocatedBytes(), (long) ROWS_PER_CHUNK_AT_CAPACITY * BYTES_PER_DICT_ID);
    }
  }

  @Test
  public void testChunkCountBoundedAcrossFullSegment()
      throws IOException {
    try (MutableKeyColumn col = newColumn(CAPACITY)) {
      col.setValue(CAPACITY - 1, 1L);
      long totalBytes = _memMgr.getTotalAllocatedBytes();
      long chunkBytes = (long) ROWS_PER_CHUNK_AT_CAPACITY * BYTES_PER_DICT_ID;
      // 512000 / 2000 = 256 chunks cover every docId, plus the chunk the constructor allocated
      // eagerly: FixedByteSVMutableForwardIndex#addBufferIfNeeded rounds its buffer count up
      // unconditionally, so an exactly-divisible growth request allocates one spare chunk.
      long expectedChunks = CAPACITY / ROWS_PER_CHUNK_AT_CAPACITY + 1;
      assertEquals(totalBytes / chunkBytes, expectedChunks,
          "Expected the chunk count to stay near MAX_CHUNKS_PER_KEY at this capacity");
      // Total footprint is nearly unchanged by chunk sizing: the flat 1000-row layout allocated
      // 513 * 4000 = 2052000 bytes for the same write. Allow one chunk of slack in either direction.
      assertTrue(Math.abs(totalBytes - 2_052_000L) <= chunkBytes,
          "Total allocation should be within one chunk of the previous layout, was " + totalBytes);
    }
  }

  @Test
  public void testSmallCapacityKeepsFloor()
      throws IOException {
    // 100000 / 256 = 390, below the 1000-row floor, so the floor applies.
    try (MutableKeyColumn col = newColumn(100_000)) {
      col.setValue(0, 1L);
      assertEquals(_memMgr.getTotalAllocatedBytes(), 1000L * BYTES_PER_DICT_ID);
    }
  }

  /// The floor a key pays just for being observed must not scale with segment capacity: the
  /// OPEN_STRUCT key space is user-controlled and mutable mode retains every observed key, so a
  /// segment that meets a few thousand rare keys would otherwise reserve hundreds of MB it never
  /// writes into.
  @Test
  public void testRareKeyFootprintCappedAtLargeCapacity()
      throws IOException {
    try (MutableKeyColumn col = newColumn(LARGE_CAPACITY)) {
      col.setValue(0, 1L);
      // 16000 bytes, not the 78124 (5000000 / 256 * 4) the uncapped derivation would reserve.
      assertEquals(_memMgr.getTotalAllocatedBytes(), (long) MAX_ROWS_PER_CHUNK * BYTES_PER_DICT_ID);
    }
  }

  /// The other side of the ceiling: a key that does span a large segment pays for it in chunk
  /// count. This pins what that costs, so the trade-off is visible if the ceiling ever moves.
  @Test
  public void testChunkCountAtLargeCapacityStaysWithinBudget()
      throws IOException {
    try (MutableKeyColumn col = newColumn(LARGE_CAPACITY)) {
      col.setValue(LARGE_CAPACITY - 1, 1L);
      long chunkBytes = (long) MAX_ROWS_PER_CHUNK * BYTES_PER_DICT_ID;
      // 5000000 / 4000 = 1250 chunks to cover every docId, plus the same eagerly-allocated spare
      // as above; 5000 chunks under the flat 1000-row size this sizing replaced.
      long expectedChunks = LARGE_CAPACITY / MAX_ROWS_PER_CHUNK + 1;
      assertEquals(_memMgr.getTotalAllocatedBytes() / chunkBytes, expectedChunks);
    }
  }

  @Test
  public void testValuesReadBackAcrossChunkBoundaries()
      throws IOException {
    try (MutableKeyColumn col = newColumn(CAPACITY)) {
      int[] docIds = {0, 1999, 2000, 2001, 5999, 6000, CAPACITY - 1};
      for (int i = 0; i < docIds.length; i++) {
        col.setValue(docIds[i], (long) (i + 100));
      }
      for (int i = 0; i < docIds.length; i++) {
        assertEquals(col.getValue(docIds[i]), (long) (i + 100),
            "Wrong value at docId " + docIds[i]);
      }
      // A docId never written is absent, not the first dictionary entry.
      assertNull(col.getValue(3000));
    }
  }

  /// Values must read back correctly when the ceiling, rather than the capacity-derived size, sets
  /// the chunk boundaries.
  @Test
  public void testValuesReadBackAcrossCeilingChunkBoundaries()
      throws IOException {
    try (MutableKeyColumn col = newColumn(LARGE_CAPACITY)) {
      int[] docIds = {0, MAX_ROWS_PER_CHUNK - 1, MAX_ROWS_PER_CHUNK, MAX_ROWS_PER_CHUNK + 1,
          LARGE_CAPACITY - 1};
      for (int i = 0; i < docIds.length; i++) {
        col.setValue(docIds[i], (long) (i + 100));
      }
      for (int i = 0; i < docIds.length; i++) {
        assertEquals(col.getValue(docIds[i]), (long) (i + 100), "Wrong value at docId " + docIds[i]);
      }
      assertNull(col.getValue(MAX_ROWS_PER_CHUNK + 2));
    }
  }
}
