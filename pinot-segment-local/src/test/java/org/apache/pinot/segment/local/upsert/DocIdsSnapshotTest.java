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
package org.apache.pinot.segment.local.upsert;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class DocIdsSnapshotTest {
  @Test
  public void testCaptureAndLegacyReaders()
      throws Exception {
    ThreadSafeMutableRoaringBitmap bitmap = bitmap(1, 4, 6, 10, 15, 17, 18, 20);
    long before = System.currentTimeMillis();
    byte[] bytes = DocIdsSnapshot.capture(bitmap, new DocIdsSnapshot.Trigger("table__0__2__0", "123")).getBytes();
    long after = System.currentTimeMillis();
    DocIdsSnapshot snapshot = DocIdsSnapshot.fromBytes(bytes);
    assertEquals(snapshot.docIds(), bitmap.getMutableRoaringBitmap());
    assertEquals(snapshot.metadata().validDocIdsCrc32(), 3650129781L);
    assertTrue(snapshot.metadata().snapshotCapturedAtMs() >= before);
    assertTrue(snapshot.metadata().snapshotCapturedAtMs() <= after);
    assertEquals(snapshot.metadata().snapshotTriggerStartOffset(), "123");
    assertEquals(snapshot.metadata().snapshotTriggerSegmentName(), "table__0__2__0");
    // Both reader styles used by old servers and snapshot consumers ignore the trailer.
    assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes)).toMutableRoaringBitmap(), snapshot.docIds());
    MutableRoaringBitmap legacy = new MutableRoaringBitmap();
    legacy.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    assertEquals(legacy, snapshot.docIds());
  }

  @Test
  public void testChecksumTracksMembershipRatherThanCardinalityOrContainerLayout()
      throws Exception {
    assertNotEquals(DocIdsSnapshot.fromBytes(DocIdsSnapshot.capture(bitmap(1, 2), null).getBytes())
            .metadata().validDocIdsCrc32(),
        DocIdsSnapshot.fromBytes(DocIdsSnapshot.capture(bitmap(1, 3), null).getBytes()).metadata().validDocIdsCrc32());
    MutableRoaringBitmap array = new MutableRoaringBitmap();
    for (int i = 0; i < 1000; i++) {
      array.add(i);
    }
    MutableRoaringBitmap runs = array.clone();
    assertTrue(runs.runOptimize());
    assertNotEquals(array.serializedSizeInBytes(), runs.serializedSizeInBytes());
    assertEquals(DocIdsSnapshot.fromBytes(DocIdsSnapshot.capture(new ThreadSafeMutableRoaringBitmap(array), null)
            .getBytes()).metadata().validDocIdsCrc32(),
        DocIdsSnapshot.fromBytes(DocIdsSnapshot.capture(new ThreadSafeMutableRoaringBitmap(runs), null)
            .getBytes()).metadata().validDocIdsCrc32());
    assertEquals(DocIdsSnapshot.fromBytes(DocIdsSnapshot.capture(bitmap(), null).getBytes())
        .metadata().validDocIdsCrc32(), 0L);
  }

  @Test
  public void testLegacyAndUnreadableDiagnosticsDoNotPreventBitmapRecovery()
      throws Exception {
    ThreadSafeMutableRoaringBitmap bitmap = bitmap(1, 2, 3);
    byte[] legacy = bitmap.getBytes();
    assertNull(DocIdsSnapshot.fromBytes(legacy).metadata());
    byte[] bytes = DocIdsSnapshot.capture(bitmap, null).getBytes();
    byte[] truncated = Arrays.copyOf(bytes, bytes.length - 1);
    assertNull(DocIdsSnapshot.fromBytes(truncated).metadata());
    assertEquals(DocIdsSnapshot.fromBytes(truncated).docIds(), bitmap.getMutableRoaringBitmap());
    ByteBuffer.wrap(bytes).putInt(legacy.length + Integer.BYTES, 99);
    assertNull(DocIdsSnapshot.fromBytes(bytes).metadata());
    assertEquals(DocIdsSnapshot.fromBytes(bytes).docIds(), bitmap.getMutableRoaringBitmap());
  }

  @Test
  public void testAgeAndAbsentTrigger() {
    DocIdsSnapshot.Metadata metadata = new DocIdsSnapshot.Metadata(1L, 100L, null, null);
    assertEquals(metadata.toResponse(130L).get("snapshotAgeMs"), 30L);
    assertFalse(metadata.toResponse(99L).containsKey("snapshotAgeMs"));
    assertFalse(metadata.toResponse(130L).containsKey("snapshotTriggerStartOffset"));
    assertFalse(metadata.toResponse(130L).containsKey("snapshotTriggerSegmentName"));
  }

  private static ThreadSafeMutableRoaringBitmap bitmap(int... ids) {
    ThreadSafeMutableRoaringBitmap bitmap = new ThreadSafeMutableRoaringBitmap();
    for (int id : ids) {
      bitmap.add(id);
    }
    return bitmap;
  }
}
