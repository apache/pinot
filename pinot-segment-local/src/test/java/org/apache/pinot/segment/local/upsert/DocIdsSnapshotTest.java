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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.pinot.segment.local.upsert.DocIdsSnapshot.DocIdsType;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class DocIdsSnapshotTest {
  @Test
  public void testCaptureAndLegacyReaders()
      throws Exception {
    ThreadSafeMutableRoaringBitmap bitmap = bitmap(1, 4, 6, 10, 15, 17, 18, 20);
    long before = System.currentTimeMillis();
    byte[] bytes = fileBytes(bitmap, new DocIdsSnapshot.Trigger("table__0__2__0", "123"));
    long after = System.currentTimeMillis();
    DocIdsSnapshot snapshot = DocIdsSnapshot.fromBytes(bytes);
    assertEquals(snapshot.docIds(), bitmap.getMutableRoaringBitmap());
    assertEquals(snapshot.metadata().docIdsCrc(), 3650129781L);
    assertEquals(snapshot.metadata().docIdsType(), DocIdsType.VALID_DOC_IDS);
    assertTrue(snapshot.metadata().snapshotCapturedAtMs() >= before);
    assertTrue(snapshot.metadata().snapshotCapturedAtMs() <= after);
    assertEquals(snapshot.metadata().snapshotConsumedUpToOffset(), "123");
    assertEquals(snapshot.metadata().snapshotConsumingSegmentName(), "table__0__2__0");
    // Both reader styles used by old servers and snapshot consumers ignore the trailer.
    assertEquals(new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes)).toMutableRoaringBitmap(), snapshot.docIds());
    MutableRoaringBitmap legacy = new MutableRoaringBitmap();
    legacy.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    assertEquals(legacy, snapshot.docIds());
  }

  @Test
  public void testChecksumTracksMembershipRatherThanCardinalityOrContainerLayout()
      throws Exception {
    assertNotEquals(crc(bitmap(1, 2)), crc(bitmap(1, 3)));
    MutableRoaringBitmap array = new MutableRoaringBitmap();
    for (int i = 0; i < 1000; i++) {
      array.add(i);
    }
    MutableRoaringBitmap runs = array.clone();
    assertTrue(runs.runOptimize());
    assertNotEquals(array.serializedSizeInBytes(), runs.serializedSizeInBytes());
    assertEquals(crc(new ThreadSafeMutableRoaringBitmap(array)), crc(new ThreadSafeMutableRoaringBitmap(runs)));
    assertEquals(crc(bitmap()), 0L);
  }

  @Test
  public void testLegacyAndUnreadableDiagnosticsDoNotPreventBitmapRecovery()
      throws Exception {
    ThreadSafeMutableRoaringBitmap bitmap = bitmap(1, 2, 3);
    byte[] legacy = bitmap.getBytes();
    assertNull(DocIdsSnapshot.fromBytes(legacy).metadata());
    byte[] bytes = fileBytes(bitmap, null);
    byte[] truncated = Arrays.copyOf(bytes, bytes.length - 1);
    assertNull(DocIdsSnapshot.fromBytes(truncated).metadata());
    assertEquals(DocIdsSnapshot.fromBytes(truncated).docIds(), bitmap.getMutableRoaringBitmap());
    ByteBuffer.wrap(bytes).putInt(legacy.length + Integer.BYTES, 99);
    assertNull(DocIdsSnapshot.fromBytes(bytes).metadata());
    assertEquals(DocIdsSnapshot.fromBytes(bytes).docIds(), bitmap.getMutableRoaringBitmap());
  }

  @Test
  public void testUnknownDiagnosticsFieldsAreIgnored()
      throws Exception {
    byte[] bytes = fileBytes(bitmap(1, 2, 3), null);
    // A newer server may add a field to the trailer. Insert one before the closing brace.
    byte[] extra = ",\"futureField\":1}".getBytes(StandardCharsets.UTF_8);
    byte[] newer = Arrays.copyOf(bytes, bytes.length - 1 + extra.length);
    System.arraycopy(extra, 0, newer, bytes.length - 1, extra.length);
    assertNotNull(DocIdsSnapshot.fromBytes(newer).metadata());
    assertEquals(DocIdsSnapshot.fromBytes(newer).metadata(), DocIdsSnapshot.fromBytes(bytes).metadata());
  }

  @Test
  public void testAgeAndAbsentTrigger() {
    DocIdsSnapshot.Metadata metadata = new DocIdsSnapshot.Metadata(1L, DocIdsType.QUERYABLE_DOC_IDS, 100L, null, null);
    assertEquals(metadata.toResponse(130L).get("snapshotAgeMs"), 30L);
    assertEquals(metadata.toResponse(130L).get("docIdsType"), "QUERYABLE_DOC_IDS");
    assertFalse(metadata.toResponse(99L).containsKey("snapshotAgeMs"));
    assertFalse(metadata.toResponse(130L).containsKey("snapshotConsumedUpToOffset"));
    assertFalse(metadata.toResponse(130L).containsKey("snapshotConsumingSegmentName"));
  }

  private static long crc(ThreadSafeMutableRoaringBitmap bitmap)
      throws Exception {
    return DocIdsSnapshot.fromBytes(fileBytes(bitmap, null)).metadata().docIdsCrc();
  }

  private static byte[] fileBytes(ThreadSafeMutableRoaringBitmap bitmap, DocIdsSnapshot.Trigger trigger)
      throws Exception {
    ThreadSafeMutableRoaringBitmap.CardinalityAndBytes docIds = bitmap.getBytesAndCardinality();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DocIdsSnapshot.write(out, docIds.getBytes(), DocIdsSnapshot.metadata(docIds, DocIdsType.VALID_DOC_IDS, trigger));
    return out.toByteArray();
  }

  private static ThreadSafeMutableRoaringBitmap bitmap(int... ids) {
    ThreadSafeMutableRoaringBitmap bitmap = new ThreadSafeMutableRoaringBitmap();
    for (int id : ids) {
      bitmap.add(id);
    }
    return bitmap;
  }
}
