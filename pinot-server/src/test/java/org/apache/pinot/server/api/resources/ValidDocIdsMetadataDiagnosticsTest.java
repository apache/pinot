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
package org.apache.pinot.server.api.resources;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


/// Verifies that diagnostic observations retain membership, file identity and consumer provenance.
public class ValidDocIdsMetadataDiagnosticsTest {
  private static final String SEGMENT_NAME = "table__0__0__20260923T0000Z";

  @Test
  public void testCrcReflectsMembershipNotRepresentation() {
    MutableRoaringBitmap bitmap = MutableRoaringBitmap.bitmapOf(1, 2);
    MutableRoaringBitmap different = MutableRoaringBitmap.bitmapOf(1, 3);
    assertEquals(bitmap.getCardinality(), different.getCardinality());
    assertNotEquals(ValidDocIdsMetadataDiagnostics.computeCrc32(bitmap),
        ValidDocIdsMetadataDiagnostics.computeCrc32(different));
    assertEquals(ValidDocIdsMetadataDiagnostics.computeCrc32(new MutableRoaringBitmap()), 0L);
    // The wire algorithm is fixed: CRC32 over 00 00 00 01 00 00 00 02.
    assertEquals(ValidDocIdsMetadataDiagnostics.computeCrc32(bitmap), 3058472949L);

    bitmap.add(0L, 10000L);
    MutableRoaringBitmap optimized = bitmap.clone();
    assertTrue(optimized.runOptimize());
    assertNotEquals(bitmap.serializedSizeInBytes(), optimized.serializedSizeInBytes());
    assertEquals(ValidDocIdsMetadataDiagnostics.computeCrc32(bitmap),
        ValidDocIdsMetadataDiagnostics.computeCrc32(optimized));
  }

  @Test
  public void testCaptureBracketsOffsetsAndKeepsOverlappingConsumers() {
    RealtimeSegmentDataManager first = consumer("table__0__1__20260923T0000Z", 100, 101);
    RealtimeSegmentDataManager second = consumer("table__0__2__20260923T0000Z", 200, 200);
    RealtimeSegmentDataManager otherPartition = consumer("table__1__1__20260923T0000Z", 900, 901);
    long before = System.currentTimeMillis();
    ValidDocIdsMetadataDiagnostics diagnostics = new ValidDocIdsMetadataDiagnostics(segment(SEGMENT_NAME, null),
        "IN_MEMORY", Map.of(0, List.of(first, second), 1, List.of(otherPartition)));
    Map<String, Object> result = diagnostics.finish(MutableRoaringBitmap.bitmapOf(1, 2));
    long after = System.currentTimeMillis();
    assertTrue((long) result.get("captureStartTimeMs") >= before);
    assertTrue((long) result.get("captureEndTimeMs") <= after);
    assertTrue((long) result.get("captureEndTimeMs") >= (long) result.get("captureStartTimeMs"));
    assertEquals(result.get("consumingSegmentOffsetsBefore"),
        Map.of(first.getSegmentName(), "100", second.getSegmentName(), "200"));
    assertEquals(result.get("consumingSegmentOffsetsAfter"),
        Map.of(first.getSegmentName(), "101", second.getSegmentName(), "200"));
    assertFalse(result.containsKey("snapshotFileAgeMs"));
  }

  @Test
  public void testUnavailableOffsetsAreNotReportedAsZero() {
    Map<String, Object> noConsumer = new ValidDocIdsMetadataDiagnostics(segment(SEGMENT_NAME, null),
        "IN_MEMORY", Map.of()).finish(new MutableRoaringBitmap());
    assertEquals(noConsumer.get("consumingSegmentOffsetsBefore"), Map.of());
    assertEquals(noConsumer.get("consumingSegmentOffsetsAfter"), Map.of());
    Map<String, Object> unknownPartition = new ValidDocIdsMetadataDiagnostics(segment("uploadedSegment", null),
        "IN_MEMORY", Map.of()).finish(new MutableRoaringBitmap());
    assertFalse(unknownPartition.containsKey("consumingSegmentOffsetsBefore"));
    assertFalse(unknownPartition.containsKey("consumingSegmentOffsetsAfter"));
  }

  @DataProvider
  public Object[][] snapshotTypes() {
    return new Object[][] {
        {null, V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME},
        {ValidDocIdsType.SNAPSHOT.name(), V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME},
        {ValidDocIdsType.SNAPSHOT_WITH_DELETE.name(), V1Constants.QUERYABLE_DOC_IDS_SNAPSHOT_FILE_NAME}
    };
  }

  @Test(dataProvider = "snapshotTypes")
  public void testSnapshotAgeUsesSelectedFile(String type, String fileName)
      throws Exception {
    File directory = Files.createTempDirectory("upsert-diagnostics").toFile();
    try {
      Path snapshot = directory.toPath().resolve(fileName);
      Files.write(snapshot, new byte[]{1});
      Files.setLastModifiedTime(snapshot, FileTime.fromMillis(System.currentTimeMillis() - 60000));
      long modifiedTime = Files.getLastModifiedTime(snapshot).toMillis();
      ValidDocIdsMetadataDiagnostics diagnostics =
          new ValidDocIdsMetadataDiagnostics(segment(SEGMENT_NAME, directory), type, Map.of());
      Map<String, Object> result = diagnostics.finish(new MutableRoaringBitmap());
      assertEquals(result.get("snapshotFileAgeMs"), (long) result.get("captureEndTimeMs") - modifiedTime);
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  @Test
  public void testReplacedSnapshotDoesNotInheritOldFileAge()
      throws Exception {
    File directory = Files.createTempDirectory("upsert-diagnostics-replace").toFile();
    try {
      Path snapshot = directory.toPath().resolve(V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
      Files.write(snapshot, new byte[]{1});
      FileTime modified = Files.getLastModifiedTime(snapshot);
      ValidDocIdsMetadataDiagnostics diagnostics =
          new ValidDocIdsMetadataDiagnostics(segment(SEGMENT_NAME, directory), "SNAPSHOT", Map.of());
      Path replacement = directory.toPath().resolve("replacement");
      Files.write(replacement, new byte[]{2});
      Files.setLastModifiedTime(replacement, modified);
      Files.move(replacement, snapshot, StandardCopyOption.REPLACE_EXISTING);
      // Size and mtime alone cannot distinguish a concurrent atomic replacement.
      assertFalse(diagnostics.finish(new MutableRoaringBitmap()).containsKey("snapshotFileAgeMs"));
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  @Test
  public void testMissingAndFutureSnapshotAgeIsUnknown()
      throws Exception {
    File directory = Files.createTempDirectory("upsert-diagnostics-missing").toFile();
    try {
      IndexSegment segment = segment(SEGMENT_NAME, directory);
      assertFalse(new ValidDocIdsMetadataDiagnostics(segment, "SNAPSHOT", Map.of())
          .finish(new MutableRoaringBitmap()).containsKey("snapshotFileAgeMs"));
      Path snapshot = directory.toPath().resolve(V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
      Files.write(snapshot, new byte[]{1});
      Files.setLastModifiedTime(snapshot, FileTime.fromMillis(System.currentTimeMillis() + 60000));
      assertFalse(new ValidDocIdsMetadataDiagnostics(segment, "SNAPSHOT", Map.of())
          .finish(new MutableRoaringBitmap()).containsKey("snapshotFileAgeMs"));
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  private static RealtimeSegmentDataManager consumer(String name, long before, long after) {
    RealtimeSegmentDataManager consumer = mock(RealtimeSegmentDataManager.class);
    when(consumer.getSegmentName()).thenReturn(name);
    when(consumer.getCurrentOffset()).thenReturn(new LongMsgOffset(before), new LongMsgOffset(after));
    return consumer;
  }

  private static IndexSegment segment(String name, File directory) {
    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getSegmentName()).thenReturn(name);
    SegmentMetadata metadata = mock(SegmentMetadata.class);
    when(segment.getSegmentMetadata()).thenReturn(metadata);
    when(metadata.getIndexDir()).thenReturn(directory);
    return segment;
  }
}
