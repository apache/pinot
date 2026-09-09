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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.List;
import org.apache.pinot.segment.spi.V1Constants;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;


public class UpsertSnapshotFingerprintTest {
  @Test
  public void testPopulationAndRetainedContentAreIndependentOfTraversalOrder() {
    UpsertSnapshotFingerprint.File first = file("a", "10", "first");
    UpsertSnapshotFingerprint.File retained = file("b", "20", "retained");
    UpsertSnapshotMetadata.Content before = UpsertSnapshotFingerprint.aggregate(List.of(first, retained));
    assertEquals(UpsertSnapshotFingerprint.aggregate(List.of(retained, first)), before);
    UpsertSnapshotMetadata.Content changed =
        UpsertSnapshotFingerprint.aggregate(List.of(first, file("b", "20", "different-retained-file")));
    assertEquals(before.populationFingerprint(), changed.populationFingerprint());
    assertNotEquals(before.savedFilesFingerprint(), changed.savedFilesFingerprint());
    assertNotEquals(before.populationFingerprint(),
        UpsertSnapshotFingerprint.aggregate(List.of(first, file("b", "21", "retained"))).populationFingerprint());
    assertEquals(before.expectedFiles(), 2);
    assertEquals(before.knownFiles(), 2);
  }

  @Test
  public void testUnknownAndDuplicateEntriesCannotClaimCoverage() {
    UpsertSnapshotMetadata.Content unknown = UpsertSnapshotFingerprint.aggregate(
        List.of(file("a", "10", "known"), file("b", "20", null)));
    assertEquals(unknown.expectedFiles(), 2);
    assertEquals(unknown.knownFiles(), 1);
    assertNull(unknown.savedFilesFingerprint());
    assertNull(UpsertSnapshotFingerprint.aggregate(List.of(file("a", null, "known"))).populationFingerprint());
    assertNull(UpsertSnapshotFingerprint.aggregate(
        List.of(file("a", "10", "known"), file("a", "10", "known"))).savedFilesFingerprint());
    assertNull(UpsertSnapshotFingerprint.aggregate(List.of(file("a", "-1", "known"))).populationFingerprint());
    assertNotEquals(UpsertSnapshotFingerprint.aggregate(List.of(file("a", "12", "x"))).populationFingerprint(),
        UpsertSnapshotFingerprint.aggregate(List.of(file("a1", "2", "x"))).populationFingerprint());
  }

  @Test
  public void testFileGenerationRejectsStaleReadsAndOverlappingWrites() {
    UpsertSnapshotFingerprint.Cache cache = new UpsertSnapshotFingerprint.Cache();
    String valid = V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME;
    String queryable = V1Constants.QUERYABLE_DOC_IDS_SNAPSHOT_FILE_NAME;
    UpsertSnapshotFingerprint.State oldRead = cache.read();
    UpsertSnapshotFingerprint.State write = cache.beginChange(valid);
    cache.endChange(write, valid, "new-valid");
    cache.seedRead(oldRead, valid, "stale-read");
    assertEquals(cache.read().fingerprint(false), "new-valid");
    write = cache.beginChange(queryable);
    assertNull(cache.read().fingerprint(false));
    cache.endChange(write, queryable, "queryable");
    assertEquals(cache.read().fingerprint(false), "new-valid");
    assertEquals(cache.read().fingerprint(true), "queryable");
    UpsertSnapshotFingerprint.State first = cache.beginChange(valid);
    UpsertSnapshotFingerprint.State second = cache.beginChange(valid);
    cache.endChange(first, valid, "first");
    cache.endChange(second, valid, "second");
    assertNull(cache.read().fingerprint(false));
    write = cache.beginChange(valid);
    cache.endChange(write, valid, "restored");
    cache.invalidate();
    cache.seedRead(cache.read(), valid, "old-owner-read");
    assertNull(cache.read().fingerprint(false));
  }

  @Test
  public void testCountsAndRawHashesDoNotReplaceMembershipComparison()
      throws Exception {
    MutableRoaringBitmap first = MutableRoaringBitmap.bitmapOf(1, 2);
    MutableRoaringBitmap second = MutableRoaringBitmap.bitmapOf(1, 3);
    assertEquals(first.getCardinality(), second.getCardinality());
    assertNotEquals(first, second);
    assertNotEquals(hash(first), hash(second));
    MutableRoaringBitmap plain = new MutableRoaringBitmap();
    for (int docId = 0; docId < 100_000; docId++) {
      plain.add(docId);
    }
    MutableRoaringBitmap optimized = plain.clone();
    optimized.runOptimize();
    assertEquals(plain, optimized);
    assertNotEquals(hash(plain), hash(optimized));
  }

  @Test
  public void testCoarseActivitySeesAnEntireOperationBetweenReads() {
    UpsertSnapshotActivity activity = new UpsertSnapshotActivity();
    UpsertSnapshotMetadata.Activity before = activity.read();
    activity.begin();
    activity.end(false);
    assertEquals(activity.read().activeOperations(), 0);
    assertNotEquals(activity.read().version(), before.version());
    assertEquals(activity.read().failedOperations(), 1);
  }

  @Test
  public void testCleanupOverlapAndFailurePreserveCompletedWatermark() {
    UpsertSnapshotCleanup cleanup = new UpsertSnapshotCleanup(true);
    UpsertSnapshotMetadata.CleanupProgress first = cleanup.begin(100D, true);
    cleanup.end(first, true);
    UpsertSnapshotMetadata.CleanupProgress before = cleanup.read();
    assertEquals(before.lastCompletedWatermark(), 100D);
    UpsertSnapshotMetadata.CleanupProgress running = cleanup.begin(200D, true);
    assertEquals(running.lastCompletedWatermark(), 100D);
    assertEquals(UpsertSnapshotDiagnostics.cleanupOverlaps(running, running), true);
    cleanup.end(running, false);
    assertEquals(cleanup.read().phase(), "FAILED_POSSIBLY_PARTIAL");
    assertEquals(cleanup.read().lastCompletedWatermark(), 100D);
    assertEquals(cleanup.read().failedPasses(), 1);
    assertEquals(UpsertSnapshotDiagnostics.cleanupOverlaps(before, cleanup.read()), true);
    running = cleanup.begin(null, false);
    cleanup.end(running, true);
    assertEquals(cleanup.read().lastCompletedWatermark(), 100D);
    assertEquals(cleanup.read().failedPasses(), 1);
    before = cleanup.read();
    running = cleanup.begin(200D, true);
    cleanup.end(running, true);
    assertEquals(UpsertSnapshotDiagnostics.cleanupOverlaps(before, cleanup.read()), true);
  }

  private static UpsertSnapshotFingerprint.File file(String name, String crc, String hash) {
    return new UpsertSnapshotFingerprint.File(name, crc, "VALID", hash);
  }

  private static String hash(MutableRoaringBitmap bitmap)
      throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    bitmap.serialize(new DataOutputStream(output));
    return UpsertSnapshotFingerprint.hash(output.toByteArray());
  }
}
