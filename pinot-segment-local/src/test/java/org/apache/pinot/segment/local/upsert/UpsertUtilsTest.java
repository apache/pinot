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

import java.util.HashMap;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class UpsertUtilsTest {

  /// Non-upsert table: `_partitionUpsertMetadataManager` is never set, so `hasNoQueryableDocs`
  /// short-circuits to `false`.
  @Test
  public void testHasNoQueryableDocsNonUpsert() {
    ImmutableSegmentImpl segment = newSegment();
    assertFalse(segment.hasNoQueryableDocs());
  }

  // -------- Non-consistency upsert: fall back to live queryable / valid bitmaps. --------

  @Test
  public void testHasNoQueryableDocsLiveQueryableEmpty() {
    ImmutableSegmentImpl segment = newSegment();
    PartitionUpsertMetadataManager manager = mock(PartitionUpsertMetadataManager.class);
    when(manager.getUpsertViewManager()).thenReturn(null);
    ThreadSafeMutableRoaringBitmap queryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(queryable.isEmpty()).thenReturn(true);
    segment.enableUpsert(manager, mock(ThreadSafeMutableRoaringBitmap.class), queryable);
    assertTrue(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsLiveQueryableNonEmpty() {
    ImmutableSegmentImpl segment = newSegment();
    PartitionUpsertMetadataManager manager = mock(PartitionUpsertMetadataManager.class);
    when(manager.getUpsertViewManager()).thenReturn(null);
    ThreadSafeMutableRoaringBitmap queryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(queryable.isEmpty()).thenReturn(false);
    segment.enableUpsert(manager, mock(ThreadSafeMutableRoaringBitmap.class), queryable);
    assertFalse(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsFallsBackToValidDocIdsWhenQueryableMissing() {
    ImmutableSegmentImpl segment = newSegment();
    PartitionUpsertMetadataManager manager = mock(PartitionUpsertMetadataManager.class);
    when(manager.getUpsertViewManager()).thenReturn(null);
    ThreadSafeMutableRoaringBitmap valid = mock(ThreadSafeMutableRoaringBitmap.class);
    when(valid.isEmpty()).thenReturn(true);
    segment.enableUpsert(manager, valid, null);
    assertTrue(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsReturnsFalseWhenBothBitmapsMissing() {
    ImmutableSegmentImpl segment = newSegment();
    PartitionUpsertMetadataManager manager = mock(PartitionUpsertMetadataManager.class);
    when(manager.getUpsertViewManager()).thenReturn(null);
    segment.enableUpsert(manager, null, null);
    assertFalse(segment.hasNoQueryableDocs());
  }

  // -------- Consistency-mode upsert: snapshot is the source of truth. --------

  @Test
  public void testHasNoQueryableDocsConsistencyModeSnapshotEmpty() {
    // Even if the live bitmap has docs, the snapshot's view (empty) is what the query will scan.
    ImmutableSegmentImpl segment = newSegment();
    PartitionUpsertMetadataManager manager = mock(PartitionUpsertMetadataManager.class);
    UpsertViewManager viewManager = mock(UpsertViewManager.class);
    when(manager.getUpsertViewManager()).thenReturn(viewManager);
    when(viewManager.getQueryableDocIdsSnapshot(any())).thenReturn(new MutableRoaringBitmap());
    ThreadSafeMutableRoaringBitmap liveQueryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(liveQueryable.isEmpty()).thenReturn(false);
    segment.enableUpsert(manager, mock(ThreadSafeMutableRoaringBitmap.class), liveQueryable);
    assertTrue(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsConsistencyModeSnapshotNonEmpty() {
    // Even if the live bitmap is empty, the snapshot has docs the query will scan.
    ImmutableSegmentImpl segment = newSegment();
    PartitionUpsertMetadataManager manager = mock(PartitionUpsertMetadataManager.class);
    UpsertViewManager viewManager = mock(UpsertViewManager.class);
    MutableRoaringBitmap snapshot = new MutableRoaringBitmap();
    snapshot.add(0);
    when(manager.getUpsertViewManager()).thenReturn(viewManager);
    when(viewManager.getQueryableDocIdsSnapshot(any())).thenReturn(snapshot);
    ThreadSafeMutableRoaringBitmap liveQueryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(liveQueryable.isEmpty()).thenReturn(true);
    segment.enableUpsert(manager, mock(ThreadSafeMutableRoaringBitmap.class), liveQueryable);
    assertFalse(segment.hasNoQueryableDocs());
  }

  @Test
  public void testHasNoQueryableDocsConsistencyModeSnapshotAbsent() {
    // Consistency mode on, but this segment is not in the current refresh (first refresh hasn't
    // run, or the segment was just tracked). Live bitmap might disagree with the upcoming
    // snapshot, so do not claim empty.
    ImmutableSegmentImpl segment = newSegment();
    PartitionUpsertMetadataManager manager = mock(PartitionUpsertMetadataManager.class);
    UpsertViewManager viewManager = mock(UpsertViewManager.class);
    when(manager.getUpsertViewManager()).thenReturn(viewManager);
    when(viewManager.getQueryableDocIdsSnapshot(any())).thenReturn(null);
    ThreadSafeMutableRoaringBitmap liveQueryable = mock(ThreadSafeMutableRoaringBitmap.class);
    when(liveQueryable.isEmpty()).thenReturn(true);
    segment.enableUpsert(manager, mock(ThreadSafeMutableRoaringBitmap.class), liveQueryable);
    assertFalse(segment.hasNoQueryableDocs());
  }

  /// Returns a minimal {@link ImmutableSegmentImpl} usable for testing the upsert-aware methods.
  /// Mirrors the pattern used in {@code BasePartitionUpsertMetadataManagerTest#createImmutableSegment}.
  private static ImmutableSegmentImpl newSegment() {
    return new ImmutableSegmentImpl(
        mock(SegmentDirectory.class), mock(SegmentMetadataImpl.class), new HashMap<>(), null);
  }
}
