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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class UpsertViewManagerTest {
  @Test
  public void testTrackUntrackSegments() {
    UpsertViewManager mgr = new UpsertViewManager(UpsertConfig.ConsistencyMode.SYNC, mock(UpsertContext.class));
    IndexSegment seg1 = mock(MutableSegment.class);
    ThreadSafeMutableRoaringBitmap threadSafeMutableRoaringBitmap = mock(ThreadSafeMutableRoaringBitmap.class);
    MutableRoaringBitmap mutableRoaringBitmap = new MutableRoaringBitmap();
    when(threadSafeMutableRoaringBitmap.getMutableRoaringBitmap()).thenReturn(mutableRoaringBitmap);
    when(seg1.getValidDocIds()).thenReturn(threadSafeMutableRoaringBitmap);
    when(seg1.getSegmentName()).thenReturn("seg1");

    SegmentContext segCtx1 = new SegmentContext(seg1);
    mgr.trackSegment(seg1);
    assertEquals(mgr.getTrackedSegments(), Set.of(seg1));
    mgr.setSegmentContexts(List.of(segCtx1), new HashMap<>());
    assertSame(segCtx1.getDocIdsSnapshot(), mutableRoaringBitmap);

    mgr.untrackSegment(seg1);
    assertTrue(mgr.getTrackedSegments().isEmpty());
    segCtx1 = new SegmentContext(seg1);
    mgr.setSegmentContexts(List.of(segCtx1), new HashMap<>());
    assertNull(segCtx1.getDocIdsSnapshot());
  }

  @Test
  public void testSkipUpsertDeleteReadsValidBitmapUnderSyncMode() {
    UpsertViewManager mgr = new UpsertViewManager(UpsertConfig.ConsistencyMode.SYNC, mock(UpsertContext.class));
    IndexSegment seg1 = mockSegmentWithDistinctBitmaps();
    mgr.trackSegment(seg1);

    SegmentContext segCtx = new SegmentContext(seg1);
    mgr.setSegmentContexts(List.of(segCtx), Map.of(CommonConstants.Broker.Request.QueryOptionKey.SKIP_UPSERT_DELETE,
        "true"));
    assertSame(segCtx.getDocIdsSnapshot(), VALID_RESULT);

    SegmentContext defaultCtx = new SegmentContext(seg1);
    mgr.setSegmentContexts(List.of(defaultCtx), new HashMap<>());
    assertSame(defaultCtx.getDocIdsSnapshot(), QUERYABLE_RESULT);
  }

  @Test
  public void testSkipUpsertDeleteReadsValidBitmapUnderSnapshotMode() {
    UpsertViewManager mgr = new UpsertViewManager(UpsertConfig.ConsistencyMode.SNAPSHOT, mock(UpsertContext.class));
    IndexSegment seg1 = mockSegmentWithDistinctBitmaps();
    mgr.trackSegment(seg1);

    // skipUpsertDelete reads the cached valid-docs map, refreshed alongside the queryable-docs one on track.
    SegmentContext segCtx = new SegmentContext(seg1);
    mgr.setSegmentContexts(List.of(segCtx), Map.of(CommonConstants.Broker.Request.QueryOptionKey.SKIP_UPSERT_DELETE,
        "true"));
    assertSame(segCtx.getDocIdsSnapshot(), VALID_RESULT);

    // Without the option, the cached queryable-docs map is used instead.
    SegmentContext defaultCtx = new SegmentContext(seg1);
    mgr.setSegmentContexts(List.of(defaultCtx), new HashMap<>());
    assertSame(defaultCtx.getDocIdsSnapshot(), QUERYABLE_RESULT);
  }

  /// Without a delete column, queryable docs == valid docs; the refresh reuses one clone for both caches instead
  /// of cloning twice (see doBatchRefreshUpsertView), so both maps must resolve to the same bitmap instance.
  @Test
  public void testNoDeleteColumnReusesSameBitmapInBothCaches() {
    UpsertViewManager mgr = new UpsertViewManager(UpsertConfig.ConsistencyMode.SNAPSHOT, mock(UpsertContext.class));
    IndexSegment seg1 = mock(MutableSegment.class);
    ThreadSafeMutableRoaringBitmap validBitmap = mock(ThreadSafeMutableRoaringBitmap.class);
    when(validBitmap.getMutableRoaringBitmap()).thenReturn(new MutableRoaringBitmap());
    when(seg1.getValidDocIds()).thenReturn(validBitmap);
    when(seg1.getQueryableDocIds()).thenReturn(null);
    when(seg1.getSegmentName()).thenReturn("seg1");

    mgr.trackSegment(seg1);
    assertSame(mgr.getQueryableDocIdsSnapshot(seg1), mgr.getValidDocIdsSnapshot(seg1));
  }

  private static final MutableRoaringBitmap VALID_RESULT = new MutableRoaringBitmap();
  private static final MutableRoaringBitmap QUERYABLE_RESULT = new MutableRoaringBitmap();

  /// Mocks a segment whose valid-docs and queryable-docs bitmaps resolve to two different, identity-comparable
  /// {@link MutableRoaringBitmap} instances, so tests can assert exactly which one a code path picked.
  private static IndexSegment mockSegmentWithDistinctBitmaps() {
    IndexSegment segment = mock(MutableSegment.class);
    ThreadSafeMutableRoaringBitmap validBitmap = mock(ThreadSafeMutableRoaringBitmap.class);
    ThreadSafeMutableRoaringBitmap queryableBitmap = mock(ThreadSafeMutableRoaringBitmap.class);
    when(validBitmap.getMutableRoaringBitmap()).thenReturn(VALID_RESULT);
    when(queryableBitmap.getMutableRoaringBitmap()).thenReturn(QUERYABLE_RESULT);
    when(segment.getValidDocIds()).thenReturn(validBitmap);
    when(segment.getQueryableDocIds()).thenReturn(queryableBitmap);
    when(segment.getSegmentName()).thenReturn("seg1");
    return segment;
  }
}
