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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
    assertEquals(mgr.getTrackedSegments(), Collections.singleton(seg1));
    mgr.setSegmentContexts(Collections.singletonList(segCtx1), new HashMap<>());
    assertSame(segCtx1.getQueryableDocIdsSnapshot(), mutableRoaringBitmap);

    mgr.untrackSegment(seg1);
    assertTrue(mgr.getTrackedSegments().isEmpty());
    segCtx1 = new SegmentContext(seg1);
    mgr.setSegmentContexts(Collections.singletonList(segCtx1), new HashMap<>());
    assertNull(segCtx1.getQueryableDocIdsSnapshot());
  }

  @Test
  public void testUntrackEvictsFromSnapshotView() {
    UpsertContext context = mock(UpsertContext.class);
    when(context.getUpsertViewRefreshIntervalMs()).thenReturn(0L);
    UpsertViewManager mgr = new UpsertViewManager(UpsertConfig.ConsistencyMode.SNAPSHOT, context);

    // Set up two segments with queryable doc ids
    IndexSegment seg1 = mock(MutableSegment.class);
    when(seg1.getSegmentName()).thenReturn("seg1");
    ThreadSafeMutableRoaringBitmap bitmap1 = new ThreadSafeMutableRoaringBitmap();
    bitmap1.add(1);
    bitmap1.add(3);
    when(seg1.getQueryableDocIds()).thenReturn(bitmap1);
    when(seg1.getValidDocIds()).thenReturn(bitmap1);

    IndexSegment seg2 = mock(MutableSegment.class);
    when(seg2.getSegmentName()).thenReturn("seg2");
    ThreadSafeMutableRoaringBitmap bitmap2 = new ThreadSafeMutableRoaringBitmap();
    bitmap2.add(2);
    bitmap2.add(4);
    when(seg2.getQueryableDocIds()).thenReturn(bitmap2);
    when(seg2.getValidDocIds()).thenReturn(bitmap2);

    // Track both segments — this triggers a refresh that populates _segmentQueryableDocIdsMap
    mgr.trackSegment(seg1);
    mgr.trackSegment(seg2);
    Map<IndexSegment, MutableRoaringBitmap> viewMap = mgr.getSegmentQueryableDocIdsMap();
    assertEquals(viewMap.size(), 2);
    assertTrue(viewMap.containsKey(seg1));
    assertTrue(viewMap.containsKey(seg2));

    // Mark seg1 as updated to verify _updatedSegmentsSinceLastRefresh cleanup
    mgr.getUpdatedSegmentsSinceLastRefresh().add(seg1);

    // Untrack seg1 — should eagerly evict from snapshot view map and updated set
    mgr.untrackSegment(seg1);
    assertFalse(mgr.getTrackedSegments().contains(seg1));
    assertFalse(mgr.getUpdatedSegmentsSinceLastRefresh().contains(seg1));
    Map<IndexSegment, MutableRoaringBitmap> updatedMap = mgr.getSegmentQueryableDocIdsMap();
    assertEquals(updatedMap.size(), 1);
    assertFalse(updatedMap.containsKey(seg1));
    assertTrue(updatedMap.containsKey(seg2));
  }
}
