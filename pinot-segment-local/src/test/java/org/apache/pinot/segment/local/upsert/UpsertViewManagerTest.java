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
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


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
    assertEquals(mgr.getOptionalSegments(), Collections.singleton(seg1.getSegmentName()));
    mgr.setSegmentContexts(Collections.singletonList(segCtx1), new HashMap<>());
    assertSame(segCtx1.getQueryableDocIdsSnapshot(), mutableRoaringBitmap);

    mgr.untrackSegment(seg1);
    assertTrue(mgr.getOptionalSegments().isEmpty());
    segCtx1 = new SegmentContext(seg1);
    mgr.setSegmentContexts(Collections.singletonList(segCtx1), new HashMap<>());
    assertNull(segCtx1.getQueryableDocIdsSnapshot());
  }

  @Test
  public void testNeedForceRefresh() {
    UpsertViewManager mgr = new UpsertViewManager(UpsertConfig.ConsistencyMode.SYNC, mock(UpsertContext.class));
    assertNull(mgr.getSegmentQueryableDocIdsMap());
    assertTrue(mgr.needForceRefresh());

    IndexSegment seg1 = mock(MutableSegment.class);
    when(seg1.getSegmentName()).thenReturn("seg1");
    IndexSegment seg2 = mock(ImmutableSegment.class);
    when(seg2.getSegmentName()).thenReturn("seg2");
    mgr.trackSegment(seg1);
    mgr.trackSegment(seg2);
    assertTrue(mgr.needForceRefresh());

    mgr.doBatchRefreshUpsertView(-1, true);
    Map<IndexSegment, MutableRoaringBitmap> upsertView = mgr.getSegmentQueryableDocIdsMap();
    assertNotNull(upsertView);
    // Empty bitmaps are set in segmentContexts for segments without validDocIds bitmaps.
    assertTrue(upsertView.get(seg1).isEmpty());
    assertTrue(upsertView.get(seg2).isEmpty());
    assertFalse(mgr.needForceRefresh());

    // Missing segment in upsert view.
    IndexSegment seg3 = mock(ImmutableSegment.class);
    when(seg3.getSegmentName()).thenReturn("seg3");
    mgr.trackSegment(seg3);
    assertTrue(mgr.needForceRefresh());

    // Having extra segment in upsert view.
    mgr.doBatchRefreshUpsertView(-1, true);
    assertFalse(mgr.needForceRefresh());
    mgr.untrackSegment(seg3);
    assertTrue(mgr.needForceRefresh());
  }
}
