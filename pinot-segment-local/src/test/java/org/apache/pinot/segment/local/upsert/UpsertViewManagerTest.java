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
}
