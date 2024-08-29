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
package org.apache.pinot.core.data.manager;

import java.util.Arrays;
import java.util.Collections;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class DuoSegmentDataManagerTest {
  @Test
  public void testGetSegments() {
    SegmentDataManager sdm1 = mockSegmentDataManager("seg01", false, 1);
    SegmentDataManager sdm2 = mockSegmentDataManager("seg01", true, 1);
    DuoSegmentDataManager dsdm = new DuoSegmentDataManager(sdm1, sdm2);

    assertTrue(dsdm.hasMultiSegments());
    assertSame(dsdm.getSegment(), sdm1.getSegment());
    assertEquals(dsdm.getSegments(), Arrays.asList(sdm1.getSegment(), sdm2.getSegment()));

    when(sdm1.getReferenceCount()).thenReturn(0);
    assertTrue(dsdm.hasMultiSegments());
    assertSame(dsdm.getSegment(), sdm1.getSegment());
    assertEquals(dsdm.getSegments(), Collections.singletonList(sdm2.getSegment()));

    when(sdm2.getReferenceCount()).thenReturn(0);
    assertTrue(dsdm.hasMultiSegments());
    assertSame(dsdm.getSegment(), sdm1.getSegment());
    assertTrue(dsdm.getSegments().isEmpty());
  }

  @Test
  public void testIncDecRefCnt() {
    SegmentDataManager sdm1 = mockSegmentDataManager("seg01", false, 1);
    SegmentDataManager sdm2 = mockSegmentDataManager("seg01", true, 1);
    DuoSegmentDataManager dsdm = new DuoSegmentDataManager(sdm1, sdm2);

    when(sdm1.increaseReferenceCount()).thenReturn(false);
    when(sdm2.increaseReferenceCount()).thenReturn(false);
    when(sdm1.decreaseReferenceCount()).thenReturn(false);
    when(sdm2.decreaseReferenceCount()).thenReturn(false);
    assertFalse(dsdm.increaseReferenceCount());
    assertFalse(dsdm.decreaseReferenceCount());

    when(sdm1.increaseReferenceCount()).thenReturn(true);
    when(sdm2.increaseReferenceCount()).thenReturn(false);
    when(sdm1.decreaseReferenceCount()).thenReturn(true);
    when(sdm2.decreaseReferenceCount()).thenReturn(false);
    assertTrue(dsdm.increaseReferenceCount());
    assertTrue(dsdm.decreaseReferenceCount());

    when(sdm1.increaseReferenceCount()).thenReturn(true);
    when(sdm2.increaseReferenceCount()).thenReturn(true);
    when(sdm1.decreaseReferenceCount()).thenReturn(true);
    when(sdm2.decreaseReferenceCount()).thenReturn(true);
    assertTrue(dsdm.increaseReferenceCount());
    assertTrue(dsdm.decreaseReferenceCount());
  }

  @Test
  public void testDoOffloadDestroy() {
    SegmentDataManager sdm1 = mockSegmentDataManager("seg01", false, 1);
    SegmentDataManager sdm2 = mockSegmentDataManager("seg01", true, 1);
    DuoSegmentDataManager dsdm = new DuoSegmentDataManager(sdm1, sdm2);

    dsdm.doOffload();
    verify(sdm1, times(0)).offload();
    verify(sdm2, times(0)).offload();
    dsdm.doDestroy();
    verify(sdm1, times(0)).destroy();
    verify(sdm2, times(0)).destroy();

    when(sdm1.getReferenceCount()).thenReturn(0);
    dsdm.doOffload();
    verify(sdm1, times(1)).offload();
    verify(sdm2, times(0)).offload();
    dsdm.doDestroy();
    verify(sdm1, times(1)).destroy();
    verify(sdm2, times(0)).destroy();

    reset(sdm1);
    when(sdm2.getReferenceCount()).thenReturn(0);
    dsdm.doOffload();
    verify(sdm1, times(1)).offload();
    verify(sdm2, times(1)).offload();
    dsdm.doDestroy();
    verify(sdm1, times(1)).destroy();
    verify(sdm2, times(1)).destroy();
  }

  private SegmentDataManager mockSegmentDataManager(String segmentName, boolean isMutable, int refCnt) {
    SegmentDataManager segmentDataManager =
        isMutable ? mock(RealtimeSegmentDataManager.class) : mock(ImmutableSegmentDataManager.class);
    IndexSegment segment = isMutable ? mock(MutableSegment.class) : mock(ImmutableSegment.class);
    when(segmentDataManager.getSegmentName()).thenReturn(segmentName);
    when(segmentDataManager.getSegment()).thenReturn(segment);
    when(segmentDataManager.getReferenceCount()).thenReturn(refCnt);
    return segmentDataManager;
  }
}
