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
package org.apache.pinot.core.query.executor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.core.data.manager.DuoSegmentDataManager;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


public class SingleTableExecutionInfoTest {

  private static final String TABLE_NAME_WITH_TYPE = "testTable_REALTIME";

  /**
   * For a {@link DuoSegmentDataManager}, both the primary (immutable) segment and the secondary (mutable) segment
   * must appear in the index segment list used for query execution. The primary segment alone is not enough — a
   * mutable consuming segment continues to receive rows after the immutable was committed, and those rows must
   * still be queryable for the duration of the duo window.
   */
  @Test
  public void testCreateExpandsDuoSegmentDataManagerForNonUpsertTable()
      throws TableNotFoundException {
    SegmentDataManager singleSegmentMgr = mockImmutableSegmentDataManager("seg00");
    IndexSegment singleSegment = singleSegmentMgr.getSegment();

    SegmentDataManager primary = mockImmutableSegmentDataManager("seg01");
    SegmentDataManager secondary = mockMutableSegmentDataManager("seg01");
    IndexSegment primarySegment = primary.getSegment();
    IndexSegment secondarySegment = secondary.getSegment();
    DuoSegmentDataManager duo = new DuoSegmentDataManager(primary, secondary);

    List<SegmentDataManager> acquired = Arrays.asList(singleSegmentMgr, duo);
    InstanceDataManager instanceDataManager =
        mockInstanceDataManager(acquired, /*isUpsertEnabled=*/ false);

    SingleTableExecutionInfo info = SingleTableExecutionInfo.create(instanceDataManager, TABLE_NAME_WITH_TYPE,
        Arrays.asList("seg00", "seg01"), null, mock(QueryContext.class));

    // Single-segment manager contributes 1 segment; duo contributes 2 -> 3 total
    List<IndexSegment> indexSegments = info.getIndexSegments();
    assertEquals(indexSegments.size(), 3);
    assertSame(indexSegments.get(0), singleSegment);
    assertSame(indexSegments.get(1), primarySegment);
    assertSame(indexSegments.get(2), secondarySegment);

    // Acquired segment data managers list still tracks the duo once (one reference to release)
    assertEquals(info.getSegmentDataManagers().size(), 2);
    assertEquals(info.getNumSegmentsAcquired(), 2);
  }

  /**
   * When a {@link DuoSegmentDataManager}'s secondary segment has been released (refCount=0),
   * {@link DuoSegmentDataManager#getSegments()} only returns the live primary segment. The execution info should
   * include just that one segment for the duo manager — no nulls, no stale references.
   */
  @Test
  public void testCreateExpandsDuoSegmentDataManagerWithReleasedSecondary()
      throws TableNotFoundException {
    SegmentDataManager primary = mockImmutableSegmentDataManager("seg01");
    SegmentDataManager secondary = mockMutableSegmentDataManager("seg01");
    // Simulate the secondary already released after duo manager construction
    when(secondary.getReferenceCount()).thenReturn(0);
    IndexSegment primarySegment = primary.getSegment();
    DuoSegmentDataManager duo = new DuoSegmentDataManager(primary, secondary);

    InstanceDataManager instanceDataManager =
        mockInstanceDataManager(Collections.singletonList(duo), /*isUpsertEnabled=*/ false);

    SingleTableExecutionInfo info = SingleTableExecutionInfo.create(instanceDataManager, TABLE_NAME_WITH_TYPE,
        Collections.singletonList("seg01"), null, mock(QueryContext.class));

    List<IndexSegment> indexSegments = info.getIndexSegments();
    assertEquals(indexSegments.size(), 1);
    assertSame(indexSegments.get(0), primarySegment);
  }

  /**
   * Plain (non-duo) {@link SegmentDataManager} instances are still handled correctly: each one contributes
   * exactly one segment to the index segment list, matching the pre-multi-segment behavior.
   */
  @Test
  public void testCreateKeepsSingleSegmentBehaviorWhenNoMultiSegments()
      throws TableNotFoundException {
    SegmentDataManager sdm1 = mockImmutableSegmentDataManager("seg01");
    SegmentDataManager sdm2 = mockImmutableSegmentDataManager("seg02");
    IndexSegment seg1 = sdm1.getSegment();
    IndexSegment seg2 = sdm2.getSegment();

    InstanceDataManager instanceDataManager =
        mockInstanceDataManager(Arrays.asList(sdm1, sdm2), /*isUpsertEnabled=*/ false);

    SingleTableExecutionInfo info = SingleTableExecutionInfo.create(instanceDataManager, TABLE_NAME_WITH_TYPE,
        Arrays.asList("seg01", "seg02"), null, mock(QueryContext.class));

    List<IndexSegment> indexSegments = info.getIndexSegments();
    assertEquals(indexSegments.size(), 2);
    assertSame(indexSegments.get(0), seg1);
    assertSame(indexSegments.get(1), seg2);
  }

  /**
   * {@link SingleTableExecutionInfo#create} throws {@link TableNotFoundException} when the requested table
   * is not registered with the instance data manager.
   */
  @Test
  public void testCreateThrowsTableNotFoundWhenTableMissing() {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(TABLE_NAME_WITH_TYPE)).thenReturn(null);

    expectThrows(TableNotFoundException.class,
        () -> SingleTableExecutionInfo.create(instanceDataManager, TABLE_NAME_WITH_TYPE,
            Collections.emptyList(), null, mock(QueryContext.class)));
  }

  private static InstanceDataManager mockInstanceDataManager(List<SegmentDataManager> acquired,
      boolean isUpsertEnabled) {
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.isUpsertEnabled()).thenReturn(isUpsertEnabled);
    when(tableDataManager.acquireSegments(anyList(), any(), anyList())).thenAnswer(invocation -> {
      // Return the prebuilt list of acquired managers; tests do not exercise notAcquiredSegments
      return new ArrayList<>(acquired);
    });
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(TABLE_NAME_WITH_TYPE)).thenReturn(tableDataManager);
    return instanceDataManager;
  }

  private static SegmentDataManager mockImmutableSegmentDataManager(String segmentName) {
    SegmentDataManager sdm = mock(ImmutableSegmentDataManager.class);
    IndexSegment segment = mock(ImmutableSegment.class);
    when(sdm.getSegmentName()).thenReturn(segmentName);
    when(sdm.getSegment()).thenReturn(segment);
    when(sdm.getReferenceCount()).thenReturn(1);
    return sdm;
  }

  private static SegmentDataManager mockMutableSegmentDataManager(String segmentName) {
    SegmentDataManager sdm = mock(RealtimeSegmentDataManager.class);
    IndexSegment segment = mock(MutableSegment.class);
    when(sdm.getSegmentName()).thenReturn(segmentName);
    when(sdm.getSegment()).thenReturn(segment);
    when(sdm.getReferenceCount()).thenReturn(1);
    return sdm;
  }
}
