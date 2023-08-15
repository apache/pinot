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

package org.apache.pinot.server.starter.helix;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class FreshnessBasedConsumptionStatusCheckerTest {

  private class FakeFreshnessBasedConsumptionStatusChecker extends FreshnessBasedConsumptionStatusChecker {

    private final long _now;

    public FakeFreshnessBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager,
        Set<String> consumingSegments, long minFreshnessMs, long idleTimeoutMs, long now) {
      super(instanceDataManager, consumingSegments, minFreshnessMs, idleTimeoutMs);
      _now = now;
    }

    @Override
    protected long now() {
      return _now;
    }
  }

  @Test
  public void regularCaseWithOffsetCatchup() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments, 10000L, 0L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrB0 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    MutableSegment mockSegment = mock(MutableSegment.class);
    SegmentMetadata mockSegmentMetdata = mock(SegmentMetadata.class);
    when(mockSegment.getSegmentMetadata()).thenReturn(mockSegmentMetdata);
    when(mockSegmentMetdata.getLatestIngestionTimestamp()).thenReturn(0L);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              15                       20                     now               0
    // segA1              150                      200                    now               0
    // segB0              1500                     2000                   now               0
    when(segMngrA0.getSegment()).thenReturn(mockSegment);
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.getSegment()).thenReturn(mockSegment);
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(150));
    when(segMngrB0.getSegment()).thenReturn(mockSegment);
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(1500));

    when(segMngrA0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(2000));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              20                       20                     now               0
    // segA1              300                      200                    now               0
    // segB0              1998                     2000                   now               0
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    // The unexpected case where currentOffset > latestOffset
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(300));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(1998));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              20                       20                     100               0
    // segA1              200                      200                    100               0
    // segB0              2000                     2000                   100               0
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(2000));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  private void setupLatestIngestionTimestamp(LLRealtimeSegmentDataManager segmentDataManager,
      long latestIngestionTimestamp) {
    MutableSegment mockSegment = mock(MutableSegment.class);
    SegmentMetadata mockSegmentMetdata = mock(SegmentMetadata.class);
    when(mockSegment.getSegmentMetadata()).thenReturn(mockSegmentMetdata);
    when(mockSegmentMetdata.getLatestIngestionTimestamp()).thenReturn(latestIngestionTimestamp);
    when(segmentDataManager.getSegment()).thenReturn(mockSegment);
  }

  @Test
  public void regularCaseWithFreshnessCatchup() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FakeFreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments, 10L, 0L, 100L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrB0 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    when(segMngrA0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(2000));
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    // ensure negative values are ignored
    setupLatestIngestionTimestamp(segMngrA0, Long.MIN_VALUE);
    setupLatestIngestionTimestamp(segMngrA1, -1L);
    setupLatestIngestionTimestamp(segMngrB0, 0L);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              0                       20                     100               Long.MIN_VALUE
    // segA1              0                       200                    100               -1
    // segB0              0                       2000                   100               0
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              0                       20                     100               90
    // segA1              0                       200                    100               101
    // segB0              0                       2000                   100               50
    setupLatestIngestionTimestamp(segMngrA0, 90L);
    // Unexpected case where latest ingested is somehow after current time
    setupLatestIngestionTimestamp(segMngrA1, 101L);
    setupLatestIngestionTimestamp(segMngrB0, 89L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              20                       20                     100               90
    // segA1              200                      200                    100               101
    // segB0              1999                     2000                   100               95
    setupLatestIngestionTimestamp(segMngrB0, 95L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void regularCaseWithIdleTimeout() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    long idleTimeoutMs = 10L;
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FakeFreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments, 10L, idleTimeoutMs,
            100L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrB0 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    when(segMngrA0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrB0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    // ensure negative values are ignored
    setupLatestIngestionTimestamp(segMngrA0, Long.MIN_VALUE);
    setupLatestIngestionTimestamp(segMngrA1, -1L);
    setupLatestIngestionTimestamp(segMngrB0, 0L);

    when(segMngrA0.getSegmentIdleTime()).thenReturn(0L);
    when(segMngrA1.getSegmentIdleTime()).thenReturn(0L);
    when(segMngrB0.getSegmentIdleTime()).thenReturn(0L);

    //              total idle time
    // segA0              0
    // segA1              0
    // segB0              0
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    when(segMngrA0.getSegmentIdleTime()).thenReturn(idleTimeoutMs - 1);
    when(segMngrA1.getSegmentIdleTime()).thenReturn(idleTimeoutMs);
    when(segMngrB0.getSegmentIdleTime()).thenReturn(idleTimeoutMs + 1);
    //              total idle time
    // segA0              9
    // segA1              10
    // segB0              11
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 2);

    when(segMngrA0.getSegmentIdleTime()).thenReturn(idleTimeoutMs);
    when(segMngrA1.getSegmentIdleTime()).thenReturn(idleTimeoutMs + 1);
    //              total idle time
    // segA0              10
    // segA1              11
    // segB0              11
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);

    when(segMngrA0.getSegmentIdleTime()).thenReturn(idleTimeoutMs + 1);
    //              total idle time
    // segA0              11
    // segA1              11
    // segB0              11
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void testSegmentsNeverHealthyWhenIdleTimeoutZeroAndNoOtherCriteriaMet() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FakeFreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments, 10L, 0L, 100L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrB0 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    when(segMngrA0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrB0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    // ensure negative values are ignored
    setupLatestIngestionTimestamp(segMngrA0, Long.MIN_VALUE);
    setupLatestIngestionTimestamp(segMngrA1, -1L);
    setupLatestIngestionTimestamp(segMngrB0, 0L);

    when(segMngrA0.getSegmentIdleTime()).thenReturn(0L);
    when(segMngrA1.getSegmentIdleTime()).thenReturn(0L);
    when(segMngrB0.getSegmentIdleTime()).thenReturn(0L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    when(segMngrA0.getSegmentIdleTime()).thenReturn(10L);
    when(segMngrA1.getSegmentIdleTime()).thenReturn(100L);
    when(segMngrB0.getSegmentIdleTime()).thenReturn(1000L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);
  }

  @Test
  public void segmentBeingCommmitted() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FakeFreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments, 10L, 0L, 100L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrB0 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    when(segMngrA0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(2000));
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    // ensure negative values are ignored
    setupLatestIngestionTimestamp(segMngrA0, Long.MIN_VALUE);
    setupLatestIngestionTimestamp(segMngrA1, -1L);
    setupLatestIngestionTimestamp(segMngrB0, 0L);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              0                       20                     100               Long.MIN_VALUE
    // segA1              0                       200                    100               -1
    // segB0              0                       2000                   100               0
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // segB0 is now committed; ImmutableSegmentDataManager is returned by table data manager
    ImmutableSegmentDataManager immSegMngrB0 = mock(ImmutableSegmentDataManager.class);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(immSegMngrB0);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              0                       20                     100               90
    // segA1              0                       200                    100               101
    // segB0              already committed
    setupLatestIngestionTimestamp(segMngrA0, 90L);
    // Unexpected case where latest ingested is somehow after current time
    setupLatestIngestionTimestamp(segMngrA1, 101L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void testCannotGetOffsetsOrFreshness() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Set<String> consumingSegments = ImmutableSet.of(segA0, segA1, segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FakeFreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments, 10L, 0L, 100L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    TableDataManager tableDataManagerA = mock(TableDataManager.class);
    TableDataManager tableDataManagerB = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    LLRealtimeSegmentDataManager segMngrA0 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrA1 = mock(LLRealtimeSegmentDataManager.class);
    LLRealtimeSegmentDataManager segMngrB0 = mock(LLRealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    when(segMngrA0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(20));
    when(segMngrA1.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(200));
    when(segMngrB0.fetchLatestStreamOffset(5000)).thenReturn(null);
    when(segMngrA0.getCurrentOffset()).thenReturn(null);
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    // ensure negative values are ignored
    setupLatestIngestionTimestamp(segMngrA0, Long.MIN_VALUE);
    setupLatestIngestionTimestamp(segMngrA1, 90L);
    setupLatestIngestionTimestamp(segMngrB0, 0L);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              null                    20                     100               Long.MIN_VALUE
    // segA1              0                       200                    100               90
    // segB0              0                       null                   100               0
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 2);

    //              current offset          latest stream offset    current time    last ingestion time
    // segA0              20                      20                     100               89
    // segA1              0                       200                    100               90
    // segB0              0                       0                      100               0
    setupLatestIngestionTimestamp(segMngrA0, 89L);
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segMngrB0.fetchLatestStreamOffset(5000)).thenReturn(new LongMsgOffset(0));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }
}
