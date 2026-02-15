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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class FreshnessBasedConsumptionStatusCheckerTest {

  private void setupMinimumIngestionLag(RealtimeSegmentDataManager segmentDataManager,
      long minimumIngestionLagMs) {
    MutableSegment mockSegment = mock(MutableSegment.class);
    SegmentMetadata mockSegmentMetdata = mock(SegmentMetadata.class);
    when(mockSegment.getSegmentMetadata()).thenReturn(mockSegmentMetdata);
    when(mockSegmentMetdata.getMinimumIngestionLagMs()).thenReturn(minimumIngestionLagMs);
    when(segmentDataManager.getSegment()).thenReturn(mockSegment);
  }

  @Test
  public void regularCaseWithOffsetCatchup() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Map<String, Set<String>> consumingSegments = new HashMap<>();
    consumingSegments.put("tableA_REALTIME", ImmutableSet.of(segA0, segA1));
    consumingSegments.put("tableB_REALTIME", ImmutableSet.of(segB0));
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments,
            ConsumptionStatusCheckerTestUtils.getConsumingSegments(consumingSegments), 10000L, 0L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    RealtimeTableDataManager tableDataManagerA = mock(RealtimeTableDataManager.class);
    RealtimeTableDataManager tableDataManagerB = mock(RealtimeTableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    // mock minimum ingestion lag higher than the threshold so offset catchup is used
    setupMinimumIngestionLag(segMngrA0, Long.MAX_VALUE);
    setupMinimumIngestionLag(segMngrA1, Long.MAX_VALUE);
    setupMinimumIngestionLag(segMngrB0, Long.MAX_VALUE);

    //              current offset          latest stream offset    minimum ingestion lag
    // segA0              15                       20                     MAX_VALUE
    // segA1              150                      200                    MAX_VALUE
    // segB0              1500                     2000                   MAX_VALUE
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(15));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(150));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(1500));

    StreamMetadataProvider segA0Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segA1Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segB0Provider = mock(StreamMetadataProvider.class);

    when(tableDataManagerA.getStreamMetadataProvider(segMngrA0)).thenReturn(segA0Provider);
    when(tableDataManagerA.getStreamMetadataProvider(segMngrA1)).thenReturn(segA1Provider);
    when(tableDataManagerB.getStreamMetadataProvider(segMngrB0)).thenReturn(segB0Provider);

    when(segMngrA0.getStreamPartitionId()).thenReturn(0);
    when(segMngrA1.getStreamPartitionId()).thenReturn(1);
    when(segMngrB0.getStreamPartitionId()).thenReturn(0);

    when(segA0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA1Provider.supportsOffsetLag()).thenReturn(true);
    when(segB0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(20)));
    when(segA1Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(1, new LongMsgOffset(200)));
    when(segB0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(2000)));

    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    //              current offset          latest stream offset
    // segA0              20                       20
    // segA1              300                      200
    // segB0              1998                     2000
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    // The unexpected case where currentOffset > latestOffset
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(300));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(1998));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);

    //              current offset          latest stream offset
    // segA0              20                       20
    // segA1              200                      200
    // segB0              2000                     2000
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(2000));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void testWithDroppedTableAndSegment() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Map<String, Set<String>> consumingSegments = new HashMap<>();
    consumingSegments.computeIfAbsent("tableA_REALTIME", k -> new HashSet<>()).add(segA0);
    consumingSegments.computeIfAbsent("tableA_REALTIME", k -> new HashSet<>()).add(segA1);
    consumingSegments.computeIfAbsent("tableB_REALTIME", k -> new HashSet<>()).add(segB0);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments,
            ConsumptionStatusCheckerTestUtils.getConsumingSegments(consumingSegments), 10L, 0L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    RealtimeTableDataManager tableDataManagerA = mock(RealtimeTableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(null);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(null);

    StreamMetadataProvider segA0Provider = mock(StreamMetadataProvider.class);
    when(tableDataManagerA.getStreamMetadataProvider(segMngrA0)).thenReturn(segA0Provider);
    when(segMngrA0.getStreamPartitionId()).thenReturn(0);

    when(segA0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(20)));

    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    // ensure high lag values are handled - offset catchup will be used
    setupMinimumIngestionLag(segMngrA0, Long.MAX_VALUE);

    //              current offset          latest stream offset    minimum ingestion lag
    // segA0              0                       20                     MAX_VALUE
    // segA1 (segment is absent)
    // segB0 (table is absent)
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // updatedConsumingSegments still provide 3 segments to checker but one has caught up.
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 2);
    // Remove the missing segments and check again.
    consumingSegments.get("tableA_REALTIME").remove(segA1);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);
    consumingSegments.remove("tableB_REALTIME");
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void regularCaseWithFreshnessCatchup() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Map<String, Set<String>> consumingSegments = new HashMap<>();
    consumingSegments.put("tableA_REALTIME", ImmutableSet.of(segA0, segA1));
    consumingSegments.put("tableB_REALTIME", ImmutableSet.of(segB0));
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    long minFreshnessMs = 10L;
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments,
            ConsumptionStatusCheckerTestUtils.getConsumingSegments(consumingSegments), minFreshnessMs, 0L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    RealtimeTableDataManager tableDataManagerA = mock(RealtimeTableDataManager.class);
    RealtimeTableDataManager tableDataManagerB = mock(RealtimeTableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    StreamMetadataProvider segA0Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segA1Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segB0Provider = mock(StreamMetadataProvider.class);

    when(tableDataManagerA.getStreamMetadataProvider(segMngrA0)).thenReturn(segA0Provider);
    when(tableDataManagerA.getStreamMetadataProvider(segMngrA1)).thenReturn(segA1Provider);
    when(tableDataManagerB.getStreamMetadataProvider(segMngrB0)).thenReturn(segB0Provider);

    when(segMngrA0.getStreamPartitionId()).thenReturn(0);
    when(segMngrA1.getStreamPartitionId()).thenReturn(1);
    when(segMngrB0.getStreamPartitionId()).thenReturn(0);

    when(segA0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA1Provider.supportsOffsetLag()).thenReturn(true);
    when(segB0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(20)));
    when(segA1Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(1, new LongMsgOffset(200)));
    when(segB0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(2000)));
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));

    // High minimum ingestion lag - segments not caught up
    setupMinimumIngestionLag(segMngrA0, 200L);
    setupMinimumIngestionLag(segMngrA1, 200L);
    setupMinimumIngestionLag(segMngrB0, 200L);

    //              minimum ingestion lag     minFreshnessMs
    // segA0              200                      10
    // segA1              200                      10
    // segB0              200                      10
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    //              minimum ingestion lag     minFreshnessMs
    // segA0              5                       10  (caught up)
    // segA1              10                      10  (caught up - exactly at threshold)
    // segB0              15                      10  (not caught up)
    setupMinimumIngestionLag(segMngrA0, 5L);
    setupMinimumIngestionLag(segMngrA1, 10L);
    setupMinimumIngestionLag(segMngrB0, 15L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);

    //              minimum ingestion lag     minFreshnessMs
    // segA0              5                       10  (caught up)
    // segA1              10                      10  (caught up)
    // segB0              8                       10  (caught up)
    setupMinimumIngestionLag(segMngrB0, 8L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void regularCaseWithIdleTimeout() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Map<String, Set<String>> consumingSegments = new HashMap<>();
    consumingSegments.put("tableA_REALTIME", ImmutableSet.of(segA0, segA1));
    consumingSegments.put("tableB_REALTIME", ImmutableSet.of(segB0));
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    long idleTimeoutMs = 10L;
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments,
            ConsumptionStatusCheckerTestUtils.getConsumingSegments(consumingSegments), 10L, idleTimeoutMs);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    RealtimeTableDataManager tableDataManagerA = mock(RealtimeTableDataManager.class);
    RealtimeTableDataManager tableDataManagerB = mock(RealtimeTableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    StreamMetadataProvider segA0Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segA1Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segB0Provider = mock(StreamMetadataProvider.class);

    when(tableDataManagerA.getStreamMetadataProvider(segMngrA0)).thenReturn(segA0Provider);
    when(tableDataManagerA.getStreamMetadataProvider(segMngrA1)).thenReturn(segA1Provider);
    when(tableDataManagerB.getStreamMetadataProvider(segMngrB0)).thenReturn(segB0Provider);

    when(segMngrA0.getStreamPartitionId()).thenReturn(0);
    when(segMngrA1.getStreamPartitionId()).thenReturn(1);
    when(segMngrB0.getStreamPartitionId()).thenReturn(0);

    when(segA0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA1Provider.supportsOffsetLag()).thenReturn(true);
    when(segB0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(20)));
    when(segA1Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(1, new LongMsgOffset(20)));
    when(segB0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(20)));

    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));

    // mock minimum ingestion lag higher than the threshold so idle timeout is used
    setupMinimumIngestionLag(segMngrA0, 200L);
    setupMinimumIngestionLag(segMngrA1, 200L);
    setupMinimumIngestionLag(segMngrB0, 200L);

    when(segMngrA0.getTimeSinceEventLastConsumedMs()).thenReturn(0L);
    when(segMngrA1.getTimeSinceEventLastConsumedMs()).thenReturn(0L);
    when(segMngrB0.getTimeSinceEventLastConsumedMs()).thenReturn(0L);

    //              total idle time
    // segA0              0
    // segA1              0
    // segB0              0
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    when(segMngrA0.getTimeSinceEventLastConsumedMs()).thenReturn(idleTimeoutMs - 1);
    when(segMngrA1.getTimeSinceEventLastConsumedMs()).thenReturn(idleTimeoutMs);
    when(segMngrB0.getTimeSinceEventLastConsumedMs()).thenReturn(idleTimeoutMs + 1);
    //              total idle time
    // segA0              9
    // segA1              10
    // segB0              11
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 2);

    when(segMngrA0.getTimeSinceEventLastConsumedMs()).thenReturn(idleTimeoutMs);
    when(segMngrA1.getTimeSinceEventLastConsumedMs()).thenReturn(idleTimeoutMs + 1);
    //              total idle time
    // segA0              10
    // segA1              11
    // segB0              11
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);

    when(segMngrA0.getTimeSinceEventLastConsumedMs()).thenReturn(idleTimeoutMs + 1);
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
    Map<String, Set<String>> consumingSegments = new HashMap<>();
    consumingSegments.put("tableA_REALTIME", ImmutableSet.of(segA0, segA1));
    consumingSegments.put("tableB_REALTIME", ImmutableSet.of(segB0));
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments,
            ConsumptionStatusCheckerTestUtils.getConsumingSegments(consumingSegments), 10L, 0L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    RealtimeTableDataManager tableDataManagerA = mock(RealtimeTableDataManager.class);
    RealtimeTableDataManager tableDataManagerB = mock(RealtimeTableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    StreamMetadataProvider segA0Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segA1Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segB0Provider = mock(StreamMetadataProvider.class);

    when(tableDataManagerA.getStreamMetadataProvider(segMngrA0)).thenReturn(segA0Provider);
    when(tableDataManagerA.getStreamMetadataProvider(segMngrA1)).thenReturn(segA1Provider);
    when(tableDataManagerB.getStreamMetadataProvider(segMngrB0)).thenReturn(segB0Provider);

    when(segMngrA0.getStreamPartitionId()).thenReturn(0);
    when(segMngrA1.getStreamPartitionId()).thenReturn(1);
    when(segMngrB0.getStreamPartitionId()).thenReturn(0);

    when(segA0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA1Provider.supportsOffsetLag()).thenReturn(true);
    when(segB0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(20)));
    when(segA1Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(1, new LongMsgOffset(20)));
    when(segB0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(20)));

    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    // minimum ingestion lag is higher than threshold
    setupMinimumIngestionLag(segMngrA0, 200L);
    setupMinimumIngestionLag(segMngrA1, 200L);
    setupMinimumIngestionLag(segMngrB0, 200L);

    when(segMngrA0.getTimeSinceEventLastConsumedMs()).thenReturn(0L);
    when(segMngrA1.getTimeSinceEventLastConsumedMs()).thenReturn(0L);
    when(segMngrB0.getTimeSinceEventLastConsumedMs()).thenReturn(0L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    when(segMngrA0.getTimeSinceEventLastConsumedMs()).thenReturn(10L);
    when(segMngrA1.getTimeSinceEventLastConsumedMs()).thenReturn(100L);
    when(segMngrB0.getTimeSinceEventLastConsumedMs()).thenReturn(1000L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);
  }

  @Test
  public void segmentBeingCommitted() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Map<String, Set<String>> consumingSegments = new HashMap<>();
    consumingSegments.put("tableA_REALTIME", ImmutableSet.of(segA0, segA1));
    consumingSegments.put("tableB_REALTIME", ImmutableSet.of(segB0));
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments,
            ConsumptionStatusCheckerTestUtils.getConsumingSegments(consumingSegments), 10L, 0L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    RealtimeTableDataManager tableDataManagerA = mock(RealtimeTableDataManager.class);
    RealtimeTableDataManager tableDataManagerB = mock(RealtimeTableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    StreamMetadataProvider segA0Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segA1Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segB0Provider = mock(StreamMetadataProvider.class);

    when(tableDataManagerA.getStreamMetadataProvider(segMngrA0)).thenReturn(segA0Provider);
    when(tableDataManagerA.getStreamMetadataProvider(segMngrA1)).thenReturn(segA1Provider);
    when(tableDataManagerB.getStreamMetadataProvider(segMngrB0)).thenReturn(segB0Provider);

    when(segMngrA0.getStreamPartitionId()).thenReturn(0);
    when(segMngrA1.getStreamPartitionId()).thenReturn(1);
    when(segMngrB0.getStreamPartitionId()).thenReturn(0);

    when(segA0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA1Provider.supportsOffsetLag()).thenReturn(true);
    when(segB0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(20)));
    when(segA1Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(1, new LongMsgOffset(200)));
    when(segB0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(2000)));

    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    // minimum ingestion lag is higher than threshold
    setupMinimumIngestionLag(segMngrA0, 200L);
    setupMinimumIngestionLag(segMngrA1, 200L);
    setupMinimumIngestionLag(segMngrB0, 200L);

    //              minimum ingestion lag     minFreshnessMs
    // segA0              200                      10
    // segA1              200                      10
    // segB0              200                      10
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // segB0 is now committed; ImmutableSegmentDataManager is returned by table data manager
    ImmutableSegmentDataManager immSegMngrB0 = mock(ImmutableSegmentDataManager.class);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(immSegMngrB0);

    //              minimum ingestion lag     minFreshnessMs
    // segA0              5                       10  (caught up)
    // segA1              0                       10  (caught up)
    // segB0              already committed
    setupMinimumIngestionLag(segMngrA0, 5L);
    setupMinimumIngestionLag(segMngrA1, 0L);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);
    consumingSegments.get("tableB_REALTIME").remove(segB0);
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void testCannotGetOffsetsOrFreshness() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    String segB0 = "tableB__0__0__123Z";
    Map<String, Set<String>> consumingSegments = new HashMap<>();
    consumingSegments.put("tableA_REALTIME", ImmutableSet.of(segA0, segA1));
    consumingSegments.put("tableB_REALTIME", ImmutableSet.of(segB0));
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments,
            ConsumptionStatusCheckerTestUtils.getConsumingSegments(consumingSegments), 10L, 0L);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 3);

    // setup TableDataMangers
    RealtimeTableDataManager tableDataManagerA = mock(RealtimeTableDataManager.class);
    RealtimeTableDataManager tableDataManagerB = mock(RealtimeTableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager("tableB_REALTIME")).thenReturn(tableDataManagerB);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrB0 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);
    when(tableDataManagerB.acquireSegment(segB0)).thenReturn(segMngrB0);

    StreamMetadataProvider segA0Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segA1Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segB0Provider = mock(StreamMetadataProvider.class);

    when(tableDataManagerA.getStreamMetadataProvider(segMngrA0)).thenReturn(segA0Provider);
    when(tableDataManagerA.getStreamMetadataProvider(segMngrA1)).thenReturn(segA1Provider);
    when(tableDataManagerB.getStreamMetadataProvider(segMngrB0)).thenReturn(segB0Provider);

    when(segMngrA0.getStreamPartitionId()).thenReturn(0);
    when(segMngrA1.getStreamPartitionId()).thenReturn(1);
    when(segMngrB0.getStreamPartitionId()).thenReturn(0);

    when(segA0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA1Provider.supportsOffsetLag()).thenReturn(true);
    when(segB0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(20)));
    when(segA1Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(1, new LongMsgOffset(200)));
    when(segB0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of());

    when(segMngrA0.getCurrentOffset()).thenReturn(null);
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    when(segMngrB0.getCurrentOffset()).thenReturn(new LongMsgOffset(0));
    // segA0 and segB0 have unknown minimum lag, segA1 has good lag (caught up)
    setupMinimumIngestionLag(segMngrA0, Long.MAX_VALUE);
    setupMinimumIngestionLag(segMngrA1, 5L);
    setupMinimumIngestionLag(segMngrB0, Long.MAX_VALUE);

    //              current offset          latest stream offset    minimum ingestion lag
    // segA0              null                    20                     MAX_VALUE (unknown)
    // segA1              0                       200                    5 (caught up)
    // segB0              0                       null                   MAX_VALUE (unknown)
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 2);

    //              current offset          latest stream offset    minimum ingestion lag
    // segA0              20                      20                     MAX_VALUE (offset caught up)
    // segA1              0                       200                    5 (caught up)
    // segB0              0                       0                      MAX_VALUE (offset caught up)
    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(20));
    when(segB0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(0, new LongMsgOffset(0)));
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }

  @Test
  public void testTimeoutExceptionWhenFetchingLatestStreamOffset() {
    String segA0 = "tableA__0__0__123Z";
    String segA1 = "tableA__1__0__123Z";
    Map<String, Set<String>> consumingSegments = new HashMap<>();
    consumingSegments.put("tableA_REALTIME", ImmutableSet.of(segA0, segA1));
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    long idleTimeoutMs = 10L;
    FreshnessBasedConsumptionStatusChecker statusChecker =
        new FreshnessBasedConsumptionStatusChecker(instanceDataManager, consumingSegments,
            ConsumptionStatusCheckerTestUtils.getConsumingSegments(consumingSegments), 10L, idleTimeoutMs);

    // TableDataManager is not set up yet
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 2);

    // setup TableDataManager
    RealtimeTableDataManager tableDataManagerA = mock(RealtimeTableDataManager.class);
    when(instanceDataManager.getTableDataManager("tableA_REALTIME")).thenReturn(tableDataManagerA);

    // setup SegmentDataManagers
    RealtimeSegmentDataManager segMngrA0 = mock(RealtimeSegmentDataManager.class);
    RealtimeSegmentDataManager segMngrA1 = mock(RealtimeSegmentDataManager.class);
    when(tableDataManagerA.acquireSegment(segA0)).thenReturn(segMngrA0);
    when(tableDataManagerA.acquireSegment(segA1)).thenReturn(segMngrA1);

    StreamMetadataProvider segA0Provider = mock(StreamMetadataProvider.class);
    StreamMetadataProvider segA1Provider = mock(StreamMetadataProvider.class);

    when(tableDataManagerA.getStreamMetadataProvider(segMngrA0)).thenReturn(segA0Provider);
    when(tableDataManagerA.getStreamMetadataProvider(segMngrA1)).thenReturn(segA1Provider);

    when(segMngrA0.getStreamPartitionId()).thenReturn(0);
    when(segMngrA1.getStreamPartitionId()).thenReturn(1);

    when(segA0Provider.supportsOffsetLag()).thenReturn(true);
    when(segA1Provider.supportsOffsetLag()).thenReturn(true);
    // segA0 provider throws RuntimeException - this should be caught and handled gracefully
    // In practice, RealtimeSegmentMetadataUtils wraps TimeoutException in RuntimeException
    when(segA0Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenThrow(
        new RuntimeException("Failed to fetch latest stream offset for segment: " + segA0,
            new TimeoutException("Timeout fetching latest stream offset")));
    // segA1 provider works normally
    when(segA1Provider.fetchLatestStreamOffset(anySet(), anyLong())).thenReturn(Map.of(1, new LongMsgOffset(20)));

    when(segMngrA0.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    when(segMngrA1.getCurrentOffset()).thenReturn(new LongMsgOffset(10));
    // minimum ingestion lag is higher than threshold
    setupMinimumIngestionLag(segMngrA0, Long.MAX_VALUE);
    setupMinimumIngestionLag(segMngrA1, Long.MAX_VALUE);

    // segA0 has idle time below threshold, segA1 has idle time above threshold
    when(segMngrA0.getTimeSinceEventLastConsumedMs()).thenReturn(idleTimeoutMs - 1);
    when(segMngrA1.getTimeSinceEventLastConsumedMs()).thenReturn(idleTimeoutMs + 1);

    // segA0: timeout exception when fetching latest offset, but idle time is below threshold
    //         - should not be caught up (can't determine from offset, and idle time not exceeded)
    // segA1: can fetch latest offset (10 < 20), but idle time exceeds threshold
    //         - should be caught up due to idle timeout
    // Expected: 1 segment not caught up (segA0)
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 1);

    // Now make segA0 also exceed idle timeout - it should be caught up despite timeout exception
    when(segMngrA0.getTimeSinceEventLastConsumedMs()).thenReturn(idleTimeoutMs + 1);
    // Expected: 0 segments not caught up (both exceed idle timeout)
    assertEquals(statusChecker.getNumConsumingSegmentsNotReachedIngestionCriteria(), 0);
  }
}
