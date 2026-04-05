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
package org.apache.pinot.core.data.manager.offline;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class OfflineFreshnessTrackerTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";

  private ServerMetrics _serverMetrics;
  private AtomicLong _clock;
  private OfflineFreshnessTracker _tracker;

  @BeforeMethod
  public void setUp() {
    _serverMetrics = mock(ServerMetrics.class);
    _clock = new AtomicLong(100_000L);
    _tracker = new OfflineFreshnessTracker(_serverMetrics, TABLE_NAME, () -> true, _clock::get);
  }

  @AfterMethod
  public void tearDown() {
    if (_tracker != null) {
      _tracker.shutdown();
    }
  }

  @Test
  public void testSegmentLoadedUpdatesMaxEndTime() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    assertEquals(_tracker.getMaxEndTimeForPartition(0).longValue(), 50_000L);
    assertEquals(_tracker.getTrackedPartitionCount(), 1);
    assertEquals(_tracker.getTrackedSegmentCount(), 1);
  }

  @Test
  public void testMultipleSegmentsSamePartitionMaxWins() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    _tracker.segmentLoaded("seg2", 80_000L, 0);
    _tracker.segmentLoaded("seg3", 60_000L, 0);

    assertEquals(_tracker.getMaxEndTimeForPartition(0).longValue(), 80_000L);
    assertEquals(_tracker.getTrackedPartitionCount(), 1);
    assertEquals(_tracker.getTrackedSegmentCount(), 3);
  }

  @Test
  public void testMultiplePartitions() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    _tracker.segmentLoaded("seg2", 80_000L, 1);
    _tracker.segmentLoaded("seg3", 60_000L, 2);

    assertEquals(_tracker.getMaxEndTimeForPartition(0).longValue(), 50_000L);
    assertEquals(_tracker.getMaxEndTimeForPartition(1).longValue(), 80_000L);
    assertEquals(_tracker.getMaxEndTimeForPartition(2).longValue(), 60_000L);
    assertEquals(_tracker.getTrackedPartitionCount(), 3);
  }

  @Test
  public void testSegmentRemovedRecomputesCorrectly() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    _tracker.segmentLoaded("seg2", 80_000L, 0);
    _tracker.segmentLoaded("seg3", 60_000L, 0);

    // Remove the segment with the max end time
    _tracker.segmentRemoved("seg2");

    assertEquals(_tracker.getMaxEndTimeForPartition(0).longValue(), 60_000L);
    assertEquals(_tracker.getTrackedSegmentCount(), 2);
  }

  @Test
  public void testSegmentRemovedLastSegmentRemovesPartition() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    assertEquals(_tracker.getTrackedPartitionCount(), 1);

    _tracker.segmentRemoved("seg1");

    assertNull(_tracker.getMaxEndTimeForPartition(0));
    assertEquals(_tracker.getTrackedPartitionCount(), 0);
    assertEquals(_tracker.getTrackedSegmentCount(), 0);
  }

  @Test
  public void testSegmentRemovedUnknownSegmentIsNoop() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    _tracker.segmentRemoved("unknown");

    assertEquals(_tracker.getMaxEndTimeForPartition(0).longValue(), 50_000L);
    assertEquals(_tracker.getTrackedSegmentCount(), 1);
  }

  @Test
  public void testNonPartitionedTableUsesSentinel() {
    _tracker.segmentLoaded("seg1", 50_000L, OfflineFreshnessTracker.NON_PARTITIONED_SENTINEL);

    assertEquals(
        _tracker.getMaxEndTimeForPartition(OfflineFreshnessTracker.NON_PARTITIONED_SENTINEL).longValue(), 50_000L);
    assertEquals(_tracker.getTrackedPartitionCount(), 1);
  }

  @Test
  public void testPartitionIngestionDelayMs() {
    _clock.set(100_000L);
    _tracker.segmentLoaded("seg1", 80_000L, 0);

    assertEquals(_tracker.getPartitionIngestionDelayMs(0), 20_000L);
  }

  @Test
  public void testLagIncreasesOverTime() {
    _clock.set(100_000L);
    _tracker.segmentLoaded("seg1", 80_000L, 0);
    assertEquals(_tracker.getPartitionIngestionDelayMs(0), 20_000L);

    // Advance clock
    _clock.set(150_000L);
    assertEquals(_tracker.getPartitionIngestionDelayMs(0), 70_000L);
  }

  @Test
  public void testPartitionIngestionDelayMsNonNegative() {
    _clock.set(100_000L);
    // End time in the future
    _tracker.segmentLoaded("seg1", 200_000L, 0);

    assertEquals(_tracker.getPartitionIngestionDelayMs(0), 0L);
  }

  @Test
  public void testPartitionIngestionDelayMsNoData() {
    assertEquals(_tracker.getPartitionIngestionDelayMs(0), 0L);
  }

  @Test
  public void testTableIngestionDelayMsReflectsWorstPartition() {
    _clock.set(100_000L);
    _tracker.segmentLoaded("seg1", 80_000L, 0);  // lag = 20s
    _tracker.segmentLoaded("seg2", 50_000L, 1);  // lag = 50s (worst)
    _tracker.segmentLoaded("seg3", 90_000L, 2);  // lag = 10s

    // Table lag should be worst partition lag (partition 1: 100_000 - 50_000 = 50_000)
    assertEquals(_tracker.getTableIngestionDelayMs(), 50_000L);
  }

  @Test
  public void testTableIngestionDelayMsNoData() {
    assertEquals(_tracker.getTableIngestionDelayMs(), 0L);
  }

  @Test
  public void testPartitionGaugeCreatedOnFirstSegment() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);

    verify(_serverMetrics).setOrUpdatePartitionGauge(eq(TABLE_NAME), eq(0),
        eq(ServerGauge.OFFLINE_INGESTION_DELAY_MS), any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTableGaugeRegisteredAsCallbackOnFirstSegment() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);

    // Table-level gauge should be registered as a supplier (callback), not a static value
    verify(_serverMetrics).setOrUpdateTableGauge(eq(TABLE_NAME),
        eq(ServerGauge.OFFLINE_TABLE_INGESTION_DELAY_MS), any(Supplier.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTableGaugeNotReRegisteredOnSubsequentSegments() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    _tracker.segmentLoaded("seg2", 60_000L, 1);

    // setOrUpdateTableGauge should be called only once
    verify(_serverMetrics).setOrUpdateTableGauge(eq(TABLE_NAME),
        eq(ServerGauge.OFFLINE_TABLE_INGESTION_DELAY_MS), any(Supplier.class));
  }

  @Test
  public void testPartitionGaugeNotReCreatedOnSubsequentSegments() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    _tracker.segmentLoaded("seg2", 60_000L, 0);

    // setOrUpdatePartitionGauge should be called only once for partition 0
    verify(_serverMetrics).setOrUpdatePartitionGauge(eq(TABLE_NAME), eq(0),
        eq(ServerGauge.OFFLINE_INGESTION_DELAY_MS), any());
  }

  @Test
  public void testPartitionGaugeRemovedWhenLastSegmentRemoved() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    _tracker.segmentRemoved("seg1");

    verify(_serverMetrics).removePartitionGauge(TABLE_NAME, 0, ServerGauge.OFFLINE_INGESTION_DELAY_MS);
  }

  @Test
  public void testPartitionGaugeNotRemovedWhenSegmentsRemain() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    _tracker.segmentLoaded("seg2", 60_000L, 0);
    _tracker.segmentRemoved("seg1");

    verify(_serverMetrics, never()).removePartitionGauge(anyString(), anyInt(), any(ServerGauge.class));
  }

  @Test
  public void testShutdownRemovesAllGauges() {
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    _tracker.segmentLoaded("seg2", 60_000L, 1);
    _tracker.shutdown();

    verify(_serverMetrics).removePartitionGauge(TABLE_NAME, 0, ServerGauge.OFFLINE_INGESTION_DELAY_MS);
    verify(_serverMetrics).removePartitionGauge(TABLE_NAME, 1, ServerGauge.OFFLINE_INGESTION_DELAY_MS);
    verify(_serverMetrics).removeTableGauge(TABLE_NAME, ServerGauge.OFFLINE_TABLE_INGESTION_DELAY_MS);

    // Prevent double shutdown in tearDown
    _tracker = null;
  }

  @Test
  public void testServerNotReadyReturnsZeroLag() {
    OfflineFreshnessTracker tracker =
        new OfflineFreshnessTracker(_serverMetrics, TABLE_NAME, () -> false, _clock::get);
    try {
      tracker.segmentLoaded("seg1", 50_000L, 0);
      // State is still tracked (gauges are registered, segment data stored)
      assertEquals(tracker.getTrackedPartitionCount(), 1);
      // But lag methods return 0 while server is not ready
      assertEquals(tracker.getPartitionIngestionDelayMs(0), 0L);
      assertEquals(tracker.getTableIngestionDelayMs(), 0L);
    } finally {
      tracker.shutdown();
    }
  }

  @Test
  public void testSegmentReplacedWithLaterEndTime() {
    // Simulate segment replacement without explicit remove (e.g. CRC mismatch replace)
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    assertEquals(_tracker.getMaxEndTimeForPartition(0).longValue(), 50_000L);

    // Same segment name reloaded with later end time — should auto-clean old bookkeeping
    _tracker.segmentLoaded("seg1", 80_000L, 0);
    assertEquals(_tracker.getMaxEndTimeForPartition(0).longValue(), 80_000L);
    assertEquals(_tracker.getTrackedSegmentCount(), 1);
  }

  @Test
  public void testSegmentReplacedWithEarlierEndTime() {
    // Segment replaced with an earlier end time — max should decrease
    _tracker.segmentLoaded("seg1", 80_000L, 0);
    _tracker.segmentLoaded("seg2", 60_000L, 0);
    assertEquals(_tracker.getMaxEndTimeForPartition(0).longValue(), 80_000L);

    // Replace seg1 with earlier end time
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    assertEquals(_tracker.getMaxEndTimeForPartition(0).longValue(), 60_000L);
    assertEquals(_tracker.getTrackedSegmentCount(), 2);
  }

  @Test
  public void testSegmentReplacedMovesPartition() {
    // Segment moves from partition 0 to partition 1
    _tracker.segmentLoaded("seg1", 50_000L, 0);
    assertEquals(_tracker.getTrackedPartitionCount(), 1);

    _tracker.segmentLoaded("seg1", 50_000L, 1);
    // Partition 0 should be removed (no segments left), partition 1 should exist
    assertEquals(_tracker.getTrackedPartitionCount(), 1);
    assertNull(_tracker.getMaxEndTimeForPartition(0));
    assertEquals(_tracker.getMaxEndTimeForPartition(1).longValue(), 50_000L);
    assertEquals(_tracker.getTrackedSegmentCount(), 1);
  }

  @Test
  public void testConcurrentSegmentLoadRemove()
      throws Exception {
    int numThreads = 4;
    int segmentsPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    try {
      // Load segments from multiple threads
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < numThreads; t++) {
        final int threadId = t;
        futures.add(executor.submit(() -> {
          for (int i = 0; i < segmentsPerThread; i++) {
            String segmentName = "seg-" + threadId + "-" + i;
            _tracker.segmentLoaded(segmentName, 50_000L + i, threadId);
          }
        }));
      }
      for (Future<?> f : futures) {
        f.get();
      }

      assertEquals(_tracker.getTrackedPartitionCount(), numThreads);
      assertEquals(_tracker.getTrackedSegmentCount(), numThreads * segmentsPerThread);

      // Remove segments from multiple threads
      futures.clear();
      for (int t = 0; t < numThreads; t++) {
        final int threadId = t;
        futures.add(executor.submit(() -> {
          for (int i = 0; i < segmentsPerThread; i++) {
            _tracker.segmentRemoved("seg-" + threadId + "-" + i);
          }
        }));
      }
      for (Future<?> f : futures) {
        f.get();
      }

      assertEquals(_tracker.getTrackedSegmentCount(), 0);
      assertEquals(_tracker.getTrackedPartitionCount(), 0);
    } finally {
      executor.shutdownNow();
    }
  }
}
