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
package org.apache.pinot.core.data.manager.realtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class IngestionDelayTrackerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final int TIMER_THREAD_TICK_INTERVAL_MS = 100;

  private final ServerMetrics _serverMetrics = mock(ServerMetrics.class);
  private final RealtimeTableDataManager _realtimeTableDataManager = mock(RealtimeTableDataManager.class);

  private IngestionDelayTracker createTracker() {
    IngestionDelayTracker ingestionDelayTracker =
        new IngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager, () -> true);
    // With no samples, the time reported must be zero
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 0);
    return ingestionDelayTracker;
  }

  @Test
  public void testTrackerConstructors() {
    // Test regular constructor
    IngestionDelayTracker ingestionDelayTracker =
        new IngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager, () -> true);

    Clock clock = Clock.systemUTC();
    ingestionDelayTracker.setClock(clock);

    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);
    ingestionDelayTracker.shutdown();
    // Test constructor with timer arguments
    ingestionDelayTracker = new IngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager,
        TIMER_THREAD_TICK_INTERVAL_MS, () -> true);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);
    // Test bad timer args to the constructor
    try {
      new IngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager, 0, () -> true);
      Assert.fail("Must have asserted due to invalid arguments"); // Constructor must assert
    } catch (Exception e) {
      if ((e instanceof NullPointerException) || !(e instanceof RuntimeException)) {
        Assert.fail(String.format("Unexpected exception: %s:%s", e.getClass(), e.getMessage()));
      }
    }
  }

  @Test
  public void testRecordIngestionDelayWithNoAging() {
    final long maxTestDelay = 100;
    final int partition0 = 0;
    final String segment0 = new LLCSegmentName(RAW_TABLE_NAME, partition0, 0, 123).getSegmentName();
    final int partition1 = 1;
    final String segment1 = new LLCSegmentName(RAW_TABLE_NAME, partition1, 0, 234).getSegmentName();

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples dont age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);

    // Test we follow a single partition up and down
    for (long ingestionTimeMs = 0; ingestionTimeMs <= maxTestDelay; ingestionTimeMs++) {
      long firstStreamIngestionTimeMs = ingestionTimeMs + 1;
      ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, ingestionTimeMs, firstStreamIngestionTimeMs,
          null, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
          clock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0),
          clock.millis() - firstStreamIngestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);
    }

    // Test tracking down a measure for a given partition
    for (long ingestionTimeMs = maxTestDelay; ingestionTimeMs >= 0; ingestionTimeMs--) {
      long firstStreamIngestionTimeMs = ingestionTimeMs + 1;
      ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, ingestionTimeMs, firstStreamIngestionTimeMs,
          null, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
          clock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0),
          clock.millis() - (ingestionTimeMs + 1));
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);
    }

    // Make the current partition maximum
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, maxTestDelay, maxTestDelay, null, null);

    // Bring up partition1 delay up and verify values
    for (long ingestionTimeMs = 0; ingestionTimeMs <= 2 * maxTestDelay; ingestionTimeMs++) {
      long firstStreamIngestionTimeMs = ingestionTimeMs + 1;
      ingestionDelayTracker.updateIngestionMetrics(segment1, partition1, ingestionTimeMs, firstStreamIngestionTimeMs,
          null, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1),
          clock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition1),
          clock.millis() - firstStreamIngestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), ingestionTimeMs);
    }

    // Bring down values of partition1 and verify values
    for (long ingestionTimeMs = 2 * maxTestDelay; ingestionTimeMs >= 0; ingestionTimeMs--) {
      long firstStreamIngestionTimeMs = ingestionTimeMs + 1;
      ingestionDelayTracker.updateIngestionMetrics(segment1, partition1, ingestionTimeMs, firstStreamIngestionTimeMs,
          null, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1),
          clock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition1),
          clock.millis() - firstStreamIngestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), ingestionTimeMs);
    }

    ingestionDelayTracker = createTracker();
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, 1, 1, null, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition0), 0);

    ingestionDelayTracker.shutdown();
    Assert.assertTrue(true);
  }

  @Test
  public void testRecordIngestionDelayWithAging() {
    final int partition0 = 0;
    final String segment0 = new LLCSegmentName(RAW_TABLE_NAME, partition0, 0, 123).getSegmentName();
    final long partition0Delay0 = 1000;
    final long partition0Delay1 = 10; // record lower delay to make sure max gets reduced
    final long partition0Offset0Ms = 300;
    final long partition0Offset1Ms = 1000;
    final int partition1 = 1;
    final String segment1 = new LLCSegmentName(RAW_TABLE_NAME, partition1, 0, 234).getSegmentName();
    final long partition1Delay0 = 11;
    final long partition1Offset0Ms = 150;

    IngestionDelayTracker ingestionDelayTracker = createTracker();

    // With samples for a single partition, test that sample is aged as expected
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);
    long ingestionTimeMs = clock.millis() - partition0Delay0;
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, ingestionTimeMs, ingestionTimeMs, null, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0), partition0Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0), partition0Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    // Advance clock and test aging
    Clock offsetClock = Clock.offset(clock, Duration.ofMillis(partition0Offset0Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
        (partition0Delay0 + partition0Offset0Ms));
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0),
        (partition0Delay0 + partition0Offset0Ms));
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    ingestionTimeMs = offsetClock.millis() - partition0Delay1;
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, ingestionTimeMs, ingestionTimeMs, null, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0), partition0Delay1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0), partition0Delay1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    // Add some offset to the last sample and make sure we age that measure properly
    offsetClock = Clock.offset(offsetClock, Duration.ofMillis(partition0Offset1Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
        (partition0Delay1 + partition0Offset1Ms));

    ingestionTimeMs = offsetClock.millis() - partition1Delay0;
    ingestionDelayTracker.updateIngestionMetrics(segment1, partition1, ingestionTimeMs, ingestionTimeMs, null, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1), partition1Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition1), partition1Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), ingestionTimeMs);

    // Add some offset to the last sample and make sure we age that measure properly
    offsetClock = Clock.offset(offsetClock, Duration.ofMillis(partition1Offset0Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1),
        (partition1Delay0 + partition1Offset0Ms));

    ingestionDelayTracker.shutdown();
  }

  @Test
  public void testStopTrackingIngestionDelay() {
    final long maxTestDelay = 100;
    final int maxPartition = 100;

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);

    // Record a number of partitions with delay equal to partition id
    for (int partitionId = 0; partitionId <= maxTestDelay; partitionId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, 123).getSegmentName();
      long ingestionTimeMs = clock.millis() - partitionId;
      ingestionDelayTracker.updateIngestionMetrics(segmentName, partitionId, ingestionTimeMs, ingestionTimeMs, null,
          null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionId), partitionId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partitionId), partitionId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partitionId), ingestionTimeMs);
    }
    for (int partitionId = maxPartition; partitionId >= 0; partitionId--) {
      ingestionDelayTracker.stopTrackingPartitionIngestionDelay(partitionId);
    }
    for (int partitionId = 0; partitionId <= maxTestDelay; partitionId++) {
      // Untracked partitions must return 0
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionId), 0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partitionId), 0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partitionId), Long.MIN_VALUE);
    }
  }

  @Test
  public void testStopTrackingIngestionDelayWithSegment() {
    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);

    String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, 123).getSegmentName();
    long ingestionTimeMs = clock.millis() - 10;
    ingestionDelayTracker.updateIngestionMetrics(segmentName, 0, ingestionTimeMs, ingestionTimeMs, null, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 10);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(0), 10);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), ingestionTimeMs);

    ingestionDelayTracker.stopTrackingPartitionIngestionDelay(segmentName);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);

    // Should not update metrics for removed segment
    ingestionDelayTracker.updateIngestionMetrics(segmentName, 0, ingestionTimeMs, ingestionTimeMs, null, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);
  }

  @Test
  public void testShutdown() {
    final long maxTestDelay = 100;

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);

    // Test Shutdown with partitions active
    for (int partitionId = 0; partitionId <= maxTestDelay; partitionId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, 123).getSegmentName();
      long ingestionTimeMs = clock.millis() - partitionId;
      ingestionDelayTracker.updateIngestionMetrics(segmentName, partitionId, ingestionTimeMs, ingestionTimeMs, null,
          null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionId), partitionId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partitionId), partitionId);
    }
    ingestionDelayTracker.shutdown();

    // Test shutdown with no partitions
    ingestionDelayTracker = createTracker();
    ingestionDelayTracker.shutdown();
  }

  @Test
  public void testRecordIngestionDelayOffset() {
    final int partition0 = 0;
    final String segment0 = new LLCSegmentName(RAW_TABLE_NAME, partition0, 0, 123).getSegmentName();
    final int partition1 = 1;
    final String segment1 = new LLCSegmentName(RAW_TABLE_NAME, partition1, 0, 234).getSegmentName();

    IngestionDelayTracker ingestionDelayTracker = createTracker();

    // Test tracking offset lag for a single partition
    StreamPartitionMsgOffset msgOffset0 = new LongMsgOffset(50);
    StreamPartitionMsgOffset latestOffset0 = new LongMsgOffset(150);
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, Long.MIN_VALUE, Long.MIN_VALUE, msgOffset0,
        latestOffset0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition0), 100);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionUpstreamOffset(partition0), 150);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionConsumingOffset(partition0), 50);

    // Test tracking offset lag for another partition
    StreamPartitionMsgOffset msgOffset1 = new LongMsgOffset(50);
    StreamPartitionMsgOffset latestOffset1 = new LongMsgOffset(150);
    ingestionDelayTracker.updateIngestionMetrics(segment1, partition1, Long.MIN_VALUE, Long.MIN_VALUE, msgOffset1,
        latestOffset1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition1), 100);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionUpstreamOffset(partition1), 150);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionConsumingOffset(partition1), 50);

    // Update offset lag for partition0
    msgOffset0 = new LongMsgOffset(150);
    latestOffset0 = new LongMsgOffset(200);
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, Long.MIN_VALUE, Long.MIN_VALUE, msgOffset0,
        latestOffset0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition0), 50);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionUpstreamOffset(partition0), 200);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionConsumingOffset(partition0), 150);

    ingestionDelayTracker.shutdown();
  }
}
