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
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class IngestionDelayTrackerTest {
  private static final String REALTIME_TABLE_NAME = "testTable_REALTIME";
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
      new IngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager,
          0, () -> true);
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
    final int partition1 = 1;

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples dont age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);

    // Test we follow a single partition up and down
    for (long i = 0; i <= maxTestDelay; i++) {
      long firstStreamIngestionTimeMs = i + 1;
      ingestionDelayTracker.updateIngestionDelay(i, firstStreamIngestionTimeMs, partition0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0), clock.millis() - i);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0),
          clock.millis() - firstStreamIngestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), i);
    }

    // Test tracking down a measure for a given partition
    for (long i = maxTestDelay; i >= 0; i--) {
      ingestionDelayTracker.updateIngestionDelay(i, (i + 1), partition0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0), clock.millis() - i);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0),
          clock.millis() - (i + 1));
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), i);
    }

    // Make the current partition maximum
    ingestionDelayTracker.updateIngestionDelay(maxTestDelay, maxTestDelay, partition0);

    // Bring up partition1 delay up and verify values
    for (long i = 0; i <= 2 * maxTestDelay; i++) {
      ingestionDelayTracker.updateIngestionDelay(i, (i + 1), partition1);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1), clock.millis() - i);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition1),
          clock.millis() - (i + 1));
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), i);
    }

    // Bring down values of partition1 and verify values
    for (long i = 2 * maxTestDelay; i >= 0; i--) {
      ingestionDelayTracker.updateIngestionDelay(i, (i + 1), partition1);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1), clock.millis() - i);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition1),
          clock.millis() - (i + 1));
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), i);
    }

    ingestionDelayTracker.shutdown();
    Assert.assertTrue(true);
  }

  @Test
  public void testRecordIngestionDelayWithAging() {
    final int partition0 = 0;
    final long partition0Delay0 = 1000;
    final long partition0Delay1 = 10; // record lower delay to make sure max gets reduced
    final long partition0Offset0Ms = 300;
    final long partition0Offset1Ms = 1000;
    final int partition1 = 1;
    final long partition1Delay0 = 11;
    final long partition1Offset0Ms = 150;

    IngestionDelayTracker ingestionDelayTracker = createTracker();

    // With samples for a single partition, test that sample is aged as expected
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);
    long ingestionTimeMs = (clock.millis() - partition0Delay0);
    ingestionDelayTracker.updateIngestionDelay(ingestionTimeMs,
        (clock.millis() - partition0Delay0), partition0);
    ingestionDelayTracker.updateIngestionDelay((clock.millis() - partition0Delay0), (clock.millis() - partition0Delay0),
        partition0);
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

    ingestionTimeMs = (offsetClock.millis() - partition0Delay1);
    ingestionDelayTracker.updateIngestionDelay(ingestionTimeMs,
        (offsetClock.millis() - partition0Delay1), partition0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0), partition0Delay1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0), partition0Delay1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    // Add some offset to the last sample and make sure we age that measure properly
    offsetClock = Clock.offset(offsetClock, Duration.ofMillis(partition0Offset1Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
        (partition0Delay1 + partition0Offset1Ms));

    ingestionTimeMs = (offsetClock.millis() - partition1Delay0);
    ingestionDelayTracker.updateIngestionDelay(ingestionTimeMs,
        (offsetClock.millis() - partition1Delay0), partition1);
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
    for (int partitionGroupId = 0; partitionGroupId <= maxTestDelay; partitionGroupId++) {
      long ingestionTimeMs = (clock.millis() - partitionGroupId);
      ingestionDelayTracker.updateIngestionDelay(ingestionTimeMs, ingestionTimeMs, partitionGroupId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionGroupId), partitionGroupId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partitionGroupId),
          partitionGroupId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partitionGroupId), ingestionTimeMs);
    }
    for (int partitionGroupId = maxPartition; partitionGroupId >= 0; partitionGroupId--) {
      ingestionDelayTracker.stopTrackingPartitionIngestionDelay(partitionGroupId);
    }
    for (int partitionGroupId = 0; partitionGroupId <= maxTestDelay; partitionGroupId++) {
      // Untracked partitions must return 0
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionGroupId), 0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partitionGroupId), 0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(
          partitionGroupId), Long.MIN_VALUE);
    }
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
    for (int partitionGroupId = 0; partitionGroupId <= maxTestDelay; partitionGroupId++) {
      ingestionDelayTracker.updateIngestionDelay((clock.millis() - partitionGroupId),
          (clock.millis() - partitionGroupId), partitionGroupId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionGroupId), partitionGroupId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partitionGroupId),
          partitionGroupId);
    }
    ingestionDelayTracker.shutdown();

    // Test shutdown with no partitions
    ingestionDelayTracker = createTracker();
    ingestionDelayTracker.shutdown();
  }
}
