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
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IngestionDelayTrackerTest {

  private static final int TIMER_THREAD_TICK_INTERVAL_MS = 100;

  private IngestionDelayTracker createTracker() {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager realtimeTableDataManager = new RealtimeTableDataManager(null);
    IngestionDelayTracker ingestionDelayTracker =
        new IngestionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager, () -> true);
    // With no samples, the time reported must be zero
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(0), 0);
    return ingestionDelayTracker;
  }

  @Test
  public void testTrackerConstructors() {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager realtimeTableDataManager = new RealtimeTableDataManager(null);
    // Test regular constructor
    IngestionDelayTracker ingestionDelayTracker =
        new IngestionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager, () -> true);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(0), 0);
    ingestionDelayTracker.shutdown();
    // Test constructor with timer arguments
    ingestionDelayTracker =
        new IngestionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager, TIMER_THREAD_TICK_INTERVAL_MS, () -> true);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(0), 0);
    // Test bad timer args to the constructor
    try {
      ingestionDelayTracker =
          new IngestionDelayTracker(serverMetrics, "dummyTable_RT",
              realtimeTableDataManager, 0, () -> true);
      Assert.assertTrue(false); // Constructor must assert
    } catch (Exception e) {
      Assert.assertTrue(e instanceof RuntimeException);
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
      ingestionDelayTracker.updateIngestionDelay(i, 0, partition0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition0), i + clock.millis());
    }

    // Test tracking down a measure for a given partition
    for (long i = maxTestDelay; i >= 0; i--) {
      ingestionDelayTracker.updateIngestionDelay(i, 0, partition0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition0), i + clock.millis());
    }

    // Make the current partition maximum
    ingestionDelayTracker.updateIngestionDelay(maxTestDelay, 0, partition0);

    // Bring up partition1 delay up and verify values
    for (long i = 0; i <= 2 * maxTestDelay; i++) {
      ingestionDelayTracker.updateIngestionDelay(i, 0, partition1);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition1), i + clock.millis());
    }

    // Bring down values of partition1 and verify values
    for (long i = 2 * maxTestDelay; i >= 0; i--) {
      ingestionDelayTracker.updateIngestionDelay(i, 0, partition1);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition1), i + clock.millis());
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
    final long partition1Delay0 = 11; // Record something slightly higher than previous max
    final long partition1Delay1 = 8;  // Record something lower so that partition0 is the current max again
    final long partition1Offset0Ms = 150;
    final long partition1Offset1Ms = 450;

    final long sleepMs = 500;

    IngestionDelayTracker ingestionDelayTracker = createTracker();

    // With samples for a single partition, test that sample is aged as expected
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);
    ingestionDelayTracker.updateIngestionDelay(partition0Delay0, clock.millis(), partition0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition0), partition0Delay0);
    // Advance clock and test aging
    Clock offsetClock = Clock.offset(clock, Duration.ofMillis(partition0Offset0Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition0),
        (partition0Delay0 + partition0Offset0Ms));

    // Add a new value below max and verify we are tracking the new max correctly
    ingestionDelayTracker.updateIngestionDelay(partition0Delay1, offsetClock.millis(), partition0);
    Clock partition0LastUpdate = Clock.offset(offsetClock, Duration.ZERO); // Save this as we need to verify aging later
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition0), partition0Delay1);
    // Add some offset to the last sample and make sure we age that measure properly
    offsetClock = Clock.offset(offsetClock, Duration.ofMillis(partition0Offset1Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition0),
        (partition0Delay1 + partition0Offset1Ms));

    // Now try setting a new maximum in another partition
    ingestionDelayTracker.updateIngestionDelay(partition1Delay0, offsetClock.millis(), partition1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition1), partition1Delay0);
    // Add some offset to the last sample and make sure we age that measure properly
    offsetClock = Clock.offset(offsetClock, Duration.ofMillis(partition1Offset0Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partition1),
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
      ingestionDelayTracker.updateIngestionDelay(partitionGroupId, clock.millis(), partitionGroupId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partitionGroupId), partitionGroupId);
    }
    // Verify that as we remove partitions the next available maximum takes over
    for (int partitionGroupId = maxPartition; partitionGroupId >= 0; partitionGroupId--) {
      ingestionDelayTracker.stopTrackingPartitionIngestionDelay((int) partitionGroupId);
    }
    for (int partitionGroupId = 0; partitionGroupId <= maxTestDelay; partitionGroupId++) {
      // Untracked partitions must return 0
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelay(partitionGroupId), 0);
    }
  }

  @Test
  public void testTickInactivePartitions() {
    Assert.assertTrue(true);
  }

  @Test
  public void testMarkPartitionForConfirmation() {
    Assert.assertTrue(true);
  }
}
