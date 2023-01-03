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

import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IngestionDelayTrackerTest {

  private static final int TIMER_THREAD_TICK_INTERVAL_MS = 100;
  private static final int INITIAL_TIMER_THREAD_DELAY_MS = 100;

  private void sleepMs(long timeMs) {
    while (true) {
      try {
        Thread.sleep(timeMs);
      } catch (InterruptedException e) {
        continue;
      }
      break;
    }
  }

  private long getAgeOfSample(long sampleTime) {
    long ageOfSample = System.currentTimeMillis() - sampleTime;
    ageOfSample = ageOfSample > 0 ? ageOfSample : 0;
    return ageOfSample;
  }

  private IngestionDelayTracker createTracker(boolean enableAging) {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager realtimeTableDataManager = new RealtimeTableDataManager(null);
    IngestionDelayTracker ingestionDelayTracker =
        new IngestionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager);
    // With no samples, the time reported must be zero
    Assert.assertEquals(ingestionDelayTracker.getMaximumIngestionDelay(), 0);
    ingestionDelayTracker.setEnableAging(enableAging);
    return ingestionDelayTracker;
  }

  @Test
  public void testTrackerConstructors() {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager realtimeTableDataManager = new RealtimeTableDataManager(null);
    // Test regular constructor
    IngestionDelayTracker ingestionDelayTracker =
        new IngestionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager);
    Assert.assertEquals(ingestionDelayTracker.getMaximumIngestionDelay(), 0);
    ingestionDelayTracker.shutdown();
    // Test constructor with timer arguments
    ingestionDelayTracker =
        new IngestionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager, TIMER_THREAD_TICK_INTERVAL_MS, "", true, true);
    Assert.assertEquals(ingestionDelayTracker.getMaximumIngestionDelay(), 0);
    // Test we can start a different tracker with different name
    IngestionDelayTracker prefixedIngestionDelayTracker =
        new IngestionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager, TIMER_THREAD_TICK_INTERVAL_MS, "dummyPrefix", true, true);
    Assert.assertEquals(prefixedIngestionDelayTracker.getMaximumIngestionDelay(), 0);
    prefixedIngestionDelayTracker.shutdown();
    ingestionDelayTracker.shutdown();
    // Test bad timer args to the constructor
    try {
      ingestionDelayTracker =
          new IngestionDelayTracker(serverMetrics, "dummyTable_RT",
              realtimeTableDataManager, 0, "", true, true);
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

    IngestionDelayTracker ingestionDelayTracker = createTracker(false);

    // Test we follow a single partition up and down
    for (long i = 0; i <= maxTestDelay; i++) {
      ingestionDelayTracker.updateIngestionDelay(i, 0,
          partition0);
      Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay() == i);
    }

    // Test tracking down a measure for a given partition
    for (long i = maxTestDelay; i >= 0; i--) {
      ingestionDelayTracker.updateIngestionDelay(i, 0,
          partition0);
      Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay() == i);
    }

    // Make the current partition maximum
    ingestionDelayTracker.updateIngestionDelay(maxTestDelay, 0, partition0);

    // Bring up partition1 delay up and verify values
    for (long i = 0; i <= 2 * maxTestDelay; i++) {
      ingestionDelayTracker.updateIngestionDelay(i, 0, partition1);
      if (i >= maxTestDelay) {
        Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay() == i);
      } else {
        // for values of i <= mextTestDelay, the max should be that of partition0
        Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay() == maxTestDelay);
      }
    }

    // Bring down values of partition1 and verify values
    for (long i = 2 * maxTestDelay; i >= 0; i--) {
      ingestionDelayTracker.updateIngestionDelay(i, 0, partition1);
      if (i >= maxTestDelay) {
        Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay() == i);
      } else {
        // for values of i <= mextTestDelay, the max should be that of partition0
        Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay() == maxTestDelay);
      }
    }

    ingestionDelayTracker.shutdown();
    Assert.assertTrue(true);
  }

  @Test
  public void testRecordIngestionDelayWithAging() {
    final int partition0 = 0;
    final long partition0Delay0 = 1000;
    final long partition0Delay1 = 10; // record lower delay to make sure max gets reduced
    final int partition1 = 1;
    final long partition1Delay0 = 11; // Record something slightly higher than previous max
    final long partition1Delay1 = 8;  // Record something lower so that partition0 is the current max again
    final long sleepMs = 500;

    IngestionDelayTracker ingestionDelayTracker = createTracker(true);

    // With samples for a single partition, time reported should be at least sample plus aging
    long partition0SampleTime = System.currentTimeMillis();
    ingestionDelayTracker.updateIngestionDelay(partition0Delay0, partition0SampleTime, partition0);
    long agingOfSample = getAgeOfSample(partition0SampleTime);
    Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay()
        >= (partition0Delay0 + agingOfSample));

    // Test we are aging the delay measures by sleeping some time and verifying that time was added to the measure
    sleepMs(sleepMs);
    agingOfSample = getAgeOfSample(partition0SampleTime);
    Assert.assertTrue(agingOfSample >= sleepMs);
    Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay()
        >= (partition0Delay0 + agingOfSample));

    // Add a new value below max and verify we are tracking and verify that the new maximum is being tracked.
    partition0SampleTime = System.currentTimeMillis();
    ingestionDelayTracker.updateIngestionDelay(partition0Delay1, partition0SampleTime,
        partition0);
    agingOfSample = getAgeOfSample(partition0SampleTime);
    long maxDelay = ingestionDelayTracker.getMaximumIngestionDelay();
    Assert.assertTrue(maxDelay >= (partition0Delay1 + agingOfSample));

    // Now try setting a new maximum for another partition
    long partition1SampleTime = System.currentTimeMillis();
    ingestionDelayTracker.updateIngestionDelay(partition1Delay0, partition1SampleTime,
        partition1);
    agingOfSample = getAgeOfSample(partition1SampleTime);
    maxDelay = ingestionDelayTracker.getMaximumIngestionDelay();
    Assert.assertTrue(maxDelay >= (partition1Delay0 + agingOfSample));

    // Now try to set partition 1 below partition 0 and confirm that partition 0 becomes the new max
    partition1SampleTime = System.currentTimeMillis();
    ingestionDelayTracker.updateIngestionDelay(partition1Delay1, partition1SampleTime,
        partition1);
    // Partition 0 must be the new max
    agingOfSample = getAgeOfSample(partition0SampleTime);
    maxDelay = ingestionDelayTracker.getMaximumIngestionDelay();
    Assert.assertTrue(maxDelay >= (partition0Delay1 + agingOfSample));

    // Test setting partition 1 to zero, maximum should still be partition 0 last sample
    partition1SampleTime = System.currentTimeMillis();
    ingestionDelayTracker.updateIngestionDelay(0, partition1SampleTime,
        partition1);
    agingOfSample = getAgeOfSample(partition0SampleTime);
    maxDelay = ingestionDelayTracker.getMaximumIngestionDelay();
    Assert.assertTrue(maxDelay >= (partition0Delay1 + agingOfSample));

    ingestionDelayTracker.shutdown();
  }

  @Test
  public void testStopTrackingIngestionDelay() {
    final long maxTestDelay = 100;
    final int maxPartition = 100;

    IngestionDelayTracker ingestionDelayTracker = createTracker(false);
    // Record a number of partitions with delay equal to partition id
    for (int partitionGroupId = 0; partitionGroupId <= maxTestDelay; partitionGroupId++) {
      ingestionDelayTracker.updateIngestionDelay(partitionGroupId, 0, partitionGroupId);
      Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay() == partitionGroupId);
    }
    // Verify that as we remove partitions the next available maximum takes over

    for (int partitionGroupId = maxPartition; partitionGroupId >= 0; partitionGroupId--) {
      Assert.assertTrue(ingestionDelayTracker.getMaximumIngestionDelay() == partitionGroupId);
      ingestionDelayTracker.stopTrackingPartitionIngestionDelay((int) partitionGroupId);
    }
    Assert.assertTrue(true);
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
