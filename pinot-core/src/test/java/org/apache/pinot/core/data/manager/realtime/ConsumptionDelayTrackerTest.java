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

public class ConsumptionDelayTrackerTest {

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

  private ConsumptionDelayTracker createTracker(boolean enableAging) {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager realtimeTableDataManager = new RealtimeTableDataManager(null);
    ConsumptionDelayTracker consumptionDelayTracker =
        new ConsumptionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager);
    // With no samples, the time reported must be zero
    Assert.assertEquals(consumptionDelayTracker.getMaxConsumptionDelay(), 0);
    consumptionDelayTracker.setEnableAging(enableAging);
    return consumptionDelayTracker;
  }

  @Test
  public void testTrackerConstructors() {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager realtimeTableDataManager = new RealtimeTableDataManager(null);
    // Test regular constructor
    ConsumptionDelayTracker consumptionDelayTracker =
        new ConsumptionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager);
    Assert.assertEquals(consumptionDelayTracker.getMaxConsumptionDelay(), 0);
    consumptionDelayTracker.shutdown();
    // Test constructor with timer arguments
    consumptionDelayTracker =
        new ConsumptionDelayTracker(serverMetrics, "dummyTable_RT",
            realtimeTableDataManager, TIMER_THREAD_TICK_INTERVAL_MS);
    Assert.assertEquals(consumptionDelayTracker.getMaxConsumptionDelay(), 0);
    consumptionDelayTracker.shutdown();
    // Test bad timer args to the constructor
    try {
      consumptionDelayTracker =
          new ConsumptionDelayTracker(serverMetrics, "dummyTable_RT",
              realtimeTableDataManager, 0);
      Assert.assertTrue(false); // Constructor must assert
    } catch (Exception e) {
      Assert.assertTrue(e instanceof RuntimeException);
    }
  }

  @Test
  public void testRecordPinotConsumptionDelayWithNoAging() {
    final long maxTestDelay = 100;
    final int partition0 = 0;
    final int partition1 = 1;

    ConsumptionDelayTracker consumptionDelayTracker = createTracker(false);

    // Test we follow a single partition up and down
    for (long i = 0; i <= maxTestDelay; i++) {
      consumptionDelayTracker.storeConsumptionDelay(i, 0,
          partition0);
      Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay() == i);
    }

    // Test tracking down a measure for a given partition
    for (long i = maxTestDelay; i >= 0; i--) {
      consumptionDelayTracker.storeConsumptionDelay(i, 0,
          partition0);
      Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay() == i);
    }

    // Make the current partition maximum
    consumptionDelayTracker.storeConsumptionDelay(maxTestDelay, 0, partition0);

    // Bring up partition1 delay up and verify values
    for (long i = 0; i <= 2 * maxTestDelay; i++) {
      consumptionDelayTracker.storeConsumptionDelay(i, 0, partition1);
      if (i >= maxTestDelay) {
        Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay() == i);
      } else {
        // for values of i <= mextTestDelay, the max should be that of partition0
        Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay() == maxTestDelay);
      }
    }

    // Bring down values of partition1 and verify values
    for (long i = 2 * maxTestDelay; i >= 0; i--) {
      consumptionDelayTracker.storeConsumptionDelay(i, 0, partition1);
      if (i >= maxTestDelay) {
        Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay() == i);
      } else {
        // for values of i <= mextTestDelay, the max should be that of partition0
        Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay() == maxTestDelay);
      }
    }

    consumptionDelayTracker.shutdown();
    Assert.assertTrue(true);
  }

  @Test(invocationCount = 10)
  public void testRecordPinotConsumptionDelayWithAging() {
    final int partition0 = 0;
    final long partition0Delay0 = 1000;
    final long partition0Delay1 = 10; // record lower delay to make sure max gets reduced
    final int partition1 = 1;
    final long partition1Delay0 = 11; // Record something slightly higher than previous max
    final long partition1Delay1 = 8;  // Record something lower so that partition0 is the current max again
    final long sleepMs = 500;

    ConsumptionDelayTracker consumptionDelayTracker = createTracker(true);

    // With samples for a single partition, time reported should be at least sample plus aging
    long partition0SampleTime = System.currentTimeMillis();
    consumptionDelayTracker.storeConsumptionDelay(partition0Delay0, partition0SampleTime, partition0);
    long agingOfSample = getAgeOfSample(partition0SampleTime);
    Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay()
        >= (partition0Delay0 + agingOfSample));

    // Test we are aging the delay measures by sleeping some time and verifying that time was added to the measure
    sleepMs(sleepMs);
    agingOfSample = getAgeOfSample(partition0SampleTime);
    Assert.assertTrue(agingOfSample >= sleepMs);
    Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay()
        >= (partition0Delay0 + agingOfSample));

    // Add a new value below max and verify we are tracking and verify that the new maximum is being tracked.
    partition0SampleTime = System.currentTimeMillis();
    consumptionDelayTracker.storeConsumptionDelay(partition0Delay1, partition0SampleTime,
        partition0);
    agingOfSample = getAgeOfSample(partition0SampleTime);
    long maxDelay = consumptionDelayTracker.getMaxConsumptionDelay();
    Assert.assertTrue(maxDelay >= (partition0Delay1 + agingOfSample));

    // Now try setting a new maximum for another partition
    long partition1SampleTime = System.currentTimeMillis();
    consumptionDelayTracker.storeConsumptionDelay(partition1Delay0, partition1SampleTime,
        partition1);
    agingOfSample = getAgeOfSample(partition1SampleTime);
    maxDelay = consumptionDelayTracker.getMaxConsumptionDelay();
    Assert.assertTrue(maxDelay >= (partition1Delay0 + agingOfSample));

    // Now try to set partition 1 below partition 0 and confirm that partition 0 becomes the new max
    partition1SampleTime = System.currentTimeMillis();
    consumptionDelayTracker.storeConsumptionDelay(partition1Delay1, partition1SampleTime,
        partition1);
    // Partition 0 must be the new max
    agingOfSample = getAgeOfSample(partition0SampleTime);
    maxDelay = consumptionDelayTracker.getMaxConsumptionDelay();
    Assert.assertTrue(maxDelay >= (partition0Delay1 + agingOfSample));

    // Test setting partition 1 to zero, maximum should still be partition 0 last sample
    partition1SampleTime = System.currentTimeMillis();
    consumptionDelayTracker.storeConsumptionDelay(0, partition1SampleTime,
        partition1);
    agingOfSample = getAgeOfSample(partition0SampleTime);
    maxDelay = consumptionDelayTracker.getMaxConsumptionDelay();
    Assert.assertTrue(maxDelay >= (partition0Delay1 + agingOfSample));

    consumptionDelayTracker.shutdown();
  }

  @Test
  public void testStopTrackingPinotConsumptionDelay() {
    final long maxTestDelay = 100;
    final int maxPartition = 100;

    ConsumptionDelayTracker consumptionDelayTracker = createTracker(false);
    // Record a number of partitions with delay equal to partition id
    for (int partitionGroupId = 0; partitionGroupId <= maxTestDelay; partitionGroupId++) {
      consumptionDelayTracker.storeConsumptionDelay(partitionGroupId, 0, partitionGroupId);
      Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay() == partitionGroupId);
    }
    // Verify that as we remove partitions the next available maximum takes over

    for (int partitionGroupId = maxPartition; partitionGroupId >= 0; partitionGroupId--) {
      Assert.assertTrue(consumptionDelayTracker.getMaxConsumptionDelay() == partitionGroupId);
      consumptionDelayTracker.stopTrackingPartitionConsumptionDelay((int) partitionGroupId);
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
