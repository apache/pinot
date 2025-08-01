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
package org.apache.pinot.spi.accounting;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class WorkloadBudgetManagerTest {
  PinotConfiguration _config;
  long _enforcementWindowMs = 10_000L; // 10 seconds

  @BeforeClass
  void setup() {
    _config = new PinotConfiguration();
    _config.setProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION, true);
    _config.setProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENFORCEMENT_WINDOW_MS, _enforcementWindowMs);
  }

  @Test
  void testAddOrUpdateAndRetrieveBudget() {
    WorkloadBudgetManager manager = new WorkloadBudgetManager(_config);
    manager.addOrUpdateWorkload("test-workload", 1_000_000L, 1_000_000L);

    WorkloadBudgetManager.BudgetStats stats = manager.getRemainingBudgetForWorkload("test-workload");
    assertEquals(1_000_000L, stats._cpuRemaining);
    assertEquals(1_000_000L, stats._memoryRemaining);
  }

  @Test
  void testTryChargeWithoutBudget() {
    WorkloadBudgetManager mgr = new WorkloadBudgetManager(_config);
    WorkloadBudgetManager.BudgetStats stats = mgr.tryCharge("unknown-workload", 100L, 100L);
    assertEquals(Long.MAX_VALUE, stats._cpuRemaining);
    assertEquals(Long.MAX_VALUE, stats._memoryRemaining);
  }

  @Test
  void testBudgetResetAfterInterval() throws InterruptedException {
    WorkloadBudgetManager mgr = new WorkloadBudgetManager(_config);
    mgr.addOrUpdateWorkload("reset-test", 1_000_000L, 1_000_000L);
    mgr.tryCharge("reset-test", 500_000L, 500_000L);

    // Ensure budget is charged
    WorkloadBudgetManager.BudgetStats usedStats = mgr.getRemainingBudgetForWorkload("reset-test");
    assertEquals(500_000L, usedStats._cpuRemaining);
    assertEquals(500_000L, usedStats._memoryRemaining);

    // Wait for reset window (configured as 10 seconds)
    Thread.sleep(_enforcementWindowMs + 1000L);

    // Check if reset occurred
    WorkloadBudgetManager.BudgetStats resetStats = mgr.getRemainingBudgetForWorkload("reset-test");
    assertEquals(1_000_000L, resetStats._cpuRemaining);
    assertEquals(1_000_000L, resetStats._memoryRemaining);
  }

  @Test
  void testConcurrentTryChargeSingleWorkload() throws InterruptedException {
    WorkloadBudgetManager manager = new WorkloadBudgetManager(_config);
    String workload = "concurrent-test";
    long initialCpuBudget = 2_000_000L;
    long initialMemBudget = 2_000_000L;
    manager.addOrUpdateWorkload(workload, initialCpuBudget, initialMemBudget);

    int numThreads = 20;
    int chargesPerThread = 1000;
    long cpuChargePerCall = 10L;
    long memChargePerCall = 10L;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        for (int j = 0; j < chargesPerThread; j++) {
          manager.tryCharge(workload, cpuChargePerCall, memChargePerCall);
        }
        latch.countDown();
      });
    }

    latch.await();
    executor.shutdown();

    long totalCpuCharged = numThreads * chargesPerThread * cpuChargePerCall;
    long totalMemCharged = numThreads * chargesPerThread * memChargePerCall;

    WorkloadBudgetManager.BudgetStats remaining = manager.getRemainingBudgetForWorkload(workload);
    assertEquals(initialCpuBudget - totalCpuCharged, remaining._cpuRemaining,
        "CPU budget mismatch after concurrent updates");
    assertEquals(initialMemBudget - totalMemCharged, remaining._memoryRemaining,
        "Memory budget mismatch after concurrent updates");
  }

  @Test
  void testCanAdmitQuery() {
    WorkloadBudgetManager manager = new WorkloadBudgetManager(_config);
    // Scenario 1: No budget configured -> should admit
    assertTrue(manager.canAdmitQuery("unconfigured-workload"),
        "Workload without budget should be admitted");

    // Scenario 2: Budget configured with non-zero remaining -> should admit
    String activeWorkload = "active-workload";
    manager.addOrUpdateWorkload(activeWorkload, 100L, 200L);
    assertTrue(manager.canAdmitQuery(activeWorkload), "Workload with available budget should be admitted");

    // Scenario 3: Budget depleted -> should reject
    String depletedWorkload = "depleted-workload";
    manager.addOrUpdateWorkload(depletedWorkload, 50L, 50L);
    manager.tryCharge(depletedWorkload, 50L, 50L); // deplete
    assertFalse(manager.canAdmitQuery(depletedWorkload),
        "Workload with depleted budget should be rejected");

    // Scenario 4: Budget configured with zero cpu remaining -> should reject
    String zeroCpuWorkload = "zero-cpu-workload";
    manager.addOrUpdateWorkload(zeroCpuWorkload, 0L, 100L);
    assertFalse(manager.canAdmitQuery(zeroCpuWorkload),
        "Workload with zero CPU budget should be rejected");
  }
}
