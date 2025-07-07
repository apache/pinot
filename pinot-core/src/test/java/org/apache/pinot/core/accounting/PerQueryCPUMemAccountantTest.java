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
package org.apache.pinot.core.accounting;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class PerQueryCPUMemAccountantTest {
  static class TestResourceAccountant extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {
    TestResourceAccountant(Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries) {
      super(new PinotConfiguration(), false, true, true, new HashSet<>(), "test", InstanceType.SERVER);
      _threadEntriesMap.putAll(threadEntries);
    }
  }

  @Test
  void testQueryAggregation() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregation";
    getQueryThreadEntries(queryId, threadLatch, threadEntries);

    ThreadResourceUsageAccountant accountant = new TestResourceAccountant(threadEntries);
    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 5500);
    threadLatch.countDown();
  }

  /*
   * @link{PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant#reapFinishedTask} stores the previous
   * task's status. If it is not called, then the current task info is lost.
   */
  @Test
  void testQueryAggregationCreateNewTask() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationCreateNewTask";
    getQueryThreadEntries(queryId, threadLatch, threadEntries);
    ThreadResourceUsageAccountant accountant = new TestResourceAccountant(threadEntries);

    Thread anchorThread =
        threadEntries.entrySet().stream().filter(e -> e.getValue()._currentThreadTaskStatus.get().isAnchorThread())
            .collect(Collectors.toList()).get(0).getKey();
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> workerEntry =
        threadEntries.entrySet().stream().filter(e -> e.getValue()._currentThreadTaskStatus.get().getTaskId() == 3)
            .collect(Collectors.toList()).get(0);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry.getValue();
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread));
    threadEntry._currentThreadMemoryAllocationSampleBytes = 1500;

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 4500);
    threadLatch.countDown();
  }

  @Test
  void testQueryAggregationSetToIdle() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationSetToIdle";
    getQueryThreadEntries(queryId, threadLatch, threadEntries);
    ThreadResourceUsageAccountant accountant = new TestResourceAccountant(threadEntries);

    Thread anchorThread =
        threadEntries.entrySet().stream().filter(e -> e.getValue()._currentThreadTaskStatus.get().isAnchorThread())
            .collect(Collectors.toList()).get(0).getKey();
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> workerEntry =
        threadEntries.entrySet().stream().filter(e -> e.getValue()._currentThreadTaskStatus.get().getTaskId() == 3)
            .collect(Collectors.toList()).get(0);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry.getValue();
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread));
    threadEntry.setToIdle();

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 3000);
    threadLatch.countDown();
  }

  /*
   * @link{PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant#reapFinishedTask} stores the previous
   * task's status. If it is called, then the resources of finished tasks should also be provided.
   */
  @Test
  void testQueryAggregationReapAndCreateNewTask() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationReapAndCreateNewTask";
    getQueryThreadEntries(queryId, threadLatch, threadEntries);
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);
    accountant.reapFinishedTasks();

    Thread anchorThread =
        threadEntries.entrySet().stream().filter(e -> e.getValue()._currentThreadTaskStatus.get().isAnchorThread())
            .collect(Collectors.toList()).get(0).getKey();
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> workerEntry =
        threadEntries.entrySet().stream().filter(e -> e.getValue()._currentThreadTaskStatus.get().getTaskId() == 3)
            .collect(Collectors.toList()).get(0);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry.getValue();
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread));
    threadEntry._currentThreadMemoryAllocationSampleBytes = 1500;

    accountant.reapFinishedTasks();

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 7000);
    threadLatch.countDown();
  }

  @Test
  void testQueryAggregationReapAndSetToIdle() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationReapAndSetToIdle";
    getQueryThreadEntries(queryId, threadLatch, threadEntries);
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);
    accountant.reapFinishedTasks();

    Thread anchorThread =
        threadEntries.entrySet().stream().filter(e -> e.getValue()._currentThreadTaskStatus.get().isAnchorThread())
            .collect(Collectors.toList()).get(0).getKey();
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with null
    Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> workerEntry =
        threadEntries.entrySet().stream().filter(e -> e.getValue()._currentThreadTaskStatus.get().getTaskId() == 3)
            .collect(Collectors.toList()).get(0);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry.getValue();
    threadEntry.setToIdle();

    accountant.reapFinishedTasks();

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 5500);
    threadLatch.countDown();
  }

  private void getQueryThreadEntries(String queryId, CountDownLatch threadLatch,
      Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries) {
    Thread anchorThread = new Thread(() -> {
      try {
        threadLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    anchorThread.start();

    CPUMemThreadLevelAccountingObjects.ThreadEntry anchorEntry = new CPUMemThreadLevelAccountingObjects.ThreadEntry();
    anchorEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID,
            ThreadExecutionContext.TaskType.SSE, anchorThread));
    anchorEntry._currentThreadMemoryAllocationSampleBytes = 1000;
    threadEntries.put(anchorThread, anchorEntry);

    CPUMemThreadLevelAccountingObjects.ThreadEntry worker1 = new CPUMemThreadLevelAccountingObjects.ThreadEntry();
    worker1._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 2, ThreadExecutionContext.TaskType.SSE,
            anchorThread));
    worker1._currentThreadMemoryAllocationSampleBytes = 2000;
    Thread workerThread1 = new Thread(() -> {
      try {
        threadLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    workerThread1.start();
    threadEntries.put(workerThread1, worker1);

    CPUMemThreadLevelAccountingObjects.ThreadEntry worker2 = new CPUMemThreadLevelAccountingObjects.ThreadEntry();
    worker2._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 3, ThreadExecutionContext.TaskType.SSE,
            anchorThread));
    worker2._currentThreadMemoryAllocationSampleBytes = 2500;
    Thread workerThread2 = new Thread(() -> {
      try {
        threadLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    workerThread2.start();
    threadEntries.put(workerThread2, worker2);
  }
}
