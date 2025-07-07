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
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PerQueryCPUMemAccountantTest {
  public static class TaskThread {
    public final CPUMemThreadLevelAccountingObjects.ThreadEntry _threadEntry;
    public final Thread _workerThread;

    public TaskThread(CPUMemThreadLevelAccountingObjects.ThreadEntry _threadEntry, Thread _workerThread) {
      this._threadEntry = _threadEntry;
      this._workerThread = _workerThread;
    }
  }

  static class TestResourceAccountant extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {
    TestResourceAccountant(Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries) {
      super(new PinotConfiguration(), false, true, true, new HashSet<>(), "test", InstanceType.SERVER);
      _threadEntriesMap.putAll(threadEntries);
    }

    public TaskThread getTaskThread(String queryId, int taskId) {
      Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> workerEntry =
          _threadEntriesMap.entrySet().stream().filter(
                  e -> e.getValue()._currentThreadTaskStatus.get().getTaskId() == 3 && Objects.equals(
                      e.getValue()._currentThreadTaskStatus.get().getQueryId(), queryId)).collect(Collectors.toList())
              .get(0);
      return new TaskThread(workerEntry.getValue(), workerEntry.getKey());
    }
  }

  @Test
  void testQueryAggregation() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregation";
    getQueryThreadEntries(queryId, threadLatch, threadEntries);

    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);
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
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);

    TaskThread anchorThread = accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread));
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
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);

    TaskThread anchorThread = accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread));
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

    TaskThread anchorThread = accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread));
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

    TaskThread anchorThread = accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with null
    TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // Set to Idle
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry.setToIdle();

    accountant.reapFinishedTasks();

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 5500);
    threadLatch.countDown();
  }

  @Test
  void testInActiveQuerySet() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregation";
    getQueryThreadEntries(queryId, threadLatch, threadEntries);

    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);
    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    assertTrue(accountant.getInactiveQueries().isEmpty());
    accountant.reapFinishedTasks();

    // Pick up a new task. This will add entries to _finishedMemAggregator
    TaskThread anchorThread = accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread));
    threadEntry._currentThreadMemoryAllocationSampleBytes = 1500;

    accountant.reapFinishedTasks();

    // A call to cleanInactiveQueries surprisingly adds the query id to the set.
    accountant.cleanInactive();
    assertEquals(accountant.getInactiveQueries().size(), 1);
    assertTrue(accountant.getInactiveQueries().contains(queryId));
    // A call to reapFinishedTasks should remove the query id from the inactive queries set.
    accountant.reapFinishedTasks();
    assertTrue(accountant.getInactiveQueries().isEmpty());
    threadLatch.countDown();
  }

  @Test
  void testQueryAggregationAddNewQueryTask() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationAddNewQueryTask";
    getQueryThreadEntries(queryId, threadLatch, threadEntries);
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);
    accountant.reapFinishedTasks();

    // Start a new query.
    CountDownLatch newQueryThreadLatch = new CountDownLatch(1);
    String newQueryId = "newQuery";
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> newQueryThreadEntries = new HashMap<>();
    getQueryThreadEntries(newQueryId, newQueryThreadLatch, newQueryThreadEntries);
    for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : newQueryThreadEntries.entrySet()) {
      accountant.addThreadEntry(entry.getKey(), entry.getValue());
    }

    // Create a new task for newQuery
    TaskThread anchorThread = accountant.getTaskThread(newQueryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) of first query with a new task id 5 of new query (3500 bytes)
    TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(newQueryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread));
    threadEntry._currentThreadMemoryAllocationSampleBytes = 3500;

    accountant.reapFinishedTasks();

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 2);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 5500);
    QueryResourceTracker newQueryResourceTracker = queryResourceTrackerMap.get(newQueryId);
    assertEquals(newQueryResourceTracker.getAllocatedBytes(), 9000);
    threadLatch.countDown();
    newQueryThreadLatch.countDown();
  }

  private static void getQueryThreadEntries(String queryId, CountDownLatch threadLatch,
      Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries) {
    TaskThread anchorThread = getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID, threadLatch, null);
    threadEntries.put(anchorThread._workerThread, anchorThread._threadEntry);
    anchorThread._threadEntry._currentThreadMemoryAllocationSampleBytes = 1000;

    CPUMemThreadLevelAccountingObjects.ThreadEntry anchorEntry = new CPUMemThreadLevelAccountingObjects.ThreadEntry();
    anchorEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID,
            ThreadExecutionContext.TaskType.SSE, anchorThread._workerThread));
    anchorEntry._currentThreadMemoryAllocationSampleBytes = 1000;
    threadEntries.put(anchorThread._workerThread, anchorEntry);

    TaskThread taskThread2 = getTaskThread(queryId, 2, threadLatch, anchorThread._workerThread);
    threadEntries.put(taskThread2._workerThread, taskThread2._threadEntry);
    taskThread2._threadEntry._currentThreadMemoryAllocationSampleBytes = 2000;

    TaskThread taskThread3 = getTaskThread(queryId, 3, threadLatch, anchorThread._workerThread);
    threadEntries.put(taskThread3._workerThread, taskThread3._threadEntry);
    taskThread3._threadEntry._currentThreadMemoryAllocationSampleBytes = 2500;
  }

  private static TaskThread getTaskThread(String queryId, int taskId, CountDownLatch threadLatch, Thread anchorThread) {
    CPUMemThreadLevelAccountingObjects.ThreadEntry worker1 = new CPUMemThreadLevelAccountingObjects.ThreadEntry();
    worker1._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, taskId, ThreadExecutionContext.TaskType.SSE,
            anchorThread));
    Thread workerThread1 = new Thread(() -> {
      try {
        threadLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    workerThread1.start();
    return new TaskThread(worker1, workerThread1);
  }
}
