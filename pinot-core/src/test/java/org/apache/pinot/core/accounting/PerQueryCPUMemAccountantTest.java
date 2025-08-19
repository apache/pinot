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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PerQueryCPUMemAccountantTest {

  @Test
  void testQueryAggregation() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregation";
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, threadEntries);

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
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, threadEntries);
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);

    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME));
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
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, threadEntries);
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);

    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME));
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
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, threadEntries);
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);
    accountant.reapFinishedTasks();

    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME));
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
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, threadEntries);
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);
    accountant.reapFinishedTasks();

    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with null
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
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
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, threadEntries);

    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);
    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    assertTrue(accountant.getInactiveQueries().isEmpty());
    accountant.reapFinishedTasks();

    // Pick up a new task. This will add entries to _finishedMemAggregator
    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(queryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME));
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
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, threadEntries);
    TestResourceAccountant accountant = new TestResourceAccountant(threadEntries);
    accountant.reapFinishedTasks();

    // Start a new query.
    CountDownLatch newQueryThreadLatch = new CountDownLatch(1);
    String newQueryId = "newQuery";
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> newQueryThreadEntries = new HashMap<>();
    TestResourceAccountant.getQueryThreadEntries(newQueryId, newQueryThreadLatch, newQueryThreadEntries);
    for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : newQueryThreadEntries.entrySet()) {
      accountant.addThreadEntry(entry.getKey(), entry.getValue());
    }

    // Create a new task for newQuery
    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(newQueryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) of first query with a new task id 5 of new query (3500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 3);
    assertNotNull(workerEntry);

    // New Task
    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry._currentThreadTaskStatus.set(
        new CPUMemThreadLevelAccountingObjects.TaskEntry(newQueryId, 5, ThreadExecutionContext.TaskType.SSE,
            anchorThread._workerThread, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME));
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
}
