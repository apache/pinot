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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PerQueryCPUMemAccountantTest {
  @AfterMethod
  void resetAccountant() {
    Tracing.unregisterThreadAccountant();
  }


  @Test
  void testQueryAggregation() {
    CountDownLatch threadLatch = new CountDownLatch(1);
    AtomicInteger terminationCount = new AtomicInteger(0);
    String queryId = "testQueryAggregation";

    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, terminationCount, List.of(1000, 2000, 2500));

    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 1 && queryResourceTrackerMap.containsKey(queryId)
          && queryResourceTrackerMap.get(queryId).getAllocatedBytes() == 5500;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 5500);
    threadLatch.countDown();

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "Wait for no active queries");
  }

  /*
   * @link{PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant#reapFinishedTask} stores the previous
   * task's status. If it is not called, then the current task info is lost.
   */
  @Test
  void testQueryAggregationCreateNewTask() {
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationCreateNewTask";
    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, new AtomicInteger(0), List.of(1000, 2000, 2500));

    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 1 && queryResourceTrackerMap.containsKey(queryId)
          && queryResourceTrackerMap.get(queryId).getAllocatedBytes() == 5500;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 2 (2500 bytes) with a new task id 5 (1500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 2);
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

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "Wait for no active queries");
  }

  @Test
  void testQueryAggregationSetToIdle() {
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationSetToIdle";
    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, new AtomicInteger(0), List.of(1000, 2000, 2500));

    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 1 && queryResourceTrackerMap.containsKey(queryId)
          && queryResourceTrackerMap.get(queryId).getAllocatedBytes() == 5500;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 3 (2500 bytes) with a new task id 5 (1500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 2);
    assertNotNull(workerEntry);

    CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = workerEntry._threadEntry;
    threadEntry.setToIdle();

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 3000);
    threadLatch.countDown();

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "Wait for no active queries");
  }

  /*
   * @link{PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant#reapFinishedTask} stores the previous
   * task's status. If it is called, then the resources of finished tasks should also be provided.
   */
  @Test
  void testQueryAggregationReapAndCreateNewTask() {
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationReapAndCreateNewTask";
    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, new AtomicInteger(0), List.of(1000, 2000, 2500));

    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 1 && queryResourceTrackerMap.containsKey(queryId)
          && queryResourceTrackerMap.get(queryId).getAllocatedBytes() == 5500;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    accountant.reapFinishedTasks();

    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 2 (2500 bytes) with a new task id 5 (1500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 2);
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

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "Wait for no active queries");
  }


  @Test
  void testQueryAggregationReapAndSetToIdle() {
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationReapAndSetToIdle";
    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, new AtomicInteger(0), List.of(1000, 2000, 2500));

    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 1 && queryResourceTrackerMap.containsKey(queryId)
          && queryResourceTrackerMap.get(queryId).getAllocatedBytes() == 5500;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    accountant.reapFinishedTasks();

    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 2 (2500 bytes) with null
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 2);
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

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "Wait for no active queries");
  }

  @Test
  void testInActiveQuerySet() {
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregation";

    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, new AtomicInteger(0), List.of(1000, 2000, 2500));

    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 1 && queryResourceTrackerMap.containsKey(queryId)
          && queryResourceTrackerMap.get(queryId).getAllocatedBytes() == 5500;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertTrue(accountant.getInactiveQueries().isEmpty());
    accountant.reapFinishedTasks();

    // Pick up a new task. This will add entries to _finishedMemAggregator
    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 2 (2500 bytes) with a new task id 5 (1500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 2);
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

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "Wait for no active queries");
  }

  @Test
  void testQueryAggregationAddNewQueryTask() {
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregationAddNewQueryTask";
    AtomicInteger terminatedCount = new AtomicInteger(0);
    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, terminatedCount, List.of(1000, 2000, 2500));

    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 1 && queryResourceTrackerMap.containsKey(queryId)
          && queryResourceTrackerMap.get(queryId).getAllocatedBytes() == 5500;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    accountant.reapFinishedTasks();

    // Start a new query.
    CountDownLatch newQueryThreadLatch = new CountDownLatch(1);
    String newQueryId = "newQuery";
    TestResourceAccountant.getQueryThreadEntries(newQueryId, newQueryThreadLatch, terminatedCount,
        List.of(1000, 2000, 2500));

    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 2 && queryResourceTrackerMap.containsKey(newQueryId)
          && queryResourceTrackerMap.get(queryId).getAllocatedBytes() == 5500;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    // Create a new task for newQuery
    TestResourceAccountant.TaskThread anchorThread =
        accountant.getTaskThread(newQueryId, CommonConstants.Accounting.ANCHOR_TASK_ID);
    assertNotNull(anchorThread);

    // Replace task id = 2 (2500 bytes) of first query with a new task id 5 of new query (3500 bytes)
    TestResourceAccountant.TaskThread workerEntry = accountant.getTaskThread(queryId, 2);
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

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "Wait for no active queries");
  }
}
