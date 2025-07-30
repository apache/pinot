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
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PerQueryCPUMemAccountHardCancelTest {
  @AfterMethod
  void resetAccountant() {
    Tracing.unregisterThreadAccountant();
  }

  @Test
  void testHardCancelOfSingleQueryCriticalLevel() {
    CountDownLatch sampleLatch = new CountDownLatch(1);
    String queryId = "testExpensiveAggregation";
    AtomicInteger terminationCount = new AtomicInteger(0);

    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(queryId, sampleLatch, terminationCount, List.of(1000, 2000, 2500));

    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 1 && queryResourceTrackerMap.containsKey(queryId);
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    // Ensure the Accountant state is correctly initialized
    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 5500);

    // Set the critical level heap usage ratio to a value that will trigger hard cancellation
    accountant.setCriticalLevelHeapUsageRatio(10000, 0.5);
    accountant.setHeapUsageBytes(5500);
    accountant.getWatcherTask().runOnce();
    sampleLatch.countDown();

    TestUtils.waitForCondition(aVoid -> accountant.getCancelSentQueries().contains(queryId), 10L, 1000L,
        "Waiting for query to be cancelled");
    assertTrue(accountant.getCancelSentQueries().contains(queryId));
    TestUtils.waitForCondition(aVoid -> terminationCount.get() == 4, 10L, 1000L,
        "Waiting for all threads to terminate");

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "CancelSentList was not cleared");
  }

  @Test
  void testHardCancelOfTwoQueriesAtCriticalLevel() {
    CountDownLatch sampleLatch = new CountDownLatch(1);
    String expensiveQueryId = "testExpensiveAggregation";
    String cheapQueryId = "testCheapAggregation";
    AtomicInteger terminationCount = new AtomicInteger(0);

    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(expensiveQueryId, sampleLatch, terminationCount,
        List.of(1000, 2000, 2500));
    TestResourceAccountant.getQueryThreadEntries(cheapQueryId, sampleLatch, terminationCount, List.of(100, 200, 250));

    // Ensure the Accountant state is correctly initialized
    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 2 && queryResourceTrackerMap.containsKey(expensiveQueryId)
          && queryResourceTrackerMap.containsKey(cheapQueryId)
          && queryResourceTrackerMap.get(expensiveQueryId).getAllocatedBytes() == 5500
          && queryResourceTrackerMap.get(cheapQueryId).getAllocatedBytes() == 550;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    // Set the critical level heap usage ratio to a value that will trigger hard cancellation
    accountant.setCriticalLevelHeapUsageRatio(10000, 0.5);
    accountant.setHeapUsageBytes(6050);
    accountant.getWatcherTask().runOnce();
    sampleLatch.countDown();

    TestUtils.waitForCondition(aVoid -> accountant.getCancelSentQueries().contains(expensiveQueryId), 10L, 1000L,
        "Waiting for query to be cancelled");
    assertTrue(accountant.getCancelSentQueries().contains(expensiveQueryId));
    TestUtils.waitForCondition(aVoid -> terminationCount.get() == 4, 10L, 1000L,
        "Waiting for all threads to terminate");

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "CancelSentList was not cleared");
  }

  @Test
  void testHardCancelOfTwoQueriesAtPanicLevel() {
    CountDownLatch sampleLatch = new CountDownLatch(1);
    String expensiveQueryId = "testExpensiveAggregation";
    String cheapQueryId = "testCheapAggregation";
    AtomicInteger terminationCount = new AtomicInteger(0);

    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    TestResourceAccountant.getQueryThreadEntries(expensiveQueryId, sampleLatch, terminationCount,
        List.of(1000, 2000, 2500));
    TestResourceAccountant.getQueryThreadEntries(cheapQueryId, sampleLatch, terminationCount, List.of(100, 200, 250));

    // Ensure the Accountant state is correctly initialized
    TestUtils.waitForCondition(aVoid -> {
      Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
      return queryResourceTrackerMap.size() == 2 && queryResourceTrackerMap.containsKey(expensiveQueryId)
          && queryResourceTrackerMap.containsKey(cheapQueryId)
          && queryResourceTrackerMap.get(expensiveQueryId).getAllocatedBytes() == 5500
          && queryResourceTrackerMap.get(cheapQueryId).getAllocatedBytes() == 550;
    }, 100L, 5000L, "Waiting for query resource tracker to be initialized");

    // Set the critical level heap usage ratio to a value that will trigger hard cancellation
    accountant.setPanicLevelHeapUsageRatio(10000, 0.5);
    accountant.setHeapUsageBytes(6050);
    accountant.getWatcherTask().runOnce();
    sampleLatch.countDown();

    TestUtils.waitForCondition(
        aVoid -> accountant.getCancelSentQueries().contains(expensiveQueryId) && accountant.getCancelSentQueries()
            .contains(cheapQueryId), 10L, 1000L, "Waiting for query to be cancelled");
    assertTrue(accountant.getCancelSentQueries().contains(expensiveQueryId));
    TestUtils.waitForCondition(aVoid -> terminationCount.get() == 8, 10L, 1000L,
        "Waiting for all threads to terminate");

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "CancelSentList was not cleared");
  }
}
