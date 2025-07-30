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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PerQueryCPUMemAccountCancelTest extends BasePerQueryCPUMemAccountantTest {
  @Test
  void testCancelSingleQueryCritical() {
    CountDownLatch threadLatch = new CountDownLatch(1);
    AtomicInteger terminationCount = new AtomicInteger(0);
    String queryId = "testQueryAggregation";

    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    startQueryThreads(queryId, threadLatch, terminationCount, List.of(1000, 2000, 2500));

    // Ensure the Accountant state is correctly initialized
    waitForQueryResourceTracker(accountant, queryId, 5500);

    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 5500);

    // Set the critical level heap usage ratio to a value that will trigger hard cancellation
    accountant.setCriticalLevelHeapUsageRatio(10000, 0.5);
    accountant.setHeapUsageBytes(5500);

    // Cancel a query.
    accountant.getWatcherTask().runOnce();
    threadLatch.countDown();

    TestUtils.waitForCondition(aVoid -> accountant.getCancelSentQueries().contains(queryId), 10L, 1000L,
        "Waiting for query to be cancelled");

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "CancelSentList was not cleared");
  }

  @Test
  void testCancelTwoQueryCriticalLevel() {
    CountDownLatch threadLatch = new CountDownLatch(1);
    AtomicInteger terminationCount = new AtomicInteger(0);
    String expensiveQueryId = "testExpensiveAggregation";
    String cheapQueryId = "testCheapAggregation";

    TestResourceAccountant accountant = new TestResourceAccountant();
    Tracing.register(accountant);
    startQueryThreads(expensiveQueryId, threadLatch, terminationCount, List.of(1000, 2000, 2500));
    startQueryThreads(cheapQueryId, threadLatch, terminationCount, List.of(100, 200, 250));

    // Ensure the Accountant state is correctly initialized
    waitForQueryResourceTracker(accountant, expensiveQueryId, 5500);
    waitForQueryResourceTracker(accountant, cheapQueryId, 550);

    // Set the critical level heap usage ratio to a value that will trigger hard cancellation
    accountant.setCriticalLevelHeapUsageRatio(10000, 0.5);
    accountant.setHeapUsageBytes(6050);
    // Cancel a query.
    accountant.getWatcherTask().runOnce();
    threadLatch.countDown();

    TestUtils.waitForCondition(
        aVoid -> accountant.getCancelSentQueries().contains(expensiveQueryId) && !accountant.getCancelSentQueries()
            .contains(cheapQueryId), 10L, 1000L, "Waiting for one of the queries to be cancelled");

    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "CancelSentList was not cleared");
  }
}
