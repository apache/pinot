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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PerQueryCPUMemAccountCancelTest {
  static class AlwaysTerminateMostExpensiveQueryAccountant extends TestResourceAccountant {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlwaysTerminateMostExpensiveQueryAccountant.class);
    private final List<String> _cancelLog = new ArrayList<>();

    AlwaysTerminateMostExpensiveQueryAccountant(
        Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries) {
      super(threadEntries);
    }

    @Override
    public WatcherTask createWatcherTask() {
      return new TerminatingWatcherTask();
    }

    @Override
    public void cancelQuery(String queryId, Thread anchorThread) {
      _cancelSentQueries.add(queryId);
      _cancelLog.add(queryId);
    }

    public List<String> getCancelLog() {
      return _cancelLog;
    }

    class TerminatingWatcherTask extends WatcherTask {
      TerminatingWatcherTask() {
        PinotConfiguration config = new PinotConfiguration();

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO, 0.01);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO,
            CommonConstants.Accounting.DFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO,
            CommonConstants.Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC,
            CommonConstants.Accounting.DEFAULT_CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_GC_BACKOFF_COUNT,
            CommonConstants.Accounting.DEFAULT_GC_BACKOFF_COUNT);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO,
            CommonConstants.Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_MS,
            CommonConstants.Accounting.DEFAULT_SLEEP_TIME_MS);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_GC_WAIT_TIME_MS,
            CommonConstants.Accounting.DEFAULT_CONFIG_OF_GC_WAIT_TIME_MS);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR,
            CommonConstants.Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_PUBLISHING_JVM_USAGE,
            CommonConstants.Accounting.DEFAULT_PUBLISHING_JVM_USAGE);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED,
            CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_ENABLED);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS,
            CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS);

        config.setProperty(CommonConstants.Accounting.CONFIG_OF_QUERY_KILLED_METRIC_ENABLED,
            CommonConstants.Accounting.DEFAULT_QUERY_KILLED_METRIC_ENABLED);

        QueryMonitorConfig queryMonitorConfig = new QueryMonitorConfig(config, 1000);
        _queryMonitorConfig.set(queryMonitorConfig);
      }

      @Override
      public void runOnce() {
        _aggregatedUsagePerActiveQuery = null;
        try {
          evalTriggers();
          reapFinishedTasks();
          _aggregatedUsagePerActiveQuery = getQueryResourcesImpl();
          triggeredActions();
        } catch (Exception e) {
          LOGGER.error("Caught exception while executing stats aggregation and query kill", e);
        } finally {
          // Clean inactive query stats
          cleanInactive();
        }
      }

      @Override
      public void evalTriggers() {
        _triggeringLevel = TriggeringLevel.HeapMemoryCritical;
      }
    }
  }

  @Test
  void testCancelSingleQuery() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryAggregation";
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, threadEntries);

    AlwaysTerminateMostExpensiveQueryAccountant accountant =
        new AlwaysTerminateMostExpensiveQueryAccountant(threadEntries);
    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 1);
    QueryResourceTracker queryResourceTracker = queryResourceTrackerMap.get(queryId);
    assertEquals(queryResourceTracker.getAllocatedBytes(), 5500);

    // Cancel a query.
    accountant.getWatcherTask().runOnce();
    assertEquals(accountant.getCancelLog().size(), 1);

    // Try once more. There should still be only one cancel.
    accountant.getWatcherTask().runOnce();
    assertEquals(accountant.getCancelLog().size(), 1);
    threadLatch.countDown();
    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "CancelSentList was not cleared");
  }

  @Test
  void testCancelTwoQuery() {
    Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = new HashMap<>();
    CountDownLatch threadLatch = new CountDownLatch(1);
    String queryId = "testQueryOne";
    TestResourceAccountant.getQueryThreadEntries(queryId, threadLatch, threadEntries);
    String queryId2 = "testQueryTwo";
    TestResourceAccountant.getQueryThreadEntries(queryId2, threadLatch, threadEntries);

    AlwaysTerminateMostExpensiveQueryAccountant accountant =
        new AlwaysTerminateMostExpensiveQueryAccountant(threadEntries);
    Map<String, ? extends QueryResourceTracker> queryResourceTrackerMap = accountant.getQueryResources();
    assertEquals(queryResourceTrackerMap.size(), 2);
    assertEquals(queryResourceTrackerMap.get(queryId).getAllocatedBytes(), 5500);
    assertEquals(queryResourceTrackerMap.get(queryId2).getAllocatedBytes(), 5500);

    // Cancel a query.
    accountant.getWatcherTask().runOnce();
    assertEquals(accountant.getCancelLog().size(), 1);

    accountant.getWatcherTask().runOnce();
    assertEquals(accountant.getCancelLog().size(), 2);
    threadLatch.countDown();
    TestUtils.waitForCondition(aVoid -> {
      accountant.reapFinishedTasks();
      return accountant.getCancelSentQueries().isEmpty();
    }, 100L, 1000L, "CancelSentList was not cleared");
  }
}
