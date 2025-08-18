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

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class TestResourceAccountant extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestResourceAccountant.class);
  private long _heapUsageBytes = 0;

  TestResourceAccountant() {
    super(new PinotConfiguration(), false, true, true, new HashSet<>(), "test", InstanceType.SERVER);
  }

  public void setHeapUsageBytes(long heapUsageBytes) {
    _heapUsageBytes = heapUsageBytes;
  }

  @Override
  public WatcherTask createWatcherTask() {
    return new TestResourceWatcherTask();
  }

  class TestResourceWatcherTask extends WatcherTask {
    TestResourceWatcherTask() {
      PinotConfiguration config = getPinotConfiguration();
      QueryMonitorConfig queryMonitorConfig = new QueryMonitorConfig(config, 1000);
      _queryMonitorConfig.set(queryMonitorConfig);
    }

    @Override
    public void runOnce() {
      _aggregatedUsagePerActiveQuery = null;
      _triggeringLevel = TriggeringLevel.Normal;
      try {
        evalTriggers();
        reapFinishedTasks();
        _aggregatedUsagePerActiveQuery = getQueryResourcesImpl();
        _maxHeapUsageQuery.set(_aggregatedUsagePerActiveQuery.values().stream()
            .filter(stats -> !_cancelSentQueries.contains(stats.getQueryId()))
            .max(Comparator.comparing(AggregatedStats::getAllocatedBytes)).orElse(null));
        triggeredActions();
      } catch (Exception e) {
        LOGGER.error("Caught exception while executing stats aggregation and query kill", e);
      } finally {
        // Clean inactive query stats
        cleanInactive();
      }
    }

    @Override
    public long getHeapUsageBytes() {
      return _heapUsageBytes;
    }
  }

  void setCriticalLevelHeapUsageRatio(long maxHeapSize, double ratio) {
    PinotConfiguration config = getPinotConfiguration();
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, ratio);
    _watcherTask._queryMonitorConfig.set(new QueryMonitorConfig(config, maxHeapSize));
  }

  void setPanicLevelHeapUsageRatio(long maxHeapSize, double ratio) {
    PinotConfiguration config = getPinotConfiguration();
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO, ratio);
    _watcherTask._queryMonitorConfig.set(new QueryMonitorConfig(config, maxHeapSize));
  }

  private static @NotNull PinotConfiguration getPinotConfiguration() {
    PinotConfiguration config = new PinotConfiguration();

    config.setProperty(CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO, 0.01);

    config.setProperty(CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO,
        CommonConstants.Accounting.DFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO);

    config.setProperty(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO,
        CommonConstants.Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO);

    config.setProperty(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO,
        CommonConstants.Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO);

    config.setProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_MS,
        CommonConstants.Accounting.DEFAULT_SLEEP_TIME_MS);

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

    // Enable self-termination of the thread
    config.setProperty(CommonConstants.Accounting.CONFIG_OF_THREAD_SELF_TERMINATE, true);
    return config;
  }

  public TaskThread getTaskThread(String queryId, int taskId) {
    for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : _threadEntriesMap.entrySet()) {
      CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = entry.getValue();
      if (queryId.equals(threadEntry.getQueryId()) && taskId == threadEntry.getTaskId()) {
        return new TaskThread(threadEntry, entry.getKey());
      }
    }
    throw new IllegalStateException("Failed to find thread for queryId: " + queryId + ", taskId: " + taskId);
  }

  public static class TaskThread {
    public final CPUMemThreadLevelAccountingObjects.ThreadEntry _threadEntry;
    public final Thread _workerThread;

    public TaskThread(CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry, Thread workerThread) {
      _threadEntry = threadEntry;
      _workerThread = workerThread;
    }
  }
}
