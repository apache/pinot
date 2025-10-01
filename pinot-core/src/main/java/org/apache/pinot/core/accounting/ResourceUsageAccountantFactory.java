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

import java.util.Collection;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.accounting.TrackingScope;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ResourceUsageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResourceUsageAccountantFactory implements ThreadAccountantFactory {

  @Override
  public ResourceUsageAccountant init(PinotConfiguration config, String instanceId, InstanceType instanceType) {
    return new ResourceUsageAccountant(config, instanceId, instanceType);
  }

  public static class ResourceUsageAccountant implements ThreadAccountant {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUsageAccountant.class);

    private static final String ACCOUNTANT_TASK_NAME = "ResourceUsageAccountant";
    private static final int ACCOUNTANT_PRIORITY = 4;
    private final ExecutorService _executorService = Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread(r);
      thread.setPriority(ACCOUNTANT_PRIORITY);
      thread.setDaemon(true);
      thread.setName(ACCOUNTANT_TASK_NAME);
      return thread;
    });

    // Map from the thread executing the query to the thread resource tracker.
    private final ConcurrentHashMap<Thread, ThreadResourceTrackerImpl> _threadTrackers = new ConcurrentHashMap<>();

    private final ThreadLocal<ThreadResourceTrackerImpl> _threadLocalEntry = ThreadLocal.withInitial(() -> {
      ThreadResourceTrackerImpl threadTracker = new ThreadResourceTrackerImpl();
      _threadTrackers.put(Thread.currentThread(), threadTracker);
      LOGGER.debug("Adding thread to _threadLocalEntry: {}", Thread.currentThread().getName());
      return threadTracker;
    });

    private final boolean _cpuSamplingEnabled;
    private final boolean _memorySamplingEnabled;
    private final WatcherTask _watcherTask;
    private final QueryResourceAggregator _queryResourceAggregator;
    private final WorkloadResourceAggregator _workloadResourceAggregator;
    private final EnumMap<TrackingScope, ResourceAggregator> _resourceAggregators = new EnumMap<>(TrackingScope.class);

    public ResourceUsageAccountant(PinotConfiguration config, String instanceId, InstanceType instanceType) {
      LOGGER.info("Initializing ResourceUsageAccountant");
      boolean cpuSamplingEnabled = config.getProperty(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING,
          CommonConstants.Accounting.DEFAULT_ENABLE_THREAD_CPU_SAMPLING);
      if (cpuSamplingEnabled && !ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled()) {
        LOGGER.warn("Thread CPU time measurement is not enabled in the JVM, disabling CPU sampling");
        cpuSamplingEnabled = false;
      }
      _cpuSamplingEnabled = cpuSamplingEnabled;
      boolean memorySamplingEnabled =
          config.getProperty(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING,
              CommonConstants.Accounting.DEFAULT_ENABLE_THREAD_MEMORY_SAMPLING);
      if (memorySamplingEnabled && !ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled()) {
        LOGGER.warn("Thread memory measurement is not enabled in the JVM, disabling memory sampling");
        memorySamplingEnabled = false;
      }
      _memorySamplingEnabled = memorySamplingEnabled;
      _watcherTask = new WatcherTask(config);
      _queryResourceAggregator =
          new QueryResourceAggregator(instanceId, instanceType, cpuSamplingEnabled, memorySamplingEnabled,
              _watcherTask._queryMonitorConfig);
      _workloadResourceAggregator =
          new WorkloadResourceAggregator(instanceId, instanceType, cpuSamplingEnabled, memorySamplingEnabled,
              _watcherTask._queryMonitorConfig);
      _resourceAggregators.put(TrackingScope.QUERY, _queryResourceAggregator);
      _resourceAggregators.put(TrackingScope.WORKLOAD, _workloadResourceAggregator);
      LOGGER.info(
          "Initialized ResourceUsageAccountant for {}: {} with cpuSamplingEnabled: {}, memorySamplingEnabled: {}",
          instanceType, instanceId, cpuSamplingEnabled, memorySamplingEnabled);
    }

    public QueryMonitorConfig getQueryMonitorConfig() {
      return _watcherTask.getQueryMonitorConfig();
    }

    @Override
    public void setupTask(QueryThreadContext threadContext) {
      _threadLocalEntry.get().setThreadContext(threadContext);
    }

    @Override
    public void sampleUsage() {
      ThreadResourceTrackerImpl threadTracker = _threadLocalEntry.get();
      if (_cpuSamplingEnabled) {
        threadTracker.updateCpuSnapshot();
      }
      if (_memorySamplingEnabled) {
        threadTracker.updateMemorySnapshot();
      }
    }

    @Override
    public void clear() {
      ThreadResourceTrackerImpl threadTracker = _threadLocalEntry.get();
      assert threadTracker.getThreadContext() != null;
      QueryExecutionContext executionContext = threadTracker.getThreadContext().getExecutionContext();
      String queryId = executionContext.getCid();
      String workloadName = executionContext.getWorkloadName();
      long cpuTimeNs = threadTracker.getCpuTimeNs();
      long allocatedBytes = threadTracker.getAllocatedBytes();
      _queryResourceAggregator.updateUntrackedResourceUsage(queryId, cpuTimeNs, allocatedBytes);
      _workloadResourceAggregator.updateUntrackedResourceUsage(workloadName, cpuTimeNs, allocatedBytes);
      threadTracker.clear();
    }

    @Override
    public void updateUntrackedResourceUsage(String identifier, long cpuTimeNs, long allocatedBytes,
        TrackingScope trackingScope) {
      if (!_cpuSamplingEnabled) {
        cpuTimeNs = 0;
      }
      if (!_memorySamplingEnabled) {
        allocatedBytes = 0;
      }
      _resourceAggregators.get(trackingScope).updateUntrackedResourceUsage(identifier, cpuTimeNs, allocatedBytes);
    }

    @Override
    public boolean throttleQuerySubmission() {
      return _queryResourceAggregator.getHeapUsageBytes() > _watcherTask.getQueryMonitorConfig().getAlarmingLevel();
    }

    @Override
    public void startWatcherTask() {
      _executorService.submit(_watcherTask);
    }

    @Override
    public void stopWatcherTask() {
      _executorService.shutdownNow();
    }

    @Override
    public PinotClusterConfigChangeListener getClusterConfigChangeListener() {
      return _watcherTask;
    }

    @Override
    public Collection<? extends ThreadResourceTracker> getThreadResources() {
      return _threadTrackers.values();
    }

    @Override
    public Map<String, ? extends QueryResourceTracker> getQueryResources() {
      return _queryResourceAggregator.getQueryResources(_threadTrackers);
    }

    private class WatcherTask implements Runnable, PinotClusterConfigChangeListener {
      final AtomicReference<QueryMonitorConfig> _queryMonitorConfig = new AtomicReference<>();

      int _sleepTime;

      WatcherTask(PinotConfiguration config) {
        QueryMonitorConfig queryMonitorConfig = new QueryMonitorConfig(config, ResourceUsageUtils.getMaxHeapSize());
        _queryMonitorConfig.set(queryMonitorConfig);
        logQueryMonitorConfig(queryMonitorConfig);
      }

      QueryMonitorConfig getQueryMonitorConfig() {
        return _queryMonitorConfig.get();
      }

      @Override
      public void run() {
        try {
          //noinspection InfiniteLoopStatement
          while (true) {
            try {
              runOnce();
            } finally {
              //noinspection BusyWait
              Thread.sleep(_sleepTime);
            }
          }
        } catch (InterruptedException e) {
          LOGGER.warn("WatcherTask interrupted, exiting.");
        }
      }

      private void runOnce() {
        try {
          runPreAggregation();
          runAggregation();
          runPostAggregation();
        } catch (Exception e) {
          LOGGER.error("Caught exception while executing stats aggregation and query kill", e);
          // TODO: Add a metric to track the number of watcher task errors.
        } finally {
          LOGGER.debug("_threadTrackers size: {}", _threadTrackers.size());
          for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
            resourceAggregator.cleanUpPostAggregation();
          }
          // Get sleeptime from both resourceAggregators. Pick the minimum. PerQuery Accountant modifies the sleep
          // time when condition is critical.
          int sleepTime = Integer.MAX_VALUE;
          for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
            sleepTime = Math.min(sleepTime, resourceAggregator.getAggregationSleepTimeMs());
          }
          _sleepTime = sleepTime;
        }
      }

      private void runPreAggregation() {
        // Call the pre-aggregation methods for each ResourceAggregator.
        for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
          resourceAggregator.preAggregate(_threadTrackers.values());
        }
      }

      private void runAggregation() {
        Iterator<Map.Entry<Thread, ThreadResourceTrackerImpl>> iterator = _threadTrackers.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<Thread, ThreadResourceTrackerImpl> entry = iterator.next();
          Thread thread = entry.getKey();
          if (!thread.isAlive()) {
            LOGGER.debug("Thread: {} is no longer alive, removing it from _threadTrackers", thread.getName());
            iterator.remove();
          } else {
            for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
              resourceAggregator.aggregate(entry.getValue());
            }
          }
        }
      }

      private void runPostAggregation() {
        for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
          resourceAggregator.postAggregate();
        }
      }

      @Override
      public synchronized void onChange(Set<String> changedConfigs,
          Map<String, String> clusterConfigs) {        // Filter configs that have CommonConstants
        // .PREFIX_SCHEDULER_PREFIX
        Set<String> filteredChangedConfigs = changedConfigs.stream()
            .filter(config -> config.startsWith(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX))
            .map(config -> config.replace(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".", ""))
            .collect(Collectors.toSet());

        if (filteredChangedConfigs.isEmpty()) {
          LOGGER.debug("No relevant configs changed, skipping update for QueryMonitorConfig.");
          return;
        }

        Map<String, String> filteredClusterConfigs = clusterConfigs.entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX))
            .collect(Collectors.toMap(
                entry -> entry.getKey().replace(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".", ""),
                Map.Entry::getValue));

        QueryMonitorConfig oldConfig = getQueryMonitorConfig();
        QueryMonitorConfig newConfig =
            new QueryMonitorConfig(oldConfig, filteredChangedConfigs, filteredClusterConfigs);
        _queryMonitorConfig.set(newConfig);
        logQueryMonitorConfig(newConfig);
      }

      private void logQueryMonitorConfig(QueryMonitorConfig queryMonitorConfig) {
        LOGGER.info("Updated Configuration for Query Monitor");
        LOGGER.info("Xmx is {}", queryMonitorConfig.getMaxHeapSize());
        LOGGER.info("_alarmingLevel of on heap memory is {}", queryMonitorConfig.getAlarmingLevel());
        LOGGER.info("_criticalLevel of on heap memory is {}", queryMonitorConfig.getCriticalLevel());
        LOGGER.info("_panicLevel of on heap memory is {}", queryMonitorConfig.getPanicLevel());
        LOGGER.info("_normalSleepTime is {}", queryMonitorConfig.getNormalSleepTime());
        LOGGER.info("_alarmingSleepTime is {}", queryMonitorConfig.getAlarmingSleepTime());
        LOGGER.info("_oomKillQueryEnabled: {}", queryMonitorConfig.isOomKillQueryEnabled());
        LOGGER.info("_minMemoryFootprintForKill: {}", queryMonitorConfig.getMinMemoryFootprintForKill());
        LOGGER.info("_cpuTimeBasedKillingEnabled: {}", queryMonitorConfig.isCpuTimeBasedKillingEnabled());
        LOGGER.info("_cpuTimeBasedKillingThresholdNs: {}", queryMonitorConfig.getCpuTimeBasedKillingThresholdNs());
        LOGGER.info("_workloadSleepTimeMs: {}", queryMonitorConfig.getWorkloadSleepTimeMs());
        LOGGER.info("_workloadCostEnforcementEnabled: {}", queryMonitorConfig.isWorkloadCostEnforcementEnabled());
      }
    }
  }
}
