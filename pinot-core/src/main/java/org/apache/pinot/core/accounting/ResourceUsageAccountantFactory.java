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
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.accounting.TrackingScope;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO: Incorporate query OOM kill handling in PerQueryCPUMemAccountantFactory into this class
public class ResourceUsageAccountantFactory implements ThreadAccountantFactory {

  @Override
  public ResourceUsageAccountant init(PinotConfiguration config, String instanceId, InstanceType instanceType) {
    return new ResourceUsageAccountant(config, instanceId, instanceType);
  }

  public static class ResourceUsageAccountant implements ThreadResourceUsageAccountant {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUsageAccountant.class);
    private static final String ACCOUNTANT_TASK_NAME = "ResourceUsageAccountant";
    private static final int ACCOUNTANT_PRIORITY = 4;

    private final ExecutorService _executorService = Executors.newFixedThreadPool(1, r -> {
      Thread thread = new Thread(r);
      thread.setPriority(ACCOUNTANT_PRIORITY);
      thread.setDaemon(true);
      thread.setName(ACCOUNTANT_TASK_NAME);
      return thread;
    });

    // the map to track stats entry for each thread, the entry will automatically be added when one calls
    // setThreadResourceUsageProvider on the thread, including but not limited to
    // server worker thread, runner thread, broker jetty thread, or broker netty thread
    private final ConcurrentHashMap<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> _threadEntriesMap =
        new ConcurrentHashMap<>();

    private final ThreadLocal<CPUMemThreadLevelAccountingObjects.ThreadEntry> _threadLocalEntry =
        ThreadLocal.withInitial(() -> {
          CPUMemThreadLevelAccountingObjects.ThreadEntry ret = new CPUMemThreadLevelAccountingObjects.ThreadEntry();
          _threadEntriesMap.put(Thread.currentThread(), ret);
          LOGGER.debug("Adding thread to _threadLocalEntry: {}", Thread.currentThread().getName());
          return ret;
        });

    // track thread cpu time
    private final boolean _isThreadCPUSamplingEnabled;

    // track memory usage
    private final boolean _isThreadMemorySamplingEnabled;

    private final WatcherTask _watcherTask;

    private final EnumMap<TrackingScope, ResourceAggregator> _resourceAggregators;

    public ResourceUsageAccountant(PinotConfiguration config, String instanceId, InstanceType instanceType) {
      LOGGER.info("Initializing ResourceUsageAccountant");

      boolean threadCpuTimeMeasurementEnabled = ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled();
      boolean threadMemoryMeasurementEnabled = ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled();
      LOGGER.info("threadCpuTimeMeasurementEnabled: {}, threadMemoryMeasurementEnabled: {}",
          threadCpuTimeMeasurementEnabled, threadMemoryMeasurementEnabled);

      boolean cpuSamplingConfig = config.getProperty(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING,
          CommonConstants.Accounting.DEFAULT_ENABLE_THREAD_CPU_SAMPLING);
      boolean memorySamplingConfig =
          config.getProperty(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING,
              CommonConstants.Accounting.DEFAULT_ENABLE_THREAD_MEMORY_SAMPLING);
      LOGGER.info("cpuSamplingConfig: {}, memorySamplingConfig: {}", cpuSamplingConfig, memorySamplingConfig);

      _isThreadCPUSamplingEnabled = cpuSamplingConfig && threadCpuTimeMeasurementEnabled;
      _isThreadMemorySamplingEnabled = memorySamplingConfig && threadMemoryMeasurementEnabled;
      LOGGER.info("_isThreadCPUSamplingEnabled: {}, _isThreadMemorySamplingEnabled: {}", _isThreadCPUSamplingEnabled,
          _isThreadMemorySamplingEnabled);

      _watcherTask = new WatcherTask();

      _resourceAggregators = new EnumMap<>(TrackingScope.class);

      // Add all aggregators. Configs of enabling/disabling cost collection/enforcement are handled in the aggregators.
      _resourceAggregators.put(TrackingScope.WORKLOAD,
          new WorkloadAggregator(_isThreadCPUSamplingEnabled, _isThreadMemorySamplingEnabled, config, instanceType,
              instanceId));
      _resourceAggregators.put(TrackingScope.QUERY,
          new QueryAggregator(_isThreadCPUSamplingEnabled, _isThreadMemorySamplingEnabled, config, instanceType,
              instanceId));
    }

    @Override
    public Collection<? extends ThreadResourceTracker> getThreadResources() {
      return _threadEntriesMap.values();
    }

    @Override
    public void sampleUsage() {
      sampleThreadBytesAllocated();
      sampleThreadCPUTime();
    }

    @Override
    public boolean isAnchorThreadInterrupted() {
      ThreadExecutionContext context = _threadLocalEntry.get().getCurrentThreadTaskStatus();
      if (context != null && context.getAnchorThread() != null) {
        return context.getAnchorThread().isInterrupted();
      }

      return false;
    }

    @Override
    public void setupRunner(String queryId, int taskId, ThreadExecutionContext.TaskType taskType, String workloadName) {
      _threadLocalEntry.get()._errorStatus.set(null);
      if (queryId != null) {
        _threadLocalEntry.get()
            .setThreadTaskStatus(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID, taskType, Thread.currentThread(),
                workloadName);
      }
    }

    @Override
    public void setupWorker(int taskId, ThreadExecutionContext.TaskType taskType,
        @Nullable ThreadExecutionContext parentContext) {
      _threadLocalEntry.get()._errorStatus.set(null);
      if (parentContext != null && parentContext.getQueryId() != null && parentContext.getAnchorThread() != null) {
        _threadLocalEntry.get()
            .setThreadTaskStatus(parentContext.getQueryId(), taskId, parentContext.getTaskType(),
                parentContext.getAnchorThread(), parentContext.getWorkloadName());
      }
    }

    /**
     * for testing only
     */
    public int getEntryCount() {
      return _threadEntriesMap.size();
    }

    /**
     * This function aggregates resource usage from all active threads and groups by queryId.
     * @return A map of query id, QueryResourceTracker.
     */
    @Override
    public Map<String, ? extends QueryResourceTracker> getQueryResources() {
      QueryAggregator queryAggregator = (QueryAggregator) _resourceAggregators.get(TrackingScope.QUERY);
      return queryAggregator.getQueryResources(_threadEntriesMap);
    }

    @Override
    public void updateQueryUsageConcurrently(String identifier, long cpuTimeNs, long memoryAllocatedBytes,
                                             TrackingScope trackingScope) {
      ResourceAggregator resourceAggregator = _resourceAggregators.get(trackingScope);
      if (_isThreadCPUSamplingEnabled) {
        resourceAggregator.updateConcurrentCpuUsage(identifier, cpuTimeNs);
      }
      if (_isThreadMemorySamplingEnabled) {
        resourceAggregator.updateConcurrentMemUsage(identifier, memoryAllocatedBytes);
      }
    }

    /**
     * This is to be called from a worker or a runner thread to update its corresponding cpu usage entry
     */
    @SuppressWarnings("ConstantConditions")
    public void sampleThreadCPUTime() {
      if (_isThreadCPUSamplingEnabled) {
        _threadLocalEntry.get().updateCpuSnapshot();
      }
    }

    /**
     * This is to be called from a worker or a runner thread to update its corresponding memory usage entry
     */
    @SuppressWarnings("ConstantConditions")
    public void sampleThreadBytesAllocated() {
      if (_isThreadMemorySamplingEnabled) {
        _threadLocalEntry.get().updateMemorySnapshot();
      }
    }

    @Override
    public ThreadExecutionContext getThreadExecutionContext() {
      return _threadLocalEntry.get().getCurrentThreadTaskStatus();
    }

    /**
     * clears thread accounting info once a runner/worker thread has finished a particular run
     */
    @SuppressWarnings("ConstantConditions")
    @Override
    public void clear() {
      CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = _threadLocalEntry.get();
      // clear task info + stats
      threadEntry.setToIdle();
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
    public Exception getErrorStatus() {
      return _threadLocalEntry.get()._errorStatus.getAndSet(null);
    }

    public List<CPUMemThreadLevelAccountingObjects.ThreadEntry> getAnchorThreadEntries() {
      List<CPUMemThreadLevelAccountingObjects.ThreadEntry> anchorThreadEntries = new ArrayList<>();

      for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : _threadEntriesMap.entrySet()) {
        CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = entry.getValue();
        CPUMemThreadLevelAccountingObjects.TaskEntry taskEntry = threadEntry.getCurrentThreadTaskStatus();
        if (taskEntry != null && taskEntry.isAnchorThread()) {
          anchorThreadEntries.add(threadEntry);
        }
      }
      return anchorThreadEntries;
    }

    class WatcherTask implements Runnable {
      WatcherTask() {
      }

      @Override
      public void run() {
        LOGGER.debug("Running timed task for {}", this.getClass().getName());
        while (!Thread.currentThread().isInterrupted()) {
          try {
            // Preaggregation.
            runPreAggregation();

            // Aggregation
            runAggregation();

            // Postaggregation
            runPostAggregation();
          } catch (Exception e) {
            LOGGER.error("Error in WatcherTask", e);
            // TODO: Add a metric to track the number of watcher task errors.
          } finally {
            try {
              LOGGER.debug("_threadEntriesMap size: {}", _threadEntriesMap.size());

              for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
                resourceAggregator.cleanUpPostAggregation();
              }
              // Get sleeptime from both resourceAggregators. Pick the minimum. PerQuery Accountant modifies the sleep
              // time when condition is critical.
              int sleepTime = Integer.MAX_VALUE;
              for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
                sleepTime = Math.min(sleepTime, resourceAggregator.getAggregationSleepTimeMs());
              }

              Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }

      private void runPreAggregation() {
        // Call the pre-aggregation methods for each ResourceAggregator.
        for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
          resourceAggregator.preAggregate(getAnchorThreadEntries());
        }
      }

      private void runAggregation() {
        for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : _threadEntriesMap.entrySet()) {
          CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = entry.getValue();
          CPUMemThreadLevelAccountingObjects.TaskEntry currentQueryTask = threadEntry.getCurrentThreadTaskStatus();
          Thread thread = entry.getKey();
          LOGGER.trace("tid: {}, task: {}", thread.getId(), currentQueryTask);

          for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
            resourceAggregator.aggregate(entry.getKey(), threadEntry);
          }

          long currentCPUCost = _isThreadCPUSamplingEnabled ? threadEntry._currentThreadCPUTimeSampleMS : 0;
          long currentMemoryCost =
              _isThreadMemorySamplingEnabled ? threadEntry._currentThreadMemoryAllocationSampleBytes : 0;
          threadEntry._previousThreadTaskStatus = currentQueryTask;
          threadEntry._previousThreadCPUTimeSampleMS = currentCPUCost;
          threadEntry._previousThreadMemoryAllocationSampleBytes = currentMemoryCost;

          if (!thread.isAlive()) {
            _threadEntriesMap.remove(thread);
            LOGGER.debug("Removing thread from _threadLocalEntry: {}", thread.getName());
          }
        }
      }

      private void runPostAggregation() {
        for (ResourceAggregator resourceAggregator : _resourceAggregators.values()) {
          resourceAggregator.postAggregate();
        }
      }
    }
  }
}
