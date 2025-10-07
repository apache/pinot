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

import it.unimi.dsi.fastutil.longs.LongLongMutablePair;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.accounting.TrackingScope;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.ResourceUsageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Accounting mechanism for thread task execution status and thread resource usage sampling
 * Design and algorithm see
 * https://docs.google.com/document/d/1Z9DYAfKznHQI9Wn8BjTWZYTcNRVGiPP0B8aEP3w_1jQ
 *
 * TODO: Functionalities in this Accountant are now supported in a more generic ResourceUsageAccountantFactory. Keeping
 * this around for backward compatibility. Will slowly phase this out in favor of ResourceUsageAccountantFactory after
 * achieving stability.
 */
public class PerQueryCPUMemAccountantFactory implements ThreadAccountantFactory {

  @Override
  public ThreadAccountant init(PinotConfiguration config, String instanceId, InstanceType instanceType) {
    return new PerQueryCPUMemResourceUsageAccountant(config, instanceId, instanceType);
  }

  public static class PerQueryCPUMemResourceUsageAccountant implements ThreadAccountant {
    private static final Logger LOGGER = LoggerFactory.getLogger(PerQueryCPUMemResourceUsageAccountant.class);

    /**
     * Executor service for the thread accounting task, slightly higher priority than normal priority
     */
    private static final String ACCOUNTANT_TASK_NAME = "CPUMemThreadAccountant";
    private static final int ACCOUNTANT_PRIORITY = 4;
    private final ExecutorService _executorService = Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread(r);
      thread.setPriority(ACCOUNTANT_PRIORITY);
      thread.setDaemon(true);
      thread.setName(ACCOUNTANT_TASK_NAME);
      return thread;
    });

    // Map from the thread executing the query to the thread resource tracker.
    protected final ConcurrentHashMap<Thread, ThreadResourceTrackerImpl> _threadTrackers = new ConcurrentHashMap<>();

    protected final ThreadLocal<ThreadResourceTrackerImpl> _threadLocalEntry = ThreadLocal.withInitial(() -> {
      ThreadResourceTrackerImpl threadTracker = new ThreadResourceTrackerImpl();
      _threadTrackers.put(Thread.currentThread(), threadTracker);
      LOGGER.debug("Adding thread to _threadLocalEntry: {}", Thread.currentThread().getName());
      return threadTracker;
    });

    // Tracks the CPU and memory usage for queries not tracked by any thread. Key is query id.
    // E.g. request ser/de where thread execution context cannot be set up beforehand; tasks already finished.
    protected final ConcurrentHashMap<String, LongLongMutablePair> _untrackedCpuMemUsage = new ConcurrentHashMap<>();

    // Tracks the query id of the inactive queries, which is used to clean up entries in _untrackedMemCpuUsage.
    protected final Set<String> _inactiveQueries = new HashSet<>();

    protected final String _instanceId;
    protected final InstanceType _instanceType;
    protected final boolean _cpuSamplingEnabled;
    protected final boolean _memorySamplingEnabled;

    protected final WatcherTask _watcherTask;

    public PerQueryCPUMemResourceUsageAccountant(PinotConfiguration config, String instanceId,
        InstanceType instanceType) {
      LOGGER.info("Initializing PerQueryCPUMemResourceUsageAccountant");
      _instanceId = instanceId;
      _instanceType = instanceType;
      boolean cpuSamplingEnabled = config.getProperty(Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING,
          Accounting.DEFAULT_ENABLE_THREAD_CPU_SAMPLING);
      if (cpuSamplingEnabled && !ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled()) {
        LOGGER.warn("Thread CPU time measurement is not enabled in the JVM, disabling CPU sampling");
        cpuSamplingEnabled = false;
      }
      _cpuSamplingEnabled = cpuSamplingEnabled;
      boolean memorySamplingEnabled = config.getProperty(Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING,
          Accounting.DEFAULT_ENABLE_THREAD_MEMORY_SAMPLING);
      if (memorySamplingEnabled && !ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled()) {
        LOGGER.warn("Thread memory measurement is not enabled in the JVM, disabling memory sampling");
        memorySamplingEnabled = false;
      }
      _memorySamplingEnabled = memorySamplingEnabled;
      _watcherTask = new WatcherTask(config);
      LOGGER.info("Initialized PerQueryCPUMemResourceUsageAccountant for {}: {} with cpuSamplingEnabled: {}, "
          + "memorySamplingEnabled: {}", instanceType, instanceId, cpuSamplingEnabled, memorySamplingEnabled);
    }

    public QueryMonitorConfig getQueryMonitorConfig() {
      return _watcherTask.getQueryMonitorConfig();
    }

    protected WatcherTask getWatcherTask() {
      return _watcherTask;
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
      QueryThreadContext threadContext = threadTracker.getThreadContext();
      assert threadContext != null;
      updateUntrackedCpuMemoryUsage(threadContext.getExecutionContext().getCid(), threadTracker.getCpuTimeNs(),
          threadTracker.getAllocatedBytes());
      threadTracker.clear();
    }

    @Override
    public void updateUntrackedResourceUsage(String identifier, long cpuTimeNs, long allocatedBytes,
        TrackingScope trackingScope) {
      // Only update for QUERY scope
      if (trackingScope != TrackingScope.QUERY) {
        return;
      }
      if (!_cpuSamplingEnabled) {
        cpuTimeNs = 0;
      }
      if (!_memorySamplingEnabled) {
        allocatedBytes = 0;
      }
      updateUntrackedCpuMemoryUsage(identifier, cpuTimeNs, allocatedBytes);
    }

    private void updateUntrackedCpuMemoryUsage(String queryId, long cpuTimeNs, long allocatedBytes) {
      _untrackedCpuMemUsage.compute(queryId, (k, v) -> v == null ? new LongLongMutablePair(cpuTimeNs, allocatedBytes)
          : v.left(v.leftLong() + cpuTimeNs).right(v.rightLong() + allocatedBytes));
    }

    @Override
    public boolean throttleQuerySubmission() {
      boolean shouldThrottle =
          _watcherTask.getHeapUsageBytes() > _watcherTask.getQueryMonitorConfig().getAlarmingLevel();
      if (shouldThrottle) {
        _watcherTask._metrics.addMeteredGlobalValue(_watcherTask._queriesThrottledMeter, 1);
      }
      return shouldThrottle;
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
    public Collection<ThreadResourceTrackerImpl> getThreadResources() {
      return _threadTrackers.values();
    }

    @Override
    public Map<String, QueryResourceTrackerImpl> getQueryResources() {
      Map<String, QueryResourceTrackerImpl> queryTrackers = new HashMap<>();

      for (ThreadResourceTrackerImpl threadTracker : _threadTrackers.values()) {
        QueryThreadContext threadContext = threadTracker.getThreadContext();
        if (threadContext != null) {
          QueryExecutionContext executionContext = threadContext.getExecutionContext();
          String queryId = executionContext.getCid();
          long cpuTimeNs = threadTracker.getCpuTimeNs();
          long allocatedBytes = threadTracker.getAllocatedBytes();
          queryTrackers.compute(queryId,
              (k, v) -> v == null ? new QueryResourceTrackerImpl(executionContext, cpuTimeNs, allocatedBytes)
                  : v.merge(cpuTimeNs, allocatedBytes));
        }
      }

      for (Map.Entry<String, QueryResourceTrackerImpl> entry : queryTrackers.entrySet()) {
        String activeQueryId = entry.getKey();
        LongLongMutablePair cpuMemUsage = _untrackedCpuMemUsage.get(activeQueryId);
        if (cpuMemUsage != null) {
          entry.getValue().merge(cpuMemUsage.leftLong(), cpuMemUsage.rightLong());
        }
      }

      return queryTrackers;
    }

    protected ThreadResourceTrackerImpl getThreadEntry() {
      return _threadLocalEntry.get();
    }

    /// Loops over all active threads, remove the dead threads, and update the inactive queries.
    private void processActiveThreads() {
      Iterator<Map.Entry<Thread, ThreadResourceTrackerImpl>> iterator = _threadTrackers.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Thread, ThreadResourceTrackerImpl> entry = iterator.next();
        Thread thread = entry.getKey();
        if (!thread.isAlive()) {
          LOGGER.debug("Thread: {} is no longer alive, removing it from _threadTrackers", thread.getName());
          iterator.remove();
        } else {
          ThreadResourceTrackerImpl threadTracker = entry.getValue();
          QueryThreadContext threadContext = threadTracker.getThreadContext();
          if (threadContext != null) {
            _inactiveQueries.remove(threadContext.getExecutionContext().getCid());
          }
        }
      }
    }

    /// Cleans up resource usage for inactive queries.
    private void cleanInactive() {
      for (String inactiveQueryId : _inactiveQueries) {
        _untrackedCpuMemUsage.remove(inactiveQueryId);
      }
      _inactiveQueries.clear();
      _inactiveQueries.addAll(_untrackedCpuMemUsage.keySet());
    }

    protected void postAggregation(Map<String, QueryResourceTrackerImpl> queryResourceUsages) {
    }

    protected void logQueryResourceUsage(Map<String, QueryResourceTrackerImpl> queryResourceUsages) {
      LOGGER.debug("Query resource usage: {} for the previous kill", queryResourceUsages);
    }

    protected void logTerminatedQuery(QueryResourceTracker queryTracker, long totalHeapUsage) {
      LOGGER.warn("Query: {} terminated. CPU Usage (ns): {}. Memory Usage (bytes): {}. Total Heap Usage (bytes): {}",
          queryTracker.getExecutionContext().getCid(), queryTracker.getCpuTimeNs(), queryTracker.getAllocatedBytes(),
          totalHeapUsage);
    }

    /**
     * The triggered level for the actions, only the highest level action will get triggered. Severity is defined by
     * the ordinal Normal(0) does not trigger any action.
     */
    protected enum TriggeringLevel {
      Normal, HeapMemoryAlarmingVerbose, CPUTimeBasedKilling, HeapMemoryCritical, HeapMemoryPanic
    }

    /**
     * A watcher task to perform usage sampling, aggregation, and query preemption
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected class WatcherTask implements Runnable, PinotClusterConfigChangeListener {
      protected final AtomicReference<QueryMonitorConfig> _queryMonitorConfig = new AtomicReference<>();

      protected volatile long _usedBytes;
      protected int _sleepTime;
      protected Map<String, QueryResourceTrackerImpl> _queryResourceUsages;
      protected TriggeringLevel _triggeringLevel = TriggeringLevel.Normal;

      // metrics class
      private final AbstractMetrics _metrics;
      private final AbstractMetrics.Meter _queryKilledMeter;
      private final AbstractMetrics.Meter _queriesThrottledMeter;
      private final AbstractMetrics.Meter _heapMemoryCriticalExceededMeter;
      private final AbstractMetrics.Meter _heapMemoryPanicExceededMeter;
      private final AbstractMetrics.Gauge _memoryUsageGauge;

      protected WatcherTask(PinotConfiguration config) {
        QueryMonitorConfig queryMonitorConfig = new QueryMonitorConfig(config, ResourceUsageUtils.getMaxHeapSize());
        _queryMonitorConfig.set(queryMonitorConfig);
        logQueryMonitorConfig(queryMonitorConfig);

        switch (_instanceType) {
          case SERVER:
            _metrics = ServerMetrics.get();
            _queryKilledMeter = ServerMeter.QUERIES_KILLED;
            _queriesThrottledMeter = ServerMeter.QUERIES_THROTTLED;
            _memoryUsageGauge = ServerGauge.JVM_HEAP_USED_BYTES;
            _heapMemoryCriticalExceededMeter = ServerMeter.HEAP_CRITICAL_LEVEL_EXCEEDED;
            _heapMemoryPanicExceededMeter = ServerMeter.HEAP_PANIC_LEVEL_EXCEEDED;
            break;
          case BROKER:
            _metrics = BrokerMetrics.get();
            _queryKilledMeter = BrokerMeter.QUERIES_KILLED;
            _queriesThrottledMeter = BrokerMeter.QUERIES_THROTTLED;
            _memoryUsageGauge = BrokerGauge.JVM_HEAP_USED_BYTES;
            _heapMemoryCriticalExceededMeter = BrokerMeter.HEAP_CRITICAL_LEVEL_EXCEEDED;
            _heapMemoryPanicExceededMeter = BrokerMeter.HEAP_PANIC_LEVEL_EXCEEDED;
            break;
          default:
            LOGGER.error("instanceType: {} not supported, using server metrics", _instanceType);
            _metrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
            _queryKilledMeter = ServerMeter.QUERIES_KILLED;
            _queriesThrottledMeter = ServerMeter.QUERIES_THROTTLED;
            _memoryUsageGauge = ServerGauge.JVM_HEAP_USED_BYTES;
            _heapMemoryCriticalExceededMeter = ServerMeter.HEAP_CRITICAL_LEVEL_EXCEEDED;
            _heapMemoryPanicExceededMeter = ServerMeter.HEAP_PANIC_LEVEL_EXCEEDED;
            break;
        }
      }

      protected QueryMonitorConfig getQueryMonitorConfig() {
        return _queryMonitorConfig.get();
      }

      protected long getHeapUsageBytes() {
        return _usedBytes;
      }

      @Override
      public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
        // Filter configs that have CommonConstants.PREFIX_SCHEDULER_PREFIX
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

        QueryMonitorConfig oldConfig = _queryMonitorConfig.get();
        QueryMonitorConfig newConfig =
            new QueryMonitorConfig(oldConfig, filteredChangedConfigs, filteredClusterConfigs);
        _queryMonitorConfig.set(newConfig);
        logQueryMonitorConfig(newConfig);
      }

      protected void logQueryMonitorConfig(QueryMonitorConfig queryMonitorConfig) {
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

      protected void runOnce() {
        LOGGER.debug("Running timed task for PerQueryCPUMemAccountant.");
        QueryMonitorConfig config = _queryMonitorConfig.get();
        _sleepTime = config.getNormalSleepTime();
        _queryResourceUsages = null;
        try {
          // Get the metrics used for triggering the kill
          collectTriggerMetrics();
          // Evaluate the triggering levels of query preemption
          evalTriggers();
          // Prioritize the panic check, kill ALL QUERIES immediately if triggered
          if (_triggeringLevel == TriggeringLevel.HeapMemoryPanic) {
            killAllQueries();
            processActiveThreads();
            return;
          }
          // Refresh thread usage and aggregate to per query usage if triggered
          processActiveThreads();
          if (_triggeringLevel.ordinal() > TriggeringLevel.Normal.ordinal()) {
            _queryResourceUsages = getQueryResources();
          }
          // post aggregation function
          postAggregation(_queryResourceUsages);
          // Act on one triggered actions
          triggeredActions();
        } catch (Exception e) {
          LOGGER.error("Caught exception while executing stats aggregation and query kill", e);
        } finally {
          LOGGER.debug(_queryResourceUsages == null ? "_queryResourceUsages : null" : _queryResourceUsages.toString());
          LOGGER.debug("_threadTrackers size: {}", _threadTrackers.size());

          // Publish server heap usage metrics
          if (config.isPublishHeapUsageMetric()) {
            _metrics.setValueOfGlobalGauge(_memoryUsageGauge, _usedBytes);
          }
          // Clean inactive query stats
          cleanInactive();
        }
      }

      protected void collectTriggerMetrics() {
        _usedBytes = ResourceUsageUtils.getUsedHeapSize();
        LOGGER.debug("Heap used bytes: {}", _usedBytes);
      }

      /**
       * Evaluate triggering levels of query preemption
       * Triggers should be mutually exclusive and evaluated following level high -> low
       */
      protected void evalTriggers() {
        TriggeringLevel previousTriggeringLevel = _triggeringLevel;

        // Compute the new triggering level based on the current heap usage
        QueryMonitorConfig config = _queryMonitorConfig.get();
        _triggeringLevel =
            config.isCpuTimeBasedKillingEnabled() ? TriggeringLevel.CPUTimeBasedKilling : TriggeringLevel.Normal;
        if (_usedBytes > config.getPanicLevel()) {
          _triggeringLevel = TriggeringLevel.HeapMemoryPanic;
          _metrics.addMeteredGlobalValue(_heapMemoryPanicExceededMeter, 1);
        } else if (_usedBytes > config.getCriticalLevel()) {
          _triggeringLevel = TriggeringLevel.HeapMemoryCritical;
          _metrics.addMeteredGlobalValue(_heapMemoryCriticalExceededMeter, 1);
        } else if (_usedBytes > config.getAlarmingLevel()) {
          _sleepTime = config.getAlarmingSleepTime();
          // For debugging
          if (LOGGER.isDebugEnabled() && _triggeringLevel == TriggeringLevel.Normal) {
            _triggeringLevel = TriggeringLevel.HeapMemoryAlarmingVerbose;
          }
        }

        // Log the triggering level change
        if (previousTriggeringLevel != _triggeringLevel) {
          switch (_triggeringLevel) {
            case HeapMemoryPanic:
              LOGGER.error("Heap used bytes: {} exceeds panic level: {}, killing all queries", _usedBytes,
                  config.getPanicLevel());
              break;
            case HeapMemoryCritical:
              LOGGER.warn("Heap used bytes: {} exceeds critical level: {}, killing most expensive query", _usedBytes,
                  config.getCriticalLevel());
              if (!_memorySamplingEnabled) {
                LOGGER.error("Unable to terminate queries as memory tracking is not enabled");
              }
              break;
            case CPUTimeBasedKilling:
              LOGGER.info("Entering CPU time based killing mode, killing queries that exceed threshold (ns): {}",
                  config.getCpuTimeBasedKillingThresholdNs());
              if (!_cpuSamplingEnabled) {
                LOGGER.error("Unable to terminate queries as CPU time tracking is not enabled");
              }
              break;
            case HeapMemoryAlarmingVerbose:
              LOGGER.debug("Heap used bytes: {} exceeds alarming level: {}", _usedBytes, config.getAlarmingLevel());
              break;
            case Normal:
              LOGGER.info("Heap used bytes: {} drops to safe zone, entering to normal mode", _usedBytes);
              break;
            default:
              throw new IllegalStateException("Unsupported triggering level: " + _triggeringLevel);
          }
        }
      }

      protected void triggeredActions() {
        switch (_triggeringLevel) {
          case HeapMemoryCritical:
            killMostExpensiveQuery();
            break;
          case CPUTimeBasedKilling:
            killCPUTimeExceedQueries();
            break;
          case HeapMemoryAlarmingVerbose:
            LOGGER.debug("Query resource usage: {}", _queryResourceUsages);
            break;
          default:
            break;
        }
      }

      protected void killAllQueries() {
        QueryMonitorConfig config = _queryMonitorConfig.get();
        if (!config.isOomKillQueryEnabled()) {
          LOGGER.error("Trying to kill all queries, not killing them because OOM kill is not enabled");
          return;
        }

        String errorMessage =
            "Panic OOM killed on " + _instanceType + ": " + _instanceId + " as heap used bytes: " + _usedBytes
                + " exceeds panic level: " + config.getPanicLevel();
        Set<String> killedQueries = new HashSet<>();
        for (ThreadResourceTrackerImpl threadTracker : _threadTrackers.values()) {
          QueryThreadContext threadContext = threadTracker.getThreadContext();
          if (threadContext != null) {
            QueryExecutionContext executionContext = threadContext.getExecutionContext();
            if (executionContext.terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, errorMessage)) {
              killedQueries.add(executionContext.getCid());
            }
          }
        }
        LOGGER.error("Killed {} queries: {} due to OOM panic", killedQueries.size(), killedQueries);
        if (config.isQueryKilledMetricEnabled()) {
          _metrics.addMeteredGlobalValue(_queryKilledMeter, killedQueries.size());
        }
      }

      /**
       * Kill the query with the highest cost (memory footprint/cpu time/...)
       */
      protected void killMostExpensiveQuery() {
        if (!_memorySamplingEnabled) {
          return;
        }
        if (!_queryResourceUsages.isEmpty()) {
          QueryResourceTrackerImpl maxUsageQueryTracker = _queryResourceUsages.values()
              .stream()
              .filter(queryTracker -> queryTracker.getExecutionContext().getTerminateException() == null)
              .max(Comparator.comparing(QueryResourceTrackerImpl::getAllocatedBytes))
              .orElse(null);
          if (maxUsageQueryTracker != null) {
            QueryExecutionContext executionContext = maxUsageQueryTracker.getExecutionContext();
            String queryId = executionContext.getCid();
            long allocatedBytes = maxUsageQueryTracker.getAllocatedBytes();
            QueryMonitorConfig config = _queryMonitorConfig.get();
            if (allocatedBytes > config.getMinMemoryFootprintForKill()) {
              if (config.isOomKillQueryEnabled()) {
                String errorMessage = "OOM killed on " + _instanceType + ": " + _instanceId
                    + " as the most expensive query with allocated bytes: " + allocatedBytes;
                executionContext.terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, errorMessage);
                if (config.isQueryKilledMetricEnabled()) {
                  _metrics.addMeteredGlobalValue(_queryKilledMeter, 1);
                }
                logTerminatedQuery(maxUsageQueryTracker, _usedBytes);
              } else {
                LOGGER.warn("Query: {} got picked because it allocated: {} bytes of memory, "
                    + "not killing it because OOM kill is not enabled", queryId, allocatedBytes);
              }
            } else {
              LOGGER.warn(
                  "Query: {} has most allocated bytes: {}, but below the minimum memory footprint for kill: {}, "
                      + "skipping query kill", queryId, allocatedBytes, config.getMinMemoryFootprintForKill());
            }
          }
          logQueryResourceUsage(_queryResourceUsages);
        } else {
          LOGGER.debug("Critical level memory usage query killing - No active query to kill");
        }
      }

      protected void killCPUTimeExceedQueries() {
        if (!_cpuSamplingEnabled) {
          return;
        }
        if (!_queryResourceUsages.isEmpty()) {
          QueryMonitorConfig config = _queryMonitorConfig.get();
          for (QueryResourceTrackerImpl queryTracker : _queryResourceUsages.values()) {
            QueryExecutionContext executionContext = queryTracker.getExecutionContext();
            if (executionContext.getTerminateException() != null) {
              continue;
            }
            String queryId = executionContext.getCid();
            long cpuTimeNs = queryTracker.getCpuTimeNs();
            if (cpuTimeNs > config.getCpuTimeBasedKillingThresholdNs()) {
              LOGGER.debug("Current task status recorded is {}. Query {} got picked because using {} ns of cpu time,"
                      + " greater than threshold {}", _threadTrackers, queryId, cpuTimeNs,
                  config.getCpuTimeBasedKillingThresholdNs());
              String errorMessage =
                  "CPU time based killed on " + _instanceType + ": " + _instanceId + " as it used: " + cpuTimeNs
                      + " ns of CPU time (exceeding threshold: " + config.getCpuTimeBasedKillingThresholdNs() + ")";
              executionContext.terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, errorMessage);
              if (config.isQueryKilledMetricEnabled()) {
                _metrics.addMeteredGlobalValue(_queryKilledMeter, 1);
              }
              logTerminatedQuery(queryTracker, _usedBytes);
            }
          }
          logQueryResourceUsage(_queryResourceUsages);
        } else {
          LOGGER.debug("CPU Time based query killing - No active query to kill");
        }
      }
    }
  }
}
