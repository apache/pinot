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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.accounting.MseCancelCallback;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.accounting.TrackingScope;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
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
  public ThreadResourceUsageAccountant init(PinotConfiguration config, String instanceId, InstanceType instanceType) {
    return new PerQueryCPUMemResourceUsageAccountant(config, instanceId, instanceType);
  }

  public static class PerQueryCPUMemResourceUsageAccountant implements ThreadResourceUsageAccountant {
    private static final Logger LOGGER = LoggerFactory.getLogger(PerQueryCPUMemResourceUsageAccountant.class);
    private static final boolean IS_DEBUG_MODE_ENABLED = LOGGER.isDebugEnabled();
    /**
     * Executor service for the thread accounting task, slightly higher priority than normal priority
     */
    private static final String ACCOUNTANT_TASK_NAME = "CPUMemThreadAccountant";
    private static final int ACCOUNTANT_PRIORITY = 4;
    private final ExecutorService _executorService = Executors.newFixedThreadPool(1, r -> {
      Thread thread = new Thread(r);
      thread.setPriority(ACCOUNTANT_PRIORITY);
      thread.setDaemon(true);
      thread.setName(ACCOUNTANT_TASK_NAME);
      return thread;
    });

    protected final PinotConfiguration _config;

    // the map to track stats entry for each thread, the entry will automatically be added when one calls
    // setThreadResourceUsageProvider on the thread, including but not limited to
    // server worker thread, runner thread, broker jetty thread, or broker netty thread
    protected final ConcurrentHashMap<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> _threadEntriesMap
        = new ConcurrentHashMap<>();

    // For one time concurrent update of stats. This is to provide stats collection for parts that are not
    // performance sensitive and query_id is not known beforehand (e.g. broker inbound netty thread)
    protected final ConcurrentHashMap<String, Long> _concurrentTaskCPUStatsAggregator = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, Long> _concurrentTaskMemStatsAggregator = new ConcurrentHashMap<>();

    // for stats aggregation of finished (worker) threads when the runner is still running
    protected final HashMap<String, Long> _finishedTaskCPUStatsAggregator = new HashMap<>();
    protected final HashMap<String, Long> _finishedTaskMemStatsAggregator = new HashMap<>();

    Cache<String, MseCancelCallback> _queryCancelCallbacks;

    protected final ThreadLocal<CPUMemThreadLevelAccountingObjects.ThreadEntry> _threadLocalEntry
        = ThreadLocal.withInitial(() -> {
          CPUMemThreadLevelAccountingObjects.ThreadEntry ret =
              new CPUMemThreadLevelAccountingObjects.ThreadEntry();
          addThreadEntry(Thread.currentThread(), ret);
          LOGGER.debug("Adding thread to _threadLocalEntry: {}", Thread.currentThread().getName());
          return ret;
        }
    );

    // track thread cpu time
    protected final boolean _isThreadCPUSamplingEnabled;

    // track memory usage
    protected final boolean _isThreadMemorySamplingEnabled;

    protected final Set<String> _inactiveQuery;

    protected Set<String> _cancelSentQueries;

    // the periodical task that aggregates and preempts queries
    protected final WatcherTask _watcherTask;

    // instance id of the current instance, for logging purpose
    protected final String _instanceId;

    protected final InstanceType _instanceType;

    protected PerQueryCPUMemResourceUsageAccountant(PinotConfiguration config, boolean isThreadCPUSamplingEnabled,
        boolean isThreadMemorySamplingEnabled, boolean isThreadSamplingEnabledForMSE, Set<String> inactiveQuery,
        String instanceId, InstanceType instanceType) {
      _config = config;
      _isThreadCPUSamplingEnabled = isThreadCPUSamplingEnabled;
      _isThreadMemorySamplingEnabled = isThreadMemorySamplingEnabled;
      _inactiveQuery = inactiveQuery;
      _instanceId = instanceId;
      _instanceType = instanceType;
      _cancelSentQueries = new HashSet<>();
      _watcherTask = createWatcherTask();
      _queryCancelCallbacks = CacheBuilder.newBuilder().build();
    }

    public PerQueryCPUMemResourceUsageAccountant(PinotConfiguration config, String instanceId,
        InstanceType instanceType) {

      LOGGER.info("Initializing PerQueryCPUMemResourceUsageAccountant");
      _config = config;
      _instanceId = instanceId;

      boolean threadCpuTimeMeasurementEnabled = ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled();
      boolean threadMemoryMeasurementEnabled = ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled();
      LOGGER.info("threadCpuTimeMeasurementEnabled: {}, threadMemoryMeasurementEnabled: {}",
          threadCpuTimeMeasurementEnabled, threadMemoryMeasurementEnabled);

      boolean cpuSamplingConfig =
          config.getProperty(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING,
              CommonConstants.Accounting.DEFAULT_ENABLE_THREAD_CPU_SAMPLING);
      boolean memorySamplingConfig =
          config.getProperty(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING,
              CommonConstants.Accounting.DEFAULT_ENABLE_THREAD_MEMORY_SAMPLING);
      LOGGER.info("cpuSamplingConfig: {}, memorySamplingConfig: {}",
          cpuSamplingConfig, memorySamplingConfig);

      _instanceType = instanceType;
      _isThreadCPUSamplingEnabled = cpuSamplingConfig && threadCpuTimeMeasurementEnabled;
      _isThreadMemorySamplingEnabled = memorySamplingConfig && threadMemoryMeasurementEnabled;
      LOGGER.info("_isThreadCPUSamplingEnabled: {}, _isThreadMemorySamplingEnabled: {}", _isThreadCPUSamplingEnabled,
          _isThreadMemorySamplingEnabled);

      _queryCancelCallbacks = CacheBuilder.newBuilder().maximumSize(
              config.getProperty(CommonConstants.Accounting.CONFIG_OF_CANCEL_CALLBACK_CACHE_MAX_SIZE,
                  CommonConstants.Accounting.DEFAULT_CANCEL_CALLBACK_CACHE_MAX_SIZE)).expireAfterWrite(
              config.getProperty(CommonConstants.Accounting.CONFIG_OF_CANCEL_CALLBACK_CACHE_EXPIRY_SECONDS,
                  CommonConstants.Accounting.DEFAULT_CANCEL_CALLBACK_CACHE_EXPIRY_SECONDS),
              TimeUnit.SECONDS)  // backstop for stuck entries
          .build();

      // task/query tracking
      _inactiveQuery = new HashSet<>();
      _cancelSentQueries = new HashSet<>();
      _watcherTask = createWatcherTask();
    }

    protected WatcherTask createWatcherTask() {
      return new WatcherTask();
    }

    public QueryMonitorConfig getQueryMonitorConfig() {
      return _watcherTask.getQueryMonitorConfig();
    }

    @Override
    public Collection<? extends ThreadResourceTracker> getThreadResources() {
      return _threadEntriesMap.values();
    }

    /**
     * This function aggregates resource usage from all active threads and groups by queryId.
     * @return A map of query id, QueryResourceTracker.
     */
    @Override
    public Map<String, ? extends QueryResourceTracker> getQueryResources() {
      return getQueryResourcesImpl();
    }

    protected Map<String, AggregatedStats> getQueryResourcesImpl() {
      HashMap<String, AggregatedStats> ret = new HashMap<>();

      // for each {pqr, pqw}
      for (CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry : _threadEntriesMap.values()) {
        CPUMemThreadLevelAccountingObjects.TaskEntry currentTaskStatus = threadEntry.getCurrentThreadTaskStatus();

        // if current thread is not idle
        if (currentTaskStatus != null) {
          // extract query id from queryTask string
          String queryId = currentTaskStatus.getQueryId();
          if (queryId != null) {
            Thread anchorThread = currentTaskStatus.getAnchorThread();
            boolean isAnchorThread = currentTaskStatus.isAnchorThread();
            long currentCPUSample = _isThreadCPUSamplingEnabled ? threadEntry._currentThreadCPUTimeSampleMS : 0;
            long currentMemSample =
                _isThreadMemorySamplingEnabled ? threadEntry._currentThreadMemoryAllocationSampleBytes : 0;
            ret.compute(queryId,
                (k, v) -> v == null ? new AggregatedStats(currentCPUSample, currentMemSample, anchorThread,
                    isAnchorThread, threadEntry._errorStatus, queryId)
                    : v.merge(currentCPUSample, currentMemSample, isAnchorThread,
                        threadEntry._errorStatus));
          }
        }
      }

      // if triggered, accumulate stats of finished tasks of each active query
      for (Map.Entry<String, AggregatedStats> queryIdResult : ret.entrySet()) {
        String activeQueryId = queryIdResult.getKey();
        long accumulatedCPUValue =
            _isThreadCPUSamplingEnabled ? _finishedTaskCPUStatsAggregator.getOrDefault(activeQueryId, 0L) : 0;
        long concurrentCPUValue =
            _isThreadCPUSamplingEnabled ? _concurrentTaskCPUStatsAggregator.getOrDefault(activeQueryId, 0L) : 0;
        long accumulatedMemValue =
            _isThreadMemorySamplingEnabled ? _finishedTaskMemStatsAggregator.getOrDefault(activeQueryId, 0L) : 0;
        long concurrentMemValue =
            _isThreadMemorySamplingEnabled ? _concurrentTaskMemStatsAggregator.getOrDefault(activeQueryId, 0L) : 0;
        queryIdResult.getValue()
            .merge(accumulatedCPUValue + concurrentCPUValue, accumulatedMemValue + concurrentMemValue, false, null);
      }
      return ret;
    }

    @Override
    public void sampleUsage() {
      sampleThreadBytesAllocated();
      sampleThreadCPUTime();
    }

    @Override
    public boolean throttleQuerySubmission() {
      return getWatcherTask().getHeapUsageBytes() > getWatcherTask().getQueryMonitorConfig().getAlarmingLevel();
    }

    @Override
    public void registerMseCancelCallback(String queryId, MseCancelCallback callback) {
      _queryCancelCallbacks.put(queryId, callback);
    }

    @Nullable
    public MseCancelCallback getQueryCancelCallback(String queryId) {
      return _queryCancelCallbacks.getIfPresent(queryId);
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
    public boolean isQueryTerminated() {
      QueryMonitorConfig config = _watcherTask.getQueryMonitorConfig();
      if (config.isThreadSelfTerminate() && _watcherTask.getHeapUsageBytes() > config.getPanicLevel()) {
        logSelfTerminatedQuery(_threadLocalEntry.get().getQueryId(), Thread.currentThread());
        return true;
      }
      return false;
    }

    @Override
    public void updateQueryUsageConcurrently(String queryId, long cpuTimeNs, long memoryAllocatedBytes,
                                             TrackingScope trackingScope) {
      if (trackingScope != TrackingScope.QUERY) {
        // only update for QUERY scope
        return;
      }
      if (_isThreadCPUSamplingEnabled) {
        _concurrentTaskCPUStatsAggregator.compute(queryId,
            (key, value) -> (value == null) ? cpuTimeNs : (value + cpuTimeNs));
      }
      if (_isThreadMemorySamplingEnabled) {
        _concurrentTaskMemStatsAggregator.compute(queryId,
            (key, value) -> (value == null) ? memoryAllocatedBytes : (value + memoryAllocatedBytes));
      }
    }

    /**
     * The thread would need to do {@code setThreadResourceUsageProvider} first upon it is scheduled.
     * This is to be called from a worker or a runner thread to update its corresponding cpu usage entry
     */
    @SuppressWarnings("ConstantConditions")
    public void sampleThreadCPUTime() {
      if (_isThreadCPUSamplingEnabled) {
        _threadLocalEntry.get().updateCpuSnapshot();
      }
    }

    /**
     * The thread would need to do {@code setThreadResourceUsageProvider} first upon it is scheduled.
     * This is to be called from a worker or a runner thread to update its corresponding memory usage entry
     */
    @SuppressWarnings("ConstantConditions")
    public void sampleThreadBytesAllocated() {
      if (_isThreadMemorySamplingEnabled) {
        _threadLocalEntry.get().updateMemorySnapshot();
      }
    }

    @Override
    public void setupRunner(@Nullable String queryId, int taskId, ThreadExecutionContext.TaskType taskType,
        String workloadName) {
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
        _threadLocalEntry.get().setThreadTaskStatus(parentContext.getQueryId(), taskId, parentContext.getTaskType(),
            parentContext.getAnchorThread(), parentContext.getWorkloadName());
      }
    }

    @Override
    @Nullable
    public ThreadExecutionContext getThreadExecutionContext() {
      return _threadLocalEntry.get().getCurrentThreadTaskStatus();
    }

    public CPUMemThreadLevelAccountingObjects.ThreadEntry getThreadEntry() {
      return _threadLocalEntry.get();
    }

    public void addThreadEntry(Thread thread, CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry) {
      _threadEntriesMap.put(thread, threadEntry);
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

    public WatcherTask getWatcherTask() {
      return _watcherTask;
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

    /**
     * remove in active queries from _finishedTaskStatAggregator
     */
    public void cleanInactive() {
      for (String inactiveQueryId : _inactiveQuery) {
        if (_isThreadCPUSamplingEnabled) {
          _finishedTaskCPUStatsAggregator.remove(inactiveQueryId);
          _concurrentTaskCPUStatsAggregator.remove(inactiveQueryId);
        }
        if (_isThreadMemorySamplingEnabled) {
          _finishedTaskMemStatsAggregator.remove(inactiveQueryId);
          _concurrentTaskMemStatsAggregator.remove(inactiveQueryId);
        }
        _cancelSentQueries.remove(inactiveQueryId);
        _queryCancelCallbacks.invalidate(inactiveQueryId);
      }
      _inactiveQuery.clear();
      if (_isThreadCPUSamplingEnabled) {
        _inactiveQuery.addAll(_finishedTaskCPUStatsAggregator.keySet());
        _inactiveQuery.addAll(_concurrentTaskCPUStatsAggregator.keySet());
      }
      if (_isThreadMemorySamplingEnabled) {
        _inactiveQuery.addAll(_finishedTaskMemStatsAggregator.keySet());
        _inactiveQuery.addAll(_concurrentTaskMemStatsAggregator.keySet());
      }
    }

    public Set<String> getInactiveQueries() {
      return Collections.unmodifiableSet(_inactiveQuery);
    }

    public Set<String> getCancelSentQueries() {
      return Collections.unmodifiableSet(_cancelSentQueries);
    }

    /**
     * This function moves finished tasks through 2 stages.
     * Initially, task metadata is stored in threadEntry._currentThreadTaskStatus etc.
     * In the first step, it moves this metadata to threadEntry._previousThreadTaskStatus etc.
     *
     * At the same time, the function moves the information in _threadEntry._previousThreadTaskStatus into the second
     * state. It is aggregated into _finishedTaskCPUStatsAggregator and _finishedTaskMemStatsAggregator.
     *
     * Finally it cleans up _inactiveQueries AND _threadEntriesMap.
     */
    public void reapFinishedTasks() {
      Set<String> cancellingQueries = new HashSet<>();

      // for each {pqr, pqw}
      for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : _threadEntriesMap.entrySet()) {
        // sample current usage
        CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = entry.getValue();
        long currentCPUSample = _isThreadCPUSamplingEnabled ? threadEntry._currentThreadCPUTimeSampleMS : 0;
        long currentMemSample =
            _isThreadMemorySamplingEnabled ? threadEntry._currentThreadMemoryAllocationSampleBytes : 0;
        // sample current running task status
        CPUMemThreadLevelAccountingObjects.TaskEntry currentTaskStatus = threadEntry.getCurrentThreadTaskStatus();

        // get last task on the thread
        CPUMemThreadLevelAccountingObjects.TaskEntry lastQueryTask = threadEntry._previousThreadTaskStatus;

        // accumulate recorded previous stat to it's _finishedTaskStatAggregator
        // if the last task on the same thread has finished
        if (!(currentTaskStatus == lastQueryTask)) {
          // set previous value to current task stats
          threadEntry._previousThreadTaskStatus = currentTaskStatus;
          if (lastQueryTask != null) {
            String lastQueryId = lastQueryTask.getQueryId();
            if (_isThreadCPUSamplingEnabled) {
              long lastSample = threadEntry._previousThreadCPUTimeSampleMS;
              _finishedTaskCPUStatsAggregator.merge(lastQueryId, lastSample, Long::sum);
            }
            if (_isThreadMemorySamplingEnabled) {
              long lastSample = threadEntry._previousThreadMemoryAllocationSampleBytes;
              _finishedTaskMemStatsAggregator.merge(lastQueryId, lastSample, Long::sum);
            }
          }
        }

        // record current usage values for future accumulation if this task is done
        if (_isThreadCPUSamplingEnabled) {
          threadEntry._previousThreadCPUTimeSampleMS = currentCPUSample;
        }
        if (_isThreadMemorySamplingEnabled) {
          threadEntry._previousThreadMemoryAllocationSampleBytes = currentMemSample;
        }

        // if current thread is not idle
        if (currentTaskStatus != null) {
          // extract query id from queryTask string
          String queryId = currentTaskStatus.getQueryId();
          // update inactive queries for cleanInactive()
          _inactiveQuery.remove(queryId);
          // If query is in cancelling set, retain it.
          if (_cancelSentQueries.contains(queryId)) {
            cancellingQueries.add(queryId);
          }
        }

        Thread thread = entry.getKey();
        if (!thread.isAlive()) {
          LOGGER.debug("Thread: {} is no longer alive, removing it from _threadEntriesMap", thread.getName());
          _threadEntriesMap.remove(thread);
        }
      }
      _cancelSentQueries = cancellingQueries;
    }

    protected void postAggregation(Map<String, AggregatedStats> aggregatedUsagePerActiveQuery) {
    }

    protected void logQueryResourceUsage(Map<String, ? extends QueryResourceTracker> aggregatedUsagePerActiveQuery) {
      LOGGER.warn("Query aggregation results {} for the previous kill.", aggregatedUsagePerActiveQuery);
    }

    public void cancelQuery(String queryId, Thread anchorThread) {
      MseCancelCallback callback = _queryCancelCallbacks.getIfPresent(queryId);
      if (callback != null) {
        callback.cancelQuery(Long.parseLong(queryId));
        _queryCancelCallbacks.invalidate(queryId);
      } else {
        anchorThread.interrupt();
      }
      _cancelSentQueries.add(queryId);
    }

    protected void logTerminatedQuery(QueryResourceTracker queryResourceTracker, long totalHeapMemoryUsage,
        boolean hasCallback) {
      LOGGER.warn("Query {} terminated. Memory Usage: {}. Cpu Usage: {}. Total Heap Usage: {}. Used Callback: {}",
          queryResourceTracker.getQueryId(), queryResourceTracker.getAllocatedBytes(),
          queryResourceTracker.getCpuTimeNs(), totalHeapMemoryUsage, hasCallback);
    }

    protected void logSelfTerminatedQuery(String queryId, Thread queryThread) {
      if (!_cancelSentQueries.contains(queryId)) {
        LOGGER.warn("{} self-terminated. Heap Usage: {}. Query Thread: {}",
            queryId, _watcherTask.getHeapUsageBytes(), queryThread.getName());
        _cancelSentQueries.add(queryId);
      }
    }

    @Override
    public Exception getErrorStatus() {
      return _threadLocalEntry.get()._errorStatus.getAndSet(null);
    }

    /**
     * The triggered level for the actions, only the highest level action will get triggered. Severity is defined by
     * the ordinal Normal(0) does not trigger any action.
     */
    enum TriggeringLevel {
      Normal, HeapMemoryAlarmingVerbose, CPUTimeBasedKilling, HeapMemoryCritical, HeapMemoryPanic
    }

    /**
     * aggregated usage of a query, _thread is the runner
     */
    protected static class AggregatedStats implements QueryResourceTracker {
      final String _queryId;
      final Thread _anchorThread;
      boolean _isAnchorThread;
      AtomicReference<Exception> _exceptionAtomicReference;
      long _allocatedBytes;
      long _cpuNS;

      public AggregatedStats(long cpuNS, long allocatedBytes, Thread anchorThread, boolean isAnchorThread,
          AtomicReference<Exception> exceptionAtomicReference, String queryId) {
        _cpuNS = cpuNS;
        _allocatedBytes = allocatedBytes;
        _anchorThread = anchorThread;
        _isAnchorThread = isAnchorThread;
        _exceptionAtomicReference = exceptionAtomicReference;
        _queryId = queryId;
      }

      @Override
      public String toString() {
        return "AggregatedStats{"
            + "_queryId='" + _queryId + '\''
            + ", _anchorThread=" + _anchorThread
            + ", _isAnchorThread=" + _isAnchorThread
            + ", _exceptionAtomicReference=" + _exceptionAtomicReference
            + ", _allocatedBytes=" + _allocatedBytes
            + ", _cpuNS=" + _cpuNS
            + '}';
      }

      @Override
      public String getQueryId() {
        return _queryId;
      }

      @Override
      public long getAllocatedBytes() {
        return _allocatedBytes;
      }

      @Override
      public long getCpuTimeNs() {
        return _cpuNS;
      }

      @JsonIgnore
      public Thread getAnchorThread() {
        return _anchorThread;
      }

      public AggregatedStats merge(long cpuNS, long memoryBytes, boolean isAnchorThread,
          AtomicReference<Exception> exceptionAtomicReference) {
        _cpuNS += cpuNS;
        _allocatedBytes += memoryBytes;

        // the merging results is from an anchor thread
        if (isAnchorThread) {
          _isAnchorThread = true;
          _exceptionAtomicReference = exceptionAtomicReference;
        }

        // everything else is already set during creation
        return this;
      }
    }

    /**
     * A watcher task to perform usage sampling, aggregation, and query preemption
     */
    public class WatcherTask implements Runnable, PinotClusterConfigChangeListener {

      protected AtomicReference<QueryMonitorConfig> _queryMonitorConfig = new AtomicReference<>();

      protected long _usedBytes;
      protected int _sleepTime;
      protected Map<String, AggregatedStats> _aggregatedUsagePerActiveQuery;
      protected TriggeringLevel _triggeringLevel;

      // metrics class
      private final AbstractMetrics _metrics;
      private final AbstractMetrics.Meter _queryKilledMeter;
      private final AbstractMetrics.Meter _heapMemoryCriticalExceededMeter;
      private final AbstractMetrics.Meter _heapMemoryPanicExceededMeter;
      private final AbstractMetrics.Gauge _memoryUsageGauge;

      WatcherTask() {
        _queryMonitorConfig.set(new QueryMonitorConfig(_config, ResourceUsageUtils.getMaxHeapSize()));
        logQueryMonitorConfig();

        switch (_instanceType) {
          case SERVER:
            _metrics = ServerMetrics.get();
            _queryKilledMeter = ServerMeter.QUERIES_KILLED;
            _memoryUsageGauge = ServerGauge.JVM_HEAP_USED_BYTES;
            _heapMemoryCriticalExceededMeter = ServerMeter.HEAP_CRITICAL_LEVEL_EXCEEDED;
            _heapMemoryPanicExceededMeter = ServerMeter.HEAP_PANIC_LEVEL_EXCEEDED;
            break;
          case BROKER:
            _metrics = BrokerMetrics.get();
            _queryKilledMeter = BrokerMeter.QUERIES_KILLED;
            _memoryUsageGauge = BrokerGauge.JVM_HEAP_USED_BYTES;
            _heapMemoryCriticalExceededMeter = BrokerMeter.HEAP_CRITICAL_LEVEL_EXCEEDED;
            _heapMemoryPanicExceededMeter = BrokerMeter.HEAP_PANIC_LEVEL_EXCEEDED;
            break;
          default:
            LOGGER.error("instanceType: {} not supported, using server metrics", _instanceType);
            _metrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
            _queryKilledMeter = ServerMeter.QUERIES_KILLED;
            _memoryUsageGauge = ServerGauge.JVM_HEAP_USED_BYTES;
            _heapMemoryCriticalExceededMeter = ServerMeter.HEAP_CRITICAL_LEVEL_EXCEEDED;
            _heapMemoryPanicExceededMeter = ServerMeter.HEAP_PANIC_LEVEL_EXCEEDED;
            break;
        }
      }

      public QueryMonitorConfig getQueryMonitorConfig() {
        return _queryMonitorConfig.get();
      }

      public long getHeapUsageBytes() {
        return _usedBytes;
      }

      @Override
      public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
        // Filter configs that have CommonConstants.PREFIX_SCHEDULER_PREFIX
        Set<String> filteredChangedConfigs =
            changedConfigs.stream().filter(config -> config.startsWith(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX))
                .map(config -> config.replace(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".", ""))
                .collect(Collectors.toSet());

        if (filteredChangedConfigs.isEmpty()) {
          LOGGER.debug("No relevant configs changed, skipping update for QueryMonitorConfig.");
          return;
        }

        Map<String, String> filteredClusterConfigs = clusterConfigs.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX)).collect(
                Collectors.toMap(
                    entry -> entry.getKey().replace(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".", ""),
                    Map.Entry::getValue));

        QueryMonitorConfig oldConfig = _queryMonitorConfig.get();
        QueryMonitorConfig newConfig =
            new QueryMonitorConfig(oldConfig, filteredChangedConfigs, filteredClusterConfigs);
        _queryMonitorConfig.set(newConfig);
        logQueryMonitorConfig();
      }

      private void logQueryMonitorConfig() {
        QueryMonitorConfig queryMonitorConfig = _queryMonitorConfig.get();
        // Log info for the accountant configs
        LOGGER.info("Updated Configuration for Query Monitor");
        LOGGER.info("Xmx is {}", queryMonitorConfig.getMaxHeapSize());
        LOGGER.info("_instanceType is {}", _instanceType);
        LOGGER.info("_alarmingLevel of on heap memory is {}", queryMonitorConfig.getAlarmingLevel());
        LOGGER.info("_criticalLevel of on heap memory is {}", queryMonitorConfig.getCriticalLevel());
        LOGGER.info("_panicLevel of on heap memory is {}", queryMonitorConfig.getPanicLevel());
        LOGGER.info("_normalSleepTime is {}", queryMonitorConfig.getNormalSleepTime());
        LOGGER.info("_alarmingSleepTime is {}", queryMonitorConfig.getAlarmingSleepTime());
        LOGGER.info("_oomKillQueryEnabled: {}", queryMonitorConfig.isOomKillQueryEnabled());
        LOGGER.info("_minMemoryFootprintForKill: {}", queryMonitorConfig.getMinMemoryFootprintForKill());
        LOGGER.info("_isCPUTimeBasedKillingEnabled: {}, _cpuTimeBasedKillingThresholdNS: {}",
            queryMonitorConfig.isCpuTimeBasedKillingEnabled(), queryMonitorConfig.getCpuTimeBasedKillingThresholdNS());
      }

      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            runOnce();
          } finally {
            // Sleep for sometime
            reschedule();
          }
        }
      }

      public void runOnce() {
        QueryMonitorConfig config = _queryMonitorConfig.get();

        LOGGER.debug("Running timed task for PerQueryCPUMemAccountant.");
        _triggeringLevel = TriggeringLevel.Normal;
        _sleepTime = config.getNormalSleepTime();
        _aggregatedUsagePerActiveQuery = null;
        try {
          // Get the metrics used for triggering the kill
          collectTriggerMetrics();
          // Prioritize the panic check, kill ALL QUERIES immediately if triggered
          if (outOfMemoryPanicTrigger()) {
            return;
          }
          // Check for other triggers
          evalTriggers();
          // Refresh thread usage and aggregate to per query usage if triggered
          reapFinishedTasks();
          if (_triggeringLevel.ordinal() > TriggeringLevel.Normal.ordinal()) {
            _aggregatedUsagePerActiveQuery = getQueryResourcesImpl();
          }
          // post aggregation function
          postAggregation(_aggregatedUsagePerActiveQuery);
          // Act on one triggered actions
          triggeredActions();
        } catch (Exception e) {
          LOGGER.error("Caught exception while executing stats aggregation and query kill", e);
        } finally {
          LOGGER.debug(_aggregatedUsagePerActiveQuery == null ? "_aggregatedUsagePerActiveQuery : null"
              : _aggregatedUsagePerActiveQuery.toString());
          LOGGER.debug("_threadEntriesMap size: {}", _threadEntriesMap.size());

          // Publish server heap usage metrics
          if (config.isPublishHeapUsageMetric()) {
            _metrics.setValueOfGlobalGauge(_memoryUsageGauge, _usedBytes);
          }
          // Clean inactive query stats
          cleanInactive();
        }
      }

      private void collectTriggerMetrics() {
        _usedBytes = ResourceUsageUtils.getUsedHeapSize();
        LOGGER.debug("Heap used bytes {}", _usedBytes);
      }

      /**
       * determine if panic mode need to be triggered, kill all queries if yes
       * @return if panic mode is triggered
       */
      private boolean outOfMemoryPanicTrigger() {
        long panicLevel = _queryMonitorConfig.get().getPanicLevel();
        // at this point we assume we have tried to kill some queries and the gc kicked in
        // we have no choice but to kill all queries
        if (_usedBytes >= panicLevel) {
          killAllQueries();
          _triggeringLevel = TriggeringLevel.HeapMemoryPanic;
          _metrics.addMeteredGlobalValue(_heapMemoryPanicExceededMeter, 1);
          LOGGER.error("Heap used bytes {}, greater than _panicLevel {}, Killed all queries and triggered gc!",
              _usedBytes, panicLevel);
          // read finished tasks here as will throw exception and
          reapFinishedTasks();
          return true;
        }
        return false;
      }

      /**
       * Evaluate triggering levels of query preemption
       * Triggers should be mutually exclusive and evaluated following level high -> low
       */
      protected void evalTriggers() {
        QueryMonitorConfig config = _queryMonitorConfig.get();

        if (config.isCpuTimeBasedKillingEnabled()) {
          _triggeringLevel = TriggeringLevel.CPUTimeBasedKilling;
        }

        if (_usedBytes > config.getCriticalLevel()) {
          _triggeringLevel = TriggeringLevel.HeapMemoryCritical;
          _metrics.addMeteredGlobalValue(_heapMemoryCriticalExceededMeter, 1);
        } else if (_usedBytes > config.getAlarmingLevel()) {
          _sleepTime = config.getAlarmingSleepTime();
          // For debugging
          _triggeringLevel = (IS_DEBUG_MODE_ENABLED && _triggeringLevel == TriggeringLevel.Normal)
              ? TriggeringLevel.HeapMemoryAlarmingVerbose : _triggeringLevel;
        }
      }

      /**
       * Perform actions at specific triggering levels
       */
      protected void triggeredActions() {
        switch (_triggeringLevel) {
          case HeapMemoryCritical:
            LOGGER.warn("Heap used bytes {} exceeds critical level {}", _usedBytes,
                _queryMonitorConfig.get().getCriticalLevel());
            killMostExpensiveQuery();
            break;
          case CPUTimeBasedKilling:
            killCPUTimeExceedQueries();
            break;
          case HeapMemoryAlarmingVerbose:
            LOGGER.warn("Heap used bytes {} exceeds alarming level", _usedBytes);
            LOGGER.warn("Query usage aggregation results {}", _aggregatedUsagePerActiveQuery.toString());
            break;
          default:
            break;
        }
      }

      void reschedule() {
        try {
          Thread.sleep(_sleepTime);
        } catch (InterruptedException ignored) {
        }
      }

      void killAllQueries() {
        QueryMonitorConfig config = _queryMonitorConfig.get();

        if (config.isOomKillQueryEnabled()) {
          int killedCount = 0;
          for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : _threadEntriesMap.entrySet()) {
            CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = entry.getValue();
            CPUMemThreadLevelAccountingObjects.TaskEntry taskEntry = threadEntry.getCurrentThreadTaskStatus();
            if (taskEntry != null && !_cancelSentQueries.contains(taskEntry.getQueryId())) {
              cancelQuery(taskEntry.getQueryId(), taskEntry.getAnchorThread());
              killedCount += 1;
            }
          }
          if (config.isQueryKilledMetricEnabled()) {
            _metrics.addMeteredGlobalValue(_queryKilledMeter, killedCount);
          }
        }
      }

      /**
       * Kill the query with the highest cost (memory footprint/cpu time/...)
       */
      private void killMostExpensiveQuery() {
        if (!_isThreadMemorySamplingEnabled) {
          LOGGER.warn("Unable to terminate queries as  memory tracking is not enabled");
          return;
        }
        QueryMonitorConfig config = _queryMonitorConfig.get();
        // Critical heap memory usage while no queries running
        if (_aggregatedUsagePerActiveQuery != null && !_aggregatedUsagePerActiveQuery.isEmpty()) {
          AggregatedStats maxUsageTuple;
          maxUsageTuple = _aggregatedUsagePerActiveQuery.values().stream()
              .filter(stats -> !_cancelSentQueries.contains(stats.getQueryId()))
              .max(Comparator.comparing(AggregatedStats::getAllocatedBytes)).orElse(null);
          if (maxUsageTuple != null) {
            boolean shouldKill =
                config.isOomKillQueryEnabled() && maxUsageTuple._allocatedBytes > config.getMinMemoryFootprintForKill();
            if (shouldKill) {
              maxUsageTuple._exceptionAtomicReference.set(new RuntimeException(
                  String.format(" Query %s got killed because using %d bytes of memory on %s: %s, exceeding the quota",
                      maxUsageTuple._queryId, maxUsageTuple.getAllocatedBytes(), _instanceType, _instanceId)));
              boolean hasCallBack = _queryCancelCallbacks.getIfPresent(maxUsageTuple.getQueryId()) != null;
              terminateQuery(maxUsageTuple);
              logTerminatedQuery(maxUsageTuple, _usedBytes, hasCallBack);
            } else if (!config.isOomKillQueryEnabled()) {
              LOGGER.warn("Query {} got picked because using {} bytes of memory, actual kill committed false "
                  + "because oomKillQueryEnabled is false", maxUsageTuple._queryId, maxUsageTuple._allocatedBytes);
            } else {
              LOGGER.warn("But all queries are below quota, no query killed");
            }
          }
          logQueryResourceUsage(_aggregatedUsagePerActiveQuery);
        } else {
          LOGGER.debug("No active queries to kill");
        }
      }

      private void killCPUTimeExceedQueries() {
        QueryMonitorConfig config = _queryMonitorConfig.get();

        for (Map.Entry<String, AggregatedStats> entry : _aggregatedUsagePerActiveQuery.entrySet()) {
          AggregatedStats value = entry.getValue();
          if (value._cpuNS > config.getCpuTimeBasedKillingThresholdNS()) {
            LOGGER.error("Current task status recorded is {}. Query {} got picked because using {} ns of cpu time,"
                    + " greater than threshold {}", _threadEntriesMap, value._queryId, value.getCpuTimeNs(),
                config.getCpuTimeBasedKillingThresholdNS());
            value._exceptionAtomicReference.set(new RuntimeException(
                String.format("Query %s got killed on %s: %s because using %d "
                        + "CPU time exceeding limit of %d ns CPU time", value._queryId, _instanceType, _instanceId,
                    value.getCpuTimeNs(), config.getCpuTimeBasedKillingThresholdNS())));
            boolean hasCallBack = _queryCancelCallbacks.getIfPresent(value.getQueryId()) != null;
            terminateQuery(value);
            logTerminatedQuery(value, _usedBytes, hasCallBack);
          }
        }
        logQueryResourceUsage(_aggregatedUsagePerActiveQuery);
      }

      private void terminateQuery(AggregatedStats queryResourceTracker) {
        cancelQuery(queryResourceTracker.getQueryId(), queryResourceTracker.getAnchorThread());
        if (_queryMonitorConfig.get().isQueryKilledMetricEnabled()) {
          _metrics.addMeteredGlobalValue(_queryKilledMeter, 1);
        }
      }
    }
  }
}
