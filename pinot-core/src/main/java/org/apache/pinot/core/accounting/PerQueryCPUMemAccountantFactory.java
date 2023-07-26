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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Accounting mechanism for thread task execution status and thread resource usage sampling
 * Design and algorithm see
 * https://docs.google.com/document/d/1Z9DYAfKznHQI9Wn8BjTWZYTcNRVGiPP0B8aEP3w_1jQ
 */
public class PerQueryCPUMemAccountantFactory implements ThreadAccountantFactory {

  @Override
  public ThreadResourceUsageAccountant init(PinotConfiguration config, String instanceId) {
    return new PerQueryCPUMemResourceUsageAccountant(config, instanceId);
  }

  public static class PerQueryCPUMemResourceUsageAccountant extends Tracing.DefaultThreadResourceUsageAccountant {

    /**
     * MemoryMXBean to get total heap used memory
     */
    static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();
    private static final Logger LOGGER = LoggerFactory.getLogger(PerQueryCPUMemResourceUsageAccountant.class);
    private static final boolean IS_DEBUG_MODE_ENABLED = LOGGER.isDebugEnabled();
    /**
     * Executor service for the thread accounting task, slightly higher priority than normal priority
     */
    private static final String ACCOUNTANT_TASK_NAME = "CPUMemThreadAccountant";
    private static final int ACCOUNTANT_PRIORITY = 4;
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(1, r -> {
      Thread thread = new Thread(r);
      thread.setPriority(ACCOUNTANT_PRIORITY);
      thread.setDaemon(true);
      thread.setName(ACCOUNTANT_TASK_NAME);
      return thread;
    });

    private final PinotConfiguration _config;

    // the map to track stats entry for each thread, the entry will automatically be added when one calls
    // setThreadResourceUsageProvider on the thread, including but not limited to
    // server worker thread, runner thread, broker jetty thread, or broker netty thread
    private final ConcurrentHashMap<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> _threadEntriesMap
        = new ConcurrentHashMap<>();

    // For one time concurrent update of stats. This is to provide stats collection for parts that are not
    // performance sensitive and query_id is not known beforehand (e.g. broker inbound netty thread)
    private final ConcurrentHashMap<String, Long> _concurrentTaskCPUStatsAggregator = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> _concurrentTaskMemStatsAggregator = new ConcurrentHashMap<>();

    // for stats aggregation of finished (worker) threads when the runner is still running
    private final HashMap<String, Long> _finishedTaskCPUStatsAggregator = new HashMap<>();
    private final HashMap<String, Long> _finishedTaskMemStatsAggregator = new HashMap<>();

    private final ThreadLocal<CPUMemThreadLevelAccountingObjects.ThreadEntry> _threadLocalEntry
        = ThreadLocal.withInitial(() -> {
          CPUMemThreadLevelAccountingObjects.ThreadEntry ret =
              new CPUMemThreadLevelAccountingObjects.ThreadEntry();
          _threadEntriesMap.put(Thread.currentThread(), ret);
          LOGGER.info("Adding thread to _threadLocalEntry: {}", Thread.currentThread().getName());
          return ret;
        }
    );

    // ThreadResourceUsageProvider(ThreadMXBean wrapper) per runner/worker thread
    private final ThreadLocal<ThreadResourceUsageProvider> _threadResourceUsageProvider;

    // track thread cpu time
    private final boolean _isThreadCPUSamplingEnabled;

    // track memory usage
    private final boolean _isThreadMemorySamplingEnabled;

    private final Set<String> _inactiveQuery;

    // the periodical task that aggregates and preempts queries
    private final WatcherTask _watcherTask;

    // instance id of the current instance, for logging purpose
    private final String _instanceId;

    public PerQueryCPUMemResourceUsageAccountant(PinotConfiguration config, String instanceId) {

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

      _isThreadCPUSamplingEnabled = cpuSamplingConfig && threadCpuTimeMeasurementEnabled;
      _isThreadMemorySamplingEnabled = memorySamplingConfig && threadMemoryMeasurementEnabled;
      LOGGER.info("_isThreadCPUSamplingEnabled: {}, _isThreadMemorySamplingEnabled: {}", _isThreadCPUSamplingEnabled,
          _isThreadMemorySamplingEnabled);

      // ThreadMXBean wrapper
      _threadResourceUsageProvider = new ThreadLocal<>();

      // task/query tracking
      _inactiveQuery = new HashSet<>();

      _watcherTask = new WatcherTask();
    }

    @Override
    public void sampleUsage() {
      sampleThreadBytesAllocated();
      sampleThreadCPUTime();
    }

    /**
     * for testing only
     */
    public int getEntryCount() {
      return _threadEntriesMap.size();
    }

    @Override
    public void updateQueryUsageConcurrently(String queryId) {
      if (_isThreadCPUSamplingEnabled) {
        long cpuUsageNS = getThreadResourceUsageProvider().getThreadTimeNs();
        _concurrentTaskCPUStatsAggregator.compute(queryId,
            (key, value) -> (value == null) ? cpuUsageNS : (value + cpuUsageNS));
      }
      if (_isThreadMemorySamplingEnabled) {
        long memoryAllocatedBytes = getThreadResourceUsageProvider().getThreadAllocatedBytes();
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
        _threadLocalEntry.get()._currentThreadCPUTimeSampleMS = getThreadResourceUsageProvider().getThreadTimeNs();
      }
    }

    /**
     * The thread would need to do {@code setThreadResourceUsageProvider} first upon it is scheduled.
     * This is to be called from a worker or a runner thread to update its corresponding memory usage entry
     */
    @SuppressWarnings("ConstantConditions")
    public void sampleThreadBytesAllocated() {
      if (_isThreadMemorySamplingEnabled) {
        _threadLocalEntry.get()._currentThreadMemoryAllocationSampleBytes
            = getThreadResourceUsageProvider().getThreadAllocatedBytes();
      }
    }

    private ThreadResourceUsageProvider getThreadResourceUsageProvider() {
      return _threadResourceUsageProvider.get();
    }

    @Override
    public void setThreadResourceUsageProvider(ThreadResourceUsageProvider threadResourceUsageProvider) {
      _threadResourceUsageProvider.set(threadResourceUsageProvider);
    }

    @Override
    public void createExecutionContextInner(@Nullable String queryId, int taskId, @Nullable
        ThreadExecutionContext parentContext) {
      _threadLocalEntry.get()._errorStatus.set(null);
      if (parentContext == null) {
        // is anchor thread
        assert queryId != null;
        _threadLocalEntry.get().setThreadTaskStatus(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID,
            Thread.currentThread());
      } else {
        // not anchor thread
        _threadLocalEntry.get().setThreadTaskStatus(parentContext.getQueryId(), taskId,
            parentContext.getAnchorThread());
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
      // clear threadResourceUsageProvider
      _threadResourceUsageProvider.set(null);
      // clear _anchorThread
      super.clear();
    }

    @Override
    public void startWatcherTask() {
      EXECUTOR_SERVICE.submit(_watcherTask);
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

    /**
     * aggregated the stats if the query killing process is triggered
     * @param isTriggered if the query killing process is triggered
     * @return aggregated stats of active queries if triggered
     */
    public Map<String, AggregatedStats> aggregate(boolean isTriggered) {
      HashMap<String, AggregatedStats> ret = null;
      if (isTriggered) {
        ret = new HashMap<>();
      }

      // for each {pqr, pqw}
      for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : _threadEntriesMap.entrySet()) {
        // sample current usage
        CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = entry.getValue();
        long currentCPUSample = _isThreadCPUSamplingEnabled
            ? threadEntry._currentThreadCPUTimeSampleMS : 0;
        long currentMemSample = _isThreadMemorySamplingEnabled
            ? threadEntry._currentThreadMemoryAllocationSampleBytes : 0;
        // sample current running task status
        CPUMemThreadLevelAccountingObjects.TaskEntry currentTaskStatus = threadEntry.getCurrentThreadTaskStatus();
        Thread thread = entry.getKey();
        LOGGER.trace("tid: {}, task: {}", thread.getId(), currentTaskStatus);

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
          // if triggered, accumulate active query task stats
          if (isTriggered) {
            Thread anchorThread = currentTaskStatus.getAnchorThread();
            boolean isAnchorThread = currentTaskStatus.isAnchorThread();
            ret.compute(queryId, (k, v) -> v == null
                ? new AggregatedStats(currentCPUSample, currentMemSample, anchorThread,
                isAnchorThread, threadEntry._errorStatus, queryId)
                : v.merge(currentCPUSample, currentMemSample, isAnchorThread, threadEntry._errorStatus));
          }
        }

        if (!thread.isAlive()) {
          _threadEntriesMap.remove(thread);
          LOGGER.info("Removing thread from _threadLocalEntry: {}", thread.getName());
        }
      }

      // if triggered, accumulate stats of finished tasks of each active query
      if (isTriggered) {
        for (Map.Entry<String, AggregatedStats> queryIdResult : ret.entrySet()) {
          String activeQueryId = queryIdResult.getKey();
          long accumulatedCPUValue = _isThreadCPUSamplingEnabled
              ? _finishedTaskCPUStatsAggregator.getOrDefault(activeQueryId, 0L) : 0;
          long concurrentCPUValue = _isThreadCPUSamplingEnabled
              ? _concurrentTaskCPUStatsAggregator.getOrDefault(activeQueryId, 0L) : 0;
          long accumulatedMemValue = _isThreadMemorySamplingEnabled
              ? _finishedTaskMemStatsAggregator.getOrDefault(activeQueryId, 0L) : 0;
          long concurrentMemValue = _isThreadMemorySamplingEnabled
              ? _concurrentTaskMemStatsAggregator.getOrDefault(activeQueryId, 0L) : 0;
          queryIdResult.getValue().merge(accumulatedCPUValue + concurrentCPUValue,
              accumulatedMemValue + concurrentMemValue, false, null);
        }
      }
      return ret;
    }

    public void postAggregation(Map<String, AggregatedStats> aggregatedUsagePerActiveQuery) {
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
    protected static class AggregatedStats {
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

      public long getCpuNS() {
        return _cpuNS;
      }

      public long getAllocatedBytes() {
        return _allocatedBytes;
      }

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
    class WatcherTask implements Runnable {

      // max heap usage, Xmx
      private final long _maxHeapSize = MEMORY_MX_BEAN.getHeapMemoryUsage().getMax();

      // don't kill a query if its memory footprint is below some ratio of _maxHeapSize
      private final long _minMemoryFootprintForKill = (long) (_maxHeapSize
          * _config.getProperty(CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO,
          CommonConstants.Accounting.DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO));

      // kill all queries if heap usage exceeds this
      private final long _panicLevel = (long) (_maxHeapSize
          * _config.getProperty(CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO,
          CommonConstants.Accounting.DFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO));

      // kill the most expensive query if heap usage exceeds this
      private final long _criticalLevel = (long) (_maxHeapSize
          * _config.getProperty(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO,
          CommonConstants.Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO));

      // if after gc the heap usage is still above this, kill the most expensive query
      // use this to prevent heap size oscillation and repeatedly triggering gc
      private final long _criticalLevelAfterGC = _criticalLevel - (long) (_maxHeapSize
          * _config.getProperty(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC,
          CommonConstants.Accounting.DEFAULT_CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC));

      // trigger gc if consecutively kill more than some number of queries
      // set this to 0 to always trigger gc before killing a query to give gc a second chance
      // as would minimize the chance of false positive killing in some usecases
      // should consider use -XX:+ExplicitGCInvokesConcurrent to avoid STW for some gc algorithms
      private final int _gcBackoffCount =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_GC_BACKOFF_COUNT,
              CommonConstants.Accounting.DEFAULT_GC_BACKOFF_COUNT);

      // start to sample more frequently if heap usage exceeds this
      private final long _alarmingLevel =
          (long) (_maxHeapSize
              * _config.getProperty(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO,
              CommonConstants.Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO));

      // normal sleep time
      private final int _normalSleepTime =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_MS,
              CommonConstants.Accounting.DEFAULT_SLEEP_TIME_MS);

      // wait for gc to complete, according to system.gc() javadoc, when control returns from the method call,
      // the Java Virtual Machine has made a best effort to reclaim space from all discarded objects.
      // Therefore, we default this to 0.
      // Tested with Shenandoah GC and G1GC, with -XX:+ExplicitGCInvokesConcurrent
      private final int _gcWaitTime =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_GC_WAIT_TIME_MS,
              CommonConstants.Accounting.DEFAULT_CONFIG_OF_GC_WAIT_TIME_MS);

      // alarming sleep time denominator, should be > 1 to sample more frequent at alarming level
      private final int _alarmingSleepTimeDenominator =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR,
              CommonConstants.Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR);

      // alarming sleep time
      private final int _alarmingSleepTime = _normalSleepTime / _alarmingSleepTimeDenominator;

      // the framework would not commit to kill any query if this is disabled
      private final boolean _oomKillQueryEnabled =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY,
              CommonConstants.Accounting.DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY);

      // if we want to publish the heap usage
      private final boolean _publishHeapUsageMetric =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_PUBLISHING_JVM_USAGE,
              CommonConstants.Accounting.DEFAULT_PUBLISHING_JVM_USAGE);

      // if we want kill query based on CPU time
      private final boolean _isCPUTimeBasedKillingEnabled =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED,
              CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_ENABLED) && _isThreadCPUSamplingEnabled;

      // CPU time based killing threshold
      private final long _cpuTimeBasedKillingThresholdNS =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS,
              CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS) * 1000_000L;

      //
      private final boolean _isQueryKilledMetricEnabled =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_QUERY_KILLED_METRIC_ENABLED,
              CommonConstants.Accounting.DEFAULT_QUERY_KILLED_METRIC_ENABLED);

      private final InstanceType _instanceType =
          InstanceType.valueOf(_config.getProperty(CommonConstants.Accounting.CONFIG_OF_INSTANCE_TYPE,
          CommonConstants.Accounting.DEFAULT_CONFIG_OF_INSTANCE_TYPE.toString()));

      private long _usedBytes;
      private int _sleepTime;
      private int _numQueriesKilledConsecutively = 0;
      protected Map<String, AggregatedStats> _aggregatedUsagePerActiveQuery;
      private TriggeringLevel _triggeringLevel;

      // metrics class
      private final AbstractMetrics _metrics;
      private final AbstractMetrics.Meter _queryKilledMeter;
      private final AbstractMetrics.Meter _heapMemoryCriticalExceededMeter;
      private final AbstractMetrics.Meter _heapMemoryPanicExceededMeter;
      private final AbstractMetrics.Gauge _memoryUsageGauge;

      WatcherTask() {
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

      @Override
      public void run() {
        // Log info for the accountant configs
        LOGGER.info("Starting accountant task for PerQueryCPUMemAccountant.");
        LOGGER.info("Xmx is {}", _maxHeapSize);
        LOGGER.info("_instanceType is {}", _instanceType);
        LOGGER.info("_alarmingLevel of on heap memory is {}", _alarmingLevel);
        LOGGER.info("_criticalLevel of on heap memory is {}", _criticalLevel);
        LOGGER.info("_criticalLevelAfterGC of on heap memory is {}", _criticalLevelAfterGC);
        LOGGER.info("_panicLevel of on heap memory is {}", _panicLevel);
        LOGGER.info("_gcBackoffCount is {}", _gcBackoffCount);
        LOGGER.info("_gcWaitTime is {}", _gcWaitTime);
        LOGGER.info("_normalSleepTime is {}", _normalSleepTime);
        LOGGER.info("_alarmingSleepTime is {}", _alarmingSleepTime);
        LOGGER.info("_oomKillQueryEnabled: {}", _oomKillQueryEnabled);
        LOGGER.info("_minMemoryFootprintForKill: {}", _minMemoryFootprintForKill);
        LOGGER.info("_isCPUTimeBasedKillingEnabled: {}, _cpuTimeBasedKillingThresholdNS: {}",
            _isCPUTimeBasedKillingEnabled, _cpuTimeBasedKillingThresholdNS);

        while (true) {
          LOGGER.debug("Running timed task for PerQueryCPUMemAccountant.");
          _triggeringLevel = TriggeringLevel.Normal;
          _sleepTime = _normalSleepTime;
          _aggregatedUsagePerActiveQuery = null;
          try {
            // Get the metrics used for triggering the kill
            collectTriggerMetrics();
            // Prioritize the panic check, kill ALL QUERIES immediately if triggered
            if (outOfMemoryPanicTrigger()) {
              continue;
            }
            // Check for other triggers
            evalTriggers();
            // Refresh thread usage and aggregate to per query usage if triggered
            _aggregatedUsagePerActiveQuery = aggregate(_triggeringLevel.ordinal() > TriggeringLevel.Normal.ordinal());
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
            if (_publishHeapUsageMetric) {
              _metrics.setValueOfGlobalGauge(_memoryUsageGauge, _usedBytes);
            }
            // Clean inactive query stats
            cleanInactive();
            // Sleep for sometime
            reschedule();
          }
        }
      }

      private void collectTriggerMetrics() {
        _usedBytes = MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
        LOGGER.debug("Heap used bytes {}", _usedBytes);
      }

      /**
       * determine if panic mode need to be triggered, kill all queries if yes
       * @return if panic mode is triggered
       */
      private boolean outOfMemoryPanicTrigger() {
        // at this point we assume we have tried to kill some queries and the gc kicked in
        // we have no choice but to kill all queries
        if (_usedBytes >= _panicLevel) {
          killAllQueries();
          _triggeringLevel = TriggeringLevel.HeapMemoryPanic;
          _metrics.addMeteredGlobalValue(_heapMemoryPanicExceededMeter, 1);
          LOGGER.error("Heap used bytes {}, greater than _panicLevel {}, Killed all queries and triggered gc!",
              _usedBytes, _panicLevel);
          // call aggregate here as will throw exception and
          aggregate(false);
          return true;
        }
        return false;
      }

      /**
       * Evaluate triggering levels of query preemption
       * Triggers should be mutually exclusive and evaluated following level high -> low
       */
      private void evalTriggers() {
        if (_isCPUTimeBasedKillingEnabled) {
          _triggeringLevel = TriggeringLevel.CPUTimeBasedKilling;
        }

        if (_usedBytes > _criticalLevel) {
          _triggeringLevel = TriggeringLevel.HeapMemoryCritical;
          _metrics.addMeteredGlobalValue(_heapMemoryCriticalExceededMeter, 1);
        } else if (_usedBytes > _alarmingLevel) {
          _sleepTime = _alarmingSleepTime;
          // For debugging
          _triggeringLevel = (IS_DEBUG_MODE_ENABLED && _triggeringLevel == TriggeringLevel.Normal)
              ? TriggeringLevel.HeapMemoryAlarmingVerbose : _triggeringLevel;
        }
      }

      /**
       * Perform actions at specific triggering levels
       */
      private void triggeredActions() {
        switch (_triggeringLevel) {
          case HeapMemoryCritical:
            LOGGER.warn("Heap used bytes {} exceeds critical level {}", _usedBytes, _criticalLevel);
            killMostExpensiveQuery();
            break;
          case CPUTimeBasedKilling:
            killCPUTimeExceedQueries();
            break;
          case HeapMemoryAlarmingVerbose:
            LOGGER.warn("Heap used bytes {} exceeds alarming level", _usedBytes);
            LOGGER.warn("Query usage aggregation results {}", _aggregatedUsagePerActiveQuery.toString());
            _numQueriesKilledConsecutively = 0;
            break;
          default:
            _numQueriesKilledConsecutively = 0;
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
        if (_oomKillQueryEnabled) {
          int killedCount = 0;
          for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : _threadEntriesMap.entrySet()) {
            CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = entry.getValue();
            CPUMemThreadLevelAccountingObjects.TaskEntry taskEntry = threadEntry.getCurrentThreadTaskStatus();
            if (taskEntry != null && taskEntry.isAnchorThread()) {
              threadEntry._errorStatus
                  .set(new RuntimeException(String.format("Query killed due to %s out of memory!", _instanceType)));
              taskEntry.getAnchorThread().interrupt();
              killedCount += 1;
            }
          }
          if (_isQueryKilledMetricEnabled) {
            _metrics.addMeteredGlobalValue(_queryKilledMeter, killedCount);
          }
          try {
            Thread.sleep(_normalSleepTime);
          } catch (InterruptedException ignored) {
          }
          // In this extreme case we directly trigger system.gc
          System.gc();
          _numQueriesKilledConsecutively = 0;
        }
      }

      /**
       * Kill the query with the highest cost (memory footprint/cpu time/...)
       * Will trigger gc when killing a consecutive number of queries
       * use XX:+ExplicitGCInvokesConcurrent to avoid a full gc when system.gc is triggered
       */
      private void killMostExpensiveQuery() {
        if (!_aggregatedUsagePerActiveQuery.isEmpty() && _numQueriesKilledConsecutively >= _gcBackoffCount) {
          _numQueriesKilledConsecutively = 0;
          System.gc();
          try {
            Thread.sleep(_gcWaitTime);
          } catch (InterruptedException ignored) {
          }
          _usedBytes = MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
          if (_usedBytes < _criticalLevelAfterGC) {
            return;
          }
          LOGGER.error("After GC, heap used bytes {} still exceeds _criticalLevelAfterGC level {}",
              _usedBytes, _criticalLevelAfterGC);
        }
        if (!(_isThreadMemorySamplingEnabled || _isThreadCPUSamplingEnabled)) {
          LOGGER.warn("But unable to kill query because neither memory nor cpu tracking is enabled");
          return;
        }
        // Critical heap memory usage while no queries running
        if (_aggregatedUsagePerActiveQuery.isEmpty()) {
          LOGGER.debug("No active queries to kill");
          return;
        }
        AggregatedStats maxUsageTuple;
        if (_isThreadMemorySamplingEnabled) {
          maxUsageTuple = Collections.max(_aggregatedUsagePerActiveQuery.values(),
              Comparator.comparing(AggregatedStats::getAllocatedBytes));
          boolean shouldKill = _oomKillQueryEnabled && maxUsageTuple._allocatedBytes > _minMemoryFootprintForKill;
          if (shouldKill) {
            maxUsageTuple._exceptionAtomicReference
                .set(new RuntimeException(String.format(
                    " Query %s got killed because using %d bytes of memory on %s: %s, exceeding the quota",
                    maxUsageTuple._queryId, maxUsageTuple.getAllocatedBytes(), _instanceType, _instanceId)));
            interruptRunnerThread(maxUsageTuple.getAnchorThread());
            LOGGER.error("Query {} got picked because using {} bytes of memory, actual kill committed true}",
                maxUsageTuple._queryId, maxUsageTuple._allocatedBytes);
            LOGGER.error("Current task status recorded is {}", _threadEntriesMap);
          } else if (!_oomKillQueryEnabled) {
            LOGGER.warn("Query {} got picked because using {} bytes of memory, actual kill committed false "
                    + "because oomKillQueryEnabled is false",
                maxUsageTuple._queryId, maxUsageTuple._allocatedBytes);
          } else {
            LOGGER.warn("But all queries are below quota, no query killed");
          }
        } else {
          maxUsageTuple = Collections.max(_aggregatedUsagePerActiveQuery.values(),
              Comparator.comparing(AggregatedStats::getCpuNS));
          if (_oomKillQueryEnabled) {
            maxUsageTuple._exceptionAtomicReference
                .set(new RuntimeException(String.format(
                    " Query %s got killed because memory pressure, using %d ns of CPU time on %s: %s",
                    maxUsageTuple._queryId, maxUsageTuple.getAllocatedBytes(), _instanceType, _instanceId)));
            interruptRunnerThread(maxUsageTuple.getAnchorThread());
            LOGGER.error("Query {} got picked because using {} ns of cpu time, actual kill committed true",
                maxUsageTuple._allocatedBytes, maxUsageTuple._queryId);
            LOGGER.error("Current task status recorded is {}", _threadEntriesMap);
          } else {
            LOGGER.warn("Query {} got picked because using {} bytes of memory, actual kill committed false "
                    + "because oomKillQueryEnabled is false",
                maxUsageTuple._queryId, maxUsageTuple._allocatedBytes);
          }
        }
        LOGGER.warn("Query aggregation results {} for the previous kill.", _aggregatedUsagePerActiveQuery.toString());
      }

      private void killCPUTimeExceedQueries() {
        for (Map.Entry<String, AggregatedStats> entry : _aggregatedUsagePerActiveQuery.entrySet()) {
          AggregatedStats value = entry.getValue();
          if (value._cpuNS > _cpuTimeBasedKillingThresholdNS) {
            LOGGER.error("Query {} got picked because using {} ns of cpu time, greater than threshold {}",
                value._queryId, value.getCpuNS(), _cpuTimeBasedKillingThresholdNS);
            value._exceptionAtomicReference.set(new RuntimeException(
                String.format("Query %s got killed on %s: %s because using %d "
                        + "CPU time exceeding limit of %d ns CPU time",
                    value._queryId, _instanceType, _instanceId, value.getCpuNS(), _cpuTimeBasedKillingThresholdNS)));
            interruptRunnerThread(value.getAnchorThread());
          }
        }
        LOGGER.error("Current task status recorded is {}", _threadEntriesMap);
      }

      private void interruptRunnerThread(Thread thread) {
        thread.interrupt();
        if (_isQueryKilledMetricEnabled) {
          _metrics.addMeteredGlobalValue(_queryKilledMeter, 1);
        }
        _numQueriesKilledConsecutively += 1;
      }
    }
  }
}
