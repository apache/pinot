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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.accounting.utils.RunnerWorkerThreadOffsetProvider;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
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
  public ThreadResourceUsageAccountant init(int numRunnerThreads, int numWorkerThreads, PinotConfiguration config) {
    return new PerQueryCPUMemResourceUsageAccountant(numRunnerThreads + numWorkerThreads, config);
  }

  public static class PerQueryCPUMemResourceUsageAccountant extends Tracing.DefaultThreadResourceUsageAccountant {

    /**
     * MemoryMXBean to get total heap used memory
     */
    static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();
    private static final Logger LOGGER = LoggerFactory.getLogger(PerQueryCPUMemResourceUsageAccountant.class);
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

    // number of total threads
    private final int _numThreads;
    private final PinotConfiguration _config;
    private final RunnerWorkerThreadOffsetProvider _runnerWorkerThreadOffsetProvider;

    // query_id, task_id per runner/worker thread
    private final CPUMemThreadLevelAccountingObjects.TaskEntryHolder[] _taskStatus;

    // ThreadResourceUsageProvider(ThreadMXBean wrapper) per runner/worker thread
    private final ThreadLocal<ThreadResourceUsageProvider> _threadResourceUsageProvider;

    // track thread cpu time
    private final boolean _isThreadCPUSamplingEnabled;
    // cpu time samples per runner/worker thread
    private final CPUMemThreadLevelAccountingObjects.StatsDigest _cpuTimeSamplesNS;

    // track memory usage
    private final boolean _isThreadMemorySamplingEnabled;
    // memory usage samples per runner/worker thread
    private final CPUMemThreadLevelAccountingObjects.StatsDigest _memorySamplesBytes;

    // the last seen task_id-query_id
    private final CPUMemThreadLevelAccountingObjects.TaskEntry[] _lastQueryTask;

    private final Set<String> _inactiveQuery;
    // error message store per runner/worker thread,
    // will put preemption reasons in this for the killed thread to pickup
    private final List<AtomicReference<Exception>> _errorStatus;

    // the periodical task that aggregates and preempts queries
    private final WatcherTask _watcherTask;

    public PerQueryCPUMemResourceUsageAccountant(int numThreads, PinotConfiguration config) {

      LOGGER.info("Initializing PerQueryCPUMemResourceUsageAccountant");
      _numThreads = numThreads;
      _config = config;
      _runnerWorkerThreadOffsetProvider = new RunnerWorkerThreadOffsetProvider();

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

      _taskStatus = new CPUMemThreadLevelAccountingObjects.TaskEntryHolder[_numThreads];
      _errorStatus = new ArrayList<>(_numThreads);
      for (int i = 0; i < _numThreads; i++) {
        _taskStatus[i] = new CPUMemThreadLevelAccountingObjects.TaskEntryHolder();
        _errorStatus.add(new AtomicReference<>(null));
      }

      if (_isThreadCPUSamplingEnabled) {
        _cpuTimeSamplesNS = new CPUMemThreadLevelAccountingObjects.StatsDigest(_numThreads);
      } else {
        _cpuTimeSamplesNS = null;
      }
      if (_isThreadMemorySamplingEnabled) {
        _memorySamplesBytes = new CPUMemThreadLevelAccountingObjects.StatsDigest(_numThreads);
      } else {
        _memorySamplesBytes = null;
      }

      // ThreadMXBean wrapper
      _threadResourceUsageProvider = new ThreadLocal<>();

      // task/query tracking
      _lastQueryTask = new CPUMemThreadLevelAccountingObjects.TaskEntry[_numThreads];
      _inactiveQuery = new HashSet<>();

      _watcherTask = new WatcherTask();
    }

    @Override
    public void sampleUsage() {
      sampleThreadBytesAllocated();
      sampleThreadCPUTime();
    }

    /**
     * The thread would need to do {@code setThreadResourceUsageProvider} first upon it is scheduled.
     * This is to be called from a worker or a runner thread to update its corresponding cpu usage entry
     */
    @SuppressWarnings("ConstantConditions")
    public void sampleThreadCPUTime() {
      if (_isThreadCPUSamplingEnabled) {
        int tid = _runnerWorkerThreadOffsetProvider.get();
        _cpuTimeSamplesNS._currentStatsSample[tid] = getThreadResourceUsageProvider().getThreadTimeNs();
      }
    }

    /**
     * The thread would need to do {@code setThreadResourceUsageProvider} first upon it is scheduled.
     * This is to be called from a worker or a runner thread to update its corresponding memory usage entry
     */
    @SuppressWarnings("ConstantConditions")
    public void sampleThreadBytesAllocated() {
      if (_isThreadMemorySamplingEnabled) {
        int tid = _runnerWorkerThreadOffsetProvider.get();
        _memorySamplesBytes._currentStatsSample[tid] = getThreadResourceUsageProvider().getThreadAllocatedBytes();
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
      int tid = _runnerWorkerThreadOffsetProvider.get();
      if (parentContext == null) {
        // is anchor thread
        assert queryId != null;
        _taskStatus[tid].setThreadTaskStatus(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID,
            Thread.currentThread());
      } else {
        // not anchor thread
        _taskStatus[tid].setThreadTaskStatus(parentContext.getQueryId(), taskId, parentContext.getAnchorThread());
      }
    }

    @Override
    public ThreadExecutionContext getThreadExecutionContext() {
      int tid = _runnerWorkerThreadOffsetProvider.get();
      return _taskStatus[tid].getThreadTaskStatus();
    }

    /**
     * clears thread accounting info once a runner/worker thread has finished a particular run
     */
    @SuppressWarnings("ConstantConditions")
    @Override
    public void clear() {
      int tid = _runnerWorkerThreadOffsetProvider.get();
      // clear task info
      _taskStatus[tid].setToIdle();
      // clear CPU time
      if (_isThreadCPUSamplingEnabled) {
        _cpuTimeSamplesNS._currentStatsSample[tid] = 0;
      }
      // clear memory usage
      if (_isThreadMemorySamplingEnabled) {
        _memorySamplesBytes._currentStatsSample[tid] = 0;
      }
      // clear threadResourceUsageProvider
      _threadResourceUsageProvider.set(null);
      // clear _rootThread
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
          _cpuTimeSamplesNS._finishedTaskStatAggregator.remove(inactiveQueryId);
        }
        if (_isThreadMemorySamplingEnabled) {
          _memorySamplesBytes._finishedTaskStatAggregator.remove(inactiveQueryId);
        }
      }
      _inactiveQuery.clear();
      if (_isThreadCPUSamplingEnabled) {
        _inactiveQuery.addAll(_cpuTimeSamplesNS._finishedTaskStatAggregator.keySet());
      }
      if (_isThreadMemorySamplingEnabled) {
        _inactiveQuery.addAll(_memorySamplesBytes._finishedTaskStatAggregator.keySet());
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
      for (int threadId = 0; threadId < _numThreads; threadId++) {
        // sample current usage
        long currentCPUSample = _isThreadCPUSamplingEnabled
            ? _cpuTimeSamplesNS._currentStatsSample[threadId] : 0;
        long currentMemSample = _isThreadMemorySamplingEnabled
            ? _memorySamplesBytes._currentStatsSample[threadId] : 0;
        // sample current running task status
        CPUMemThreadLevelAccountingObjects.TaskEntry currentTaskStatus = _taskStatus[threadId].getThreadTaskStatus();
        LOGGER.trace("tid: {}, task: {}", threadId, currentTaskStatus);

        // get last task on the thread
        CPUMemThreadLevelAccountingObjects.TaskEntry lastQueryTask = _lastQueryTask[threadId];

        // accumulate recorded previous stat to it's _finishedTaskStatAggregator
        // if the last task on the same thread has finished
        if (!CPUMemThreadLevelAccountingObjects.TaskEntry.isSameTask(currentTaskStatus, lastQueryTask)) {
          // set previous value to current task stats
          _lastQueryTask[threadId] = currentTaskStatus;
          if (lastQueryTask != null) {
            String lastQueryId = lastQueryTask.getQueryId();
            if (_isThreadCPUSamplingEnabled) {
              long lastSample = _cpuTimeSamplesNS._lastStatSample[threadId];
              _cpuTimeSamplesNS._finishedTaskStatAggregator.merge(lastQueryId, lastSample, Long::sum);
            }
            if (_isThreadMemorySamplingEnabled) {
              long lastSample = _memorySamplesBytes._lastStatSample[threadId];
              _memorySamplesBytes._finishedTaskStatAggregator.merge(lastQueryId, lastSample, Long::sum);
            }
          }
        }

        // record current usage values for future accumulation if this task is done
        if (_isThreadCPUSamplingEnabled) {
          _cpuTimeSamplesNS._lastStatSample[threadId] = currentCPUSample;
        }
        if (_isThreadMemorySamplingEnabled) {
          _memorySamplesBytes._lastStatSample[threadId] = currentMemSample;
        }

        // if current thread is not idle
        if (currentTaskStatus != null) {
          // extract query id from queryTask string
          String queryId = currentTaskStatus.getQueryId();
          // update inactive queries for cleanInactive()
          _inactiveQuery.remove(queryId);
          // if triggered, accumulate active query task stats
          if (isTriggered) {
            Thread thread = currentTaskStatus.getAnchorThread();
            int finalThreadId = threadId;
            boolean isAnchorThread = currentTaskStatus.isAnchorThread();
            ret.compute(queryId, (k, v) -> v == null
                ? new AggregatedStats(currentCPUSample, currentMemSample, thread, isAnchorThread,
                finalThreadId, queryId)
                : v.merge(currentCPUSample, currentMemSample, isAnchorThread, finalThreadId));
          }
        }
      }

      // if triggered, accumulate stats of finished tasks of each active query
      if (isTriggered) {
        for (Map.Entry<String, AggregatedStats> queryIdResult : ret.entrySet()) {
          String activeQueryId = queryIdResult.getKey();
          long accumulatedCPUValue = _isThreadCPUSamplingEnabled
              ? _cpuTimeSamplesNS._finishedTaskStatAggregator.getOrDefault(activeQueryId, 0L) : 0;
          long accumulatedMemValue = _isThreadMemorySamplingEnabled
              ? _memorySamplesBytes._finishedTaskStatAggregator.getOrDefault(activeQueryId, 0L) : 0;
          queryIdResult.getValue().merge(accumulatedCPUValue, accumulatedMemValue,
              false, CommonConstants.Accounting.IGNORED_TASK_ID);
        }
      }
      return ret;
    }

    @Override
    public Exception getErrorStatus() {
      return _errorStatus.get(_runnerWorkerThreadOffsetProvider.get()).getAndSet(null);
    }

    /**
     * The triggered level for the actions, only the highest level action will get triggered. Severity is defined by
     * the ordinal Normal(0) does not trigger any action.
     */
    enum TriggeringLevel {
      Normal, HeapMemoryAlarmingVerbose, HeapMemoryCritical, HeapMemoryPanic
    }

    /**
     * aggregated usage of a query, _thread is the runner
     */
    static class AggregatedStats {
      final String _queryId;
      final Thread _thread;
      int _threadId;
      boolean _isAnchorThread;
      long _allocatedBytes;
      long _cpuNS;


      public AggregatedStats(long cpuNS, long allocatedBytes, Thread thread, Boolean isAnchorThread, int threadId,
          String queryId) {
        _cpuNS = cpuNS;
        _allocatedBytes = allocatedBytes;
        _thread = thread;
        _threadId = threadId;
        _queryId = queryId;
        _isAnchorThread = isAnchorThread;
      }

      @Override
      public String toString() {
        return "AggregatedStats{"
            + "_queryId='" + _queryId + '\''
            + ", _allocatedBytes=" + _allocatedBytes
            + ", _cpuNS=" + _cpuNS
            + ", _thread=" + _thread
            + ", _threadId=" + _threadId
            + '}';
      }

      public long getCpuNS() {
        return _cpuNS;
      }

      public long getAllocatedBytes() {
        return _allocatedBytes;
      }

      public Thread getThread() {
        return _thread;
      }

      public AggregatedStats merge(long cpuNS, long memoryBytes, boolean isAnchorThread, int threadId) {
        _cpuNS += cpuNS;
        _allocatedBytes += memoryBytes;

        // the merging results is from an anchor thread
        if (isAnchorThread) {
          _isAnchorThread = true;
          _threadId = threadId;
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

      // trigger gc if consecutively kill more than some number of queries
      private final int _gcTriggerCount =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_GC_BACKOFF_COUNT,
              CommonConstants.Accounting.DEFAULT_GC_BACKOFF_COUNT);

      // start to sample more frequently if heap usage exceeds this
      private final long _alarmingLevel =
          (long) (_maxHeapSize
              * _config.getProperty(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO,
              CommonConstants.Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO));

      // normal sleep time
      private final int _normalSleepTime =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME,
              CommonConstants.Accounting.DEFAULT_SLEEP_TIME);

      // alarming sleep time factor, should be > 1 to sample more frequent at alarming level
      private final int _alarmingSleepTimeFactor =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR,
              CommonConstants.Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR);

      // alarming sleep time
      private final int _alarmingSleepTime = _normalSleepTime / _alarmingSleepTimeFactor;

      // the framework would not commit to kill any query if this is disabled
      private final boolean _oomKillQueryEnabled =
          _config.getProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY,
              CommonConstants.Accounting.DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY);

      private long _usedBytes;
      private int _sleepTime;
      private int _numQueriesKilledConsecutively = 0;
      private Map<String, AggregatedStats> _aggregatedUsagePerActiveQuery;
      private TriggeringLevel _triggeringLevel;

      @Override
      public void run() {
        LOGGER.info("Starting accountant task for PerQueryCPUMemAccountant.");
        LOGGER.info("Xmx is {}", _maxHeapSize);
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
            // Act on one triggered actions
            triggeredActions();
          } catch (Exception e) {
            LOGGER.error("Caught exception while executing stats aggregation and query kill", e);
          } finally {
            if (_aggregatedUsagePerActiveQuery != null) {
              LOGGER.debug(_aggregatedUsagePerActiveQuery.toString());
            }
            // Publish server heap usage metrics
            ServerMetrics.get().setValueOfGlobalGauge(ServerGauge.JVM_HEAP_USED_BYTES, _usedBytes);
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
        if (_usedBytes > _criticalLevel) {
          _triggeringLevel = TriggeringLevel.HeapMemoryCritical;
        } else if (_usedBytes > _alarmingLevel) {
          _triggeringLevel = LOGGER.isDebugEnabled() ? TriggeringLevel.HeapMemoryAlarmingVerbose : _triggeringLevel;
          _sleepTime = _alarmingSleepTime;
        }
      }

      /**
       * Perform actions at specific triggering levels
       */
      private void triggeredActions() {
        switch (_triggeringLevel) {
          case HeapMemoryCritical:
            LOGGER.warn("Heap used bytes {} exceeds critical level", _usedBytes);
            killMostExpensiveQuery();
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
          for (int i = 0; i < _numThreads; i++) {
            CPUMemThreadLevelAccountingObjects.TaskEntry
                taskEntry = _taskStatus[i].getThreadTaskStatus();
            if (taskEntry != null && taskEntry.isAnchorThread()) {
              _errorStatus.get(i).set(new RuntimeException("Query killed due to server out of memory!"));
              taskEntry.getAnchorThread().interrupt();
              killedCount += 1;
            }
          }
          ServerMetrics.get().addMeteredGlobalValue(ServerMeter.QUERIES_PREEMPTED, killedCount);
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
        if (!_aggregatedUsagePerActiveQuery.isEmpty() && _numQueriesKilledConsecutively >= _gcTriggerCount) {
          System.gc();
          _numQueriesKilledConsecutively = 0;
          try {
            Thread.sleep(_normalSleepTime);
          } catch (InterruptedException ignored) {
          }
          _usedBytes = MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
          if (_usedBytes < _criticalLevel) {
            return;
          }
        }
        if (!(_isThreadMemorySamplingEnabled || _isThreadCPUSamplingEnabled)) {
          LOGGER.warn("Heap used bytes {} exceeds critical level", _usedBytes);
          LOGGER.warn("But unable to kill query because neither memory nor cpu tracking is enabled");
          return;
        }
        // Critical heap memory usage while no queries running
        if (_aggregatedUsagePerActiveQuery.isEmpty()) {
          LOGGER.debug("Heap used bytes {} exceeds critical level, but no active queries", _usedBytes);
          return;
        }
        AggregatedStats maxUsageTuple;
        if (_isThreadMemorySamplingEnabled) {
          maxUsageTuple = Collections.max(_aggregatedUsagePerActiveQuery.values(),
              Comparator.comparing(AggregatedStats::getAllocatedBytes));
          boolean shouldKill = _oomKillQueryEnabled && maxUsageTuple._allocatedBytes > _minMemoryFootprintForKill;
          if (shouldKill) {
            _errorStatus.get(maxUsageTuple._threadId)
                .set(new RuntimeException(String.format(" Query %s got killed because using %d bytes of memory, "
                        + "exceeding the quota", maxUsageTuple._queryId, maxUsageTuple.getAllocatedBytes())));
            interruptRunnerThread(maxUsageTuple.getThread());
          }
          LOGGER.warn("Heap used bytes {} exceeds critical level {}", _usedBytes, _criticalLevel);
          LOGGER.error("Query {} got picked because using {} bytes of memory, actual kill committed {}",
              maxUsageTuple._queryId, maxUsageTuple._allocatedBytes, shouldKill);
        } else {
          maxUsageTuple = Collections.max(_aggregatedUsagePerActiveQuery.values(),
              Comparator.comparing(AggregatedStats::getCpuNS));
          if (_oomKillQueryEnabled) {
            _errorStatus.get(maxUsageTuple._threadId)
                .set(new RuntimeException(String.format(" Query %s got killed because server memory pressure, using "
                        + "%d ns of CPU time", maxUsageTuple._queryId, maxUsageTuple.getAllocatedBytes())));
            interruptRunnerThread(maxUsageTuple.getThread());
          }
          LOGGER.warn("Heap used bytes {} exceeds critical level {}", _usedBytes, _criticalLevel);
          LOGGER.error("Query {} got picked because using {} ns of cpu time, actual kill committed {}",
              maxUsageTuple._allocatedBytes, maxUsageTuple._queryId, _oomKillQueryEnabled);
        }
        LOGGER.warn("Query aggregation results {}", _aggregatedUsagePerActiveQuery.toString());
      }

      private void interruptRunnerThread(Thread thread) {
        thread.interrupt();
        ServerMetrics.get().addMeteredGlobalValue(ServerMeter.QUERIES_PREEMPTED, 1);
        _numQueriesKilledConsecutively += 1;
      }
    }
  }
}
