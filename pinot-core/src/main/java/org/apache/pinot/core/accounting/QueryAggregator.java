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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ResourceUsageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Aggregator that computes resource aggregation for queries. Most of the logic from PerQueryCPUMemAccountantFactory is
 * retained here for backward compatibility.
 *
 * Design and algorithm are outlined in
 * https://docs.google.com/document/d/1Z9DYAfKznHQI9Wn8BjTWZYTcNRVGiPP0B8aEP3w_1jQ
 */
public class QueryAggregator implements ResourceAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAggregator.class);

  enum TriggeringLevel {
    Normal, HeapMemoryAlarmingVerbose, CPUTimeBasedKilling, HeapMemoryCritical, HeapMemoryPanic
  }

  // For one time concurrent update of stats. This is to provide stats collection for parts that are not
  // performance sensitive and query_id is not known beforehand (e.g. broker inbound netty thread)
  private final ConcurrentHashMap<String, Long> _concurrentTaskCPUStatsAggregator = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> _concurrentTaskMemStatsAggregator = new ConcurrentHashMap<>();

  // for stats aggregation of finished (worker) threads when the runner is still running.
  private final HashMap<String, Long> _finishedTaskCPUStatsAggregator = new HashMap<>();
  private final HashMap<String, Long> _finishedTaskMemStatsAggregator = new HashMap<>();

  private final boolean _isThreadCPUSamplingEnabled;
  private final boolean _isThreadMemorySamplingEnabled;

  private final Set<String> _inactiveQuery;
  private final PinotConfiguration _config;

  private final InstanceType _instanceType;
  private final String _instanceId;

  // max heap usage, Xmx
  private final long _maxHeapSize = ResourceUsageUtils.getMaxHeapSize();

  // don't kill a query if its memory footprint is below some ratio of _maxHeapSize
  private final long _minMemoryFootprintForKill;

  // kill all queries if heap usage exceeds this
  private final long _panicLevel;

  // kill the most expensive query if heap usage exceeds this
  private final long _criticalLevel;

  // if after gc the heap usage is still above this, kill the most expensive query
  // use this to prevent heap size oscillation and repeatedly triggering gc
  private final long _criticalLevelAfterGC;

  // trigger gc if consecutively kill more than some number of queries
  // set this to 0 to always trigger gc before killing a query to give gc a second chance
  // as would minimize the chance of false positive killing in some usecases
  // should consider use -XX:+ExplicitGCInvokesConcurrent to avoid STW for some gc algorithms
  private final int _gcBackoffCount;

  // start to sample more frequently if heap usage exceeds this
  private final long _alarmingLevel;

  // normal sleep time
  private final int _normalSleepTime;

  // wait for gc to complete, according to system.gc() javadoc, when control returns from the method call,
  // the Java Virtual Machine has made a best effort to reclaim space from all discarded objects.
  // Therefore, we default this to 0.
  // Tested with Shenandoah GC and G1GC, with -XX:+ExplicitGCInvokesConcurrent
  private final int _gcWaitTime;

  // alarming sleep time denominator, should be > 1 to sample more frequent at alarming level
  private final int _alarmingSleepTimeDenominator;

  // alarming sleep time
  private final int _alarmingSleepTime;

  // the framework would not commit to kill any query if this is disabled
  private final boolean _oomKillQueryEnabled;

  // if we want to publish the heap usage
  private final boolean _publishHeapUsageMetric;

  // if we want kill query based on CPU time
  private final boolean _isCPUTimeBasedKillingEnabled;

  // CPU time based killing threshold
  private final long _cpuTimeBasedKillingThresholdNS;

  private final boolean _isQueryKilledMetricEnabled;

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

  // Add constructor
  public QueryAggregator(boolean isThreadCPUSamplingEnabled, boolean isThreadMemSamplingEnabled,
      PinotConfiguration config, InstanceType instanceType, String instanceId) {
    _isThreadCPUSamplingEnabled = isThreadCPUSamplingEnabled;
    _isThreadMemorySamplingEnabled = isThreadMemSamplingEnabled;
    _config = config;
    _instanceType = instanceType;
    _instanceId = instanceId;

    _minMemoryFootprintForKill = (long) (_maxHeapSize * _config.getProperty(
        CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO,
        CommonConstants.Accounting.DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO));
    _panicLevel =
        (long) (_maxHeapSize * _config.getProperty(CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO,
            CommonConstants.Accounting.DFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO));
    _criticalLevel =
        (long) (_maxHeapSize * _config.getProperty(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO,
            CommonConstants.Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO));
    _criticalLevelAfterGC = _criticalLevel - (long) (_maxHeapSize * _config.getProperty(
        CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC,
        CommonConstants.Accounting.DEFAULT_CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC));
    _gcBackoffCount = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_GC_BACKOFF_COUNT,
        CommonConstants.Accounting.DEFAULT_GC_BACKOFF_COUNT);
    _alarmingLevel =
        (long) (_maxHeapSize * _config.getProperty(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO,
            CommonConstants.Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO));
    _normalSleepTime = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_MS,
        CommonConstants.Accounting.DEFAULT_SLEEP_TIME_MS);
    _gcWaitTime = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_GC_WAIT_TIME_MS,
        CommonConstants.Accounting.DEFAULT_CONFIG_OF_GC_WAIT_TIME_MS);
    _alarmingSleepTimeDenominator = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_DENOMINATOR,
        CommonConstants.Accounting.DEFAULT_SLEEP_TIME_DENOMINATOR);
    _alarmingSleepTime = _normalSleepTime / _alarmingSleepTimeDenominator;
    _oomKillQueryEnabled = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY,
        CommonConstants.Accounting.DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY);
    _publishHeapUsageMetric = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_PUBLISHING_JVM_USAGE,
        CommonConstants.Accounting.DEFAULT_PUBLISHING_JVM_USAGE);
    _isCPUTimeBasedKillingEnabled =
        _config.getProperty(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED,
            CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_ENABLED) && _isThreadCPUSamplingEnabled;
    _cpuTimeBasedKillingThresholdNS =
        _config.getProperty(CommonConstants.Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS,
            CommonConstants.Accounting.DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS) * 1000_000L;
    _isQueryKilledMetricEnabled = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_QUERY_KILLED_METRIC_ENABLED,
        CommonConstants.Accounting.DEFAULT_QUERY_KILLED_METRIC_ENABLED);

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

    _inactiveQuery = new HashSet<>();

    LOGGER.info("Starting accountant task for QueryAggregator.");
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
    LOGGER.info("_isCPUTimeBasedKillingEnabled: {}, _cpuTimeBasedKillingThresholdNS: {}", _isCPUTimeBasedKillingEnabled,
        _cpuTimeBasedKillingThresholdNS);
  }

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
      return "AggregatedStats{" + "_queryId='" + _queryId + '\'' + ", _anchorThread=" + _anchorThread
          + ", _isAnchorThread=" + _isAnchorThread + ", _exceptionAtomicReference=" + _exceptionAtomicReference
          + ", _allocatedBytes=" + _allocatedBytes + ", _cpuNS=" + _cpuNS + '}';
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

  public int getAggregationSleepTimeMs() {
    return _sleepTime;
  }

  // Implement getQueryResources

  public void preAggregate(List<CPUMemThreadLevelAccountingObjects.ThreadEntry> anchorThreadEntries) {
    LOGGER.debug("Running pre-aggregate for QueryAggregator.");
    _sleepTime = _normalSleepTime;
    _triggeringLevel = TriggeringLevel.Normal;
    collectTriggerMetrics();
    evalTriggers();
    if (_triggeringLevel == TriggeringLevel.HeapMemoryPanic) {
      killAllQueries(anchorThreadEntries);
      LOGGER.error("Killed all queries and triggered gc!");
      // Set the triggering level back to normal for aggregation phase.
      _triggeringLevel = TriggeringLevel.Normal;
    }

    _aggregatedUsagePerActiveQuery = null;
    if (isTriggered()) {
      _aggregatedUsagePerActiveQuery = new HashMap<>();
    }
  }

  public void aggregate(Thread thread, CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry) {
    // ThreadEntry returns 0 if CPU/Mem sampling is not enabled.
    long currentCPUSample = threadEntry._currentThreadCPUTimeSampleMS;
    long currentMemSample = threadEntry._currentThreadMemoryAllocationSampleBytes;
    // sample current running task status
    CPUMemThreadLevelAccountingObjects.TaskEntry currentTaskStatus = threadEntry.getCurrentThreadTaskStatus();
    LOGGER.trace("tid: {}, task: {}", thread.getId(), currentTaskStatus);

    // get last task on the thread
    CPUMemThreadLevelAccountingObjects.TaskEntry lastQueryTask = threadEntry._previousThreadTaskStatus;

    // accumulate recorded previous stat to it's _finishedTaskStatAggregator
    // if the last task on the same thread has finished
    if (!(currentTaskStatus == lastQueryTask)) {
      // set previous value to current task stats
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

    // if current thread is not idle
    if (currentTaskStatus != null) {
      // extract query id from queryTask string
      String queryId = currentTaskStatus.getQueryId();
      // update inactive queries for cleanInactive()
      _inactiveQuery.remove(queryId);
      // if triggered, accumulate active query task stats
      if (isTriggered()) {
        Thread anchorThread = currentTaskStatus.getAnchorThread();
        boolean isAnchorThread = currentTaskStatus.isAnchorThread();
        _aggregatedUsagePerActiveQuery.compute(queryId,
            (k, v) -> v == null ? new AggregatedStats(currentCPUSample, currentMemSample, anchorThread, isAnchorThread,
                threadEntry._errorStatus, queryId)
                : v.merge(currentCPUSample, currentMemSample, isAnchorThread, threadEntry._errorStatus));
      }
    }
  }

  public void postAggregate() {
    if (!isTriggered()) {
      return;
    }

    for (Map.Entry<String, AggregatedStats> queryIdResult : _aggregatedUsagePerActiveQuery.entrySet()) {
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
      _usedBytes = ResourceUsageUtils.getUsedHeapSize();
      if (_usedBytes < _criticalLevelAfterGC) {
        return;
      }
      LOGGER.error("After GC, heap used bytes {} still exceeds _criticalLevelAfterGC level {}", _usedBytes,
          _criticalLevelAfterGC);
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
        maxUsageTuple._exceptionAtomicReference.set(new RuntimeException(
            String.format(" Query %s got killed because using %d bytes of memory on %s: %s, exceeding the quota",
                maxUsageTuple._queryId, maxUsageTuple.getAllocatedBytes(), _instanceType, _instanceId)));
        interruptRunnerThread(maxUsageTuple.getAnchorThread());
        logTerminatedQuery(maxUsageTuple, _usedBytes);
      } else if (!_oomKillQueryEnabled) {
        LOGGER.warn("Query {} got picked because using {} bytes of memory, actual kill committed false "
            + "because oomKillQueryEnabled is false", maxUsageTuple._queryId, maxUsageTuple._allocatedBytes);
      } else {
        LOGGER.warn("But all queries are below quota, no query killed");
      }
    }
    logQueryResourceUsage(_aggregatedUsagePerActiveQuery);
  }

  protected void logQueryResourceUsage(Map<String, ? extends QueryResourceTracker> aggregatedUsagePerActiveQuery) {
    LOGGER.warn("Query aggregation results {} for the previous kill.", aggregatedUsagePerActiveQuery);
  }

  protected void logTerminatedQuery(QueryResourceTracker queryResourceTracker, long totalHeapMemoryUsage) {
    LOGGER.warn("Query {} terminated. Memory Usage: {}. Cpu Usage: {}. Total Heap Usage: {}",
        queryResourceTracker.getQueryId(), queryResourceTracker.getAllocatedBytes(),
        queryResourceTracker.getCpuTimeNs(), totalHeapMemoryUsage);
  }

  private void killCPUTimeExceedQueries() {
    for (Map.Entry<String, AggregatedStats> entry : _aggregatedUsagePerActiveQuery.entrySet()) {
      AggregatedStats value = entry.getValue();
      if (value._cpuNS > _cpuTimeBasedKillingThresholdNS) {
        LOGGER.error("Query {} got picked because using {} ns of cpu time," + " greater than threshold {}",
            value._queryId, value.getCpuTimeNs(), _cpuTimeBasedKillingThresholdNS);
        value._exceptionAtomicReference.set(new RuntimeException(String.format(
            "Query %s got killed on %s: %s because using %d " + "CPU time exceeding limit of %d ns CPU time",
            value._queryId, _instanceType, _instanceId, value.getCpuTimeNs(), _cpuTimeBasedKillingThresholdNS)));
        interruptRunnerThread(value.getAnchorThread());
        logTerminatedQuery(value, _usedBytes);
      }
    }
    logQueryResourceUsage(_aggregatedUsagePerActiveQuery);
  }

  private void interruptRunnerThread(Thread thread) {
    thread.interrupt();
    if (_isQueryKilledMetricEnabled) {
      _metrics.addMeteredGlobalValue(_queryKilledMeter, 1);
    }
    _numQueriesKilledConsecutively += 1;
  }

  void killAllQueries(List<CPUMemThreadLevelAccountingObjects.ThreadEntry> anchorThreadEntries) {
    if (_oomKillQueryEnabled) {
      LOGGER.error("Heap used bytes {}, greater than _panicLevel {}, Killing all queries and triggering gc!",
          _usedBytes, _panicLevel);

      int killedCount = 0;

      for (CPUMemThreadLevelAccountingObjects.ThreadEntry anchorThreadEntry : anchorThreadEntries) {
        CPUMemThreadLevelAccountingObjects.TaskEntry taskEntry = anchorThreadEntry.getCurrentThreadTaskStatus();
        anchorThreadEntry._errorStatus.set(
            new RuntimeException(String.format("Query killed due to %s out of memory!", _instanceType)));
        taskEntry.getAnchorThread().interrupt();
        killedCount += 1;
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

  private void evalTriggers() {
    // CPU based triggers
    if (_isCPUTimeBasedKillingEnabled) {
      _triggeringLevel = TriggeringLevel.CPUTimeBasedKilling;
    }

    // Memory based triggers
    if (_usedBytes >= _panicLevel) {
      _triggeringLevel = TriggeringLevel.HeapMemoryPanic;
      _metrics.addMeteredGlobalValue(_heapMemoryPanicExceededMeter, 1);
    } else if (_usedBytes > _criticalLevel) {
      _triggeringLevel = TriggeringLevel.HeapMemoryCritical;
      _metrics.addMeteredGlobalValue(_heapMemoryCriticalExceededMeter, 1);
    } else if (_usedBytes > _alarmingLevel) {
      _sleepTime = _alarmingSleepTime;
      _triggeringLevel = (LOGGER.isDebugEnabled() && _triggeringLevel == TriggeringLevel.Normal)
          ? TriggeringLevel.HeapMemoryAlarmingVerbose : _triggeringLevel;
    }
  }

  @Override
  public void updateConcurrentCpuUsage(String queryId, long cpuUsageNS) {
    _concurrentTaskCPUStatsAggregator.compute(queryId,
        (key, value) -> (value == null) ? cpuUsageNS : (value + cpuUsageNS));
  }

  @Override
  public void updateConcurrentMemUsage(String queryId, long memoryAllocatedBytes) {
    _concurrentTaskMemStatsAggregator.compute(queryId,
        (key, value) -> (value == null) ? memoryAllocatedBytes : (value + memoryAllocatedBytes));
  }

  private void cleanInactive() {
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

  public void cleanUpPostAggregation() {
    LOGGER.debug(_aggregatedUsagePerActiveQuery == null ? "_aggregatedUsagePerActiveQuery : null"
        : _aggregatedUsagePerActiveQuery.toString());
    if (_publishHeapUsageMetric) {
      _metrics.setValueOfGlobalGauge(_memoryUsageGauge, _usedBytes);
    }
    cleanInactive();
  }

  /**
   * Uses a read-only copy of ThreadEntriesMap to aggregates resource usage from all active threads and groups by
   * queryId.
   *
   * @param threadEntriesMap immutable, read-only map for aggregating query stats.
   * @return
   */
  public Map<String, ? extends QueryResourceTracker> getQueryResources(
      Map<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntriesMap) {
    HashMap<String, AggregatedStats> ret = new HashMap<>();

    // for each {pqr, pqw}
    for (Map.Entry<Thread, CPUMemThreadLevelAccountingObjects.ThreadEntry> entry : threadEntriesMap.entrySet()) {
      // sample current usage
      CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry = entry.getValue();
      long currentCPUSample = _isThreadCPUSamplingEnabled ? threadEntry._currentThreadCPUTimeSampleMS : 0;
      long currentMemSample =
          _isThreadMemorySamplingEnabled ? threadEntry._currentThreadMemoryAllocationSampleBytes : 0;
      // sample current running task status
      CPUMemThreadLevelAccountingObjects.TaskEntry currentTaskStatus = threadEntry.getCurrentThreadTaskStatus();
      Thread thread = entry.getKey();
      LOGGER.trace("tid: {}, task: {}", thread.getId(), currentTaskStatus);

      // if current thread is not idle
      if (currentTaskStatus != null) {
        // extract query id from queryTask string
        String queryId = currentTaskStatus.getQueryId();
        if (queryId != null) {
          Thread anchorThread = currentTaskStatus.getAnchorThread();
          boolean isAnchorThread = currentTaskStatus.isAnchorThread();
          ret.compute(queryId,
              (k, v) -> v == null ? new AggregatedStats(currentCPUSample, currentMemSample, anchorThread,
                  isAnchorThread, threadEntry._errorStatus, queryId)
                  : v.merge(currentCPUSample, currentMemSample, isAnchorThread, threadEntry._errorStatus));
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

  private void collectTriggerMetrics() {
    _usedBytes = ResourceUsageUtils.getUsedHeapSize();
    LOGGER.debug("Heap used bytes {}", _usedBytes);
  }

  private boolean isTriggered() {
    return _triggeringLevel.ordinal() > TriggeringLevel.Normal.ordinal();
  }
}
