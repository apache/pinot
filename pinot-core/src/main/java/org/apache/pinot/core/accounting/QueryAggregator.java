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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.spi.accounting.MseCancelCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregator that computes resource aggregation for queries. Most of the logic from PerQueryCPUMemAccountantFactory is
 * retained here for backward compatibility.
 *
 * TODO: Integrate recent changes in PerQueryCPUMemAccountantFactory
 */
public class QueryAggregator implements ResourceAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAggregator.class);
  private static final boolean IS_DEBUG_MODE_ENABLED = LOGGER.isDebugEnabled();

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

  Cache<String, MseCancelCallback> _queryCancelCallbacks;

  private final boolean _isThreadCPUSamplingEnabled;
  private final boolean _isThreadMemorySamplingEnabled;

  private final Set<String> _inactiveQuery;
  private Set<String> _cancelSentQueries;
  private final PinotConfiguration _config;

  private final InstanceType _instanceType;
  private final String _instanceId;

  // Centralized configuration using QueryMonitorConfig
  private final AtomicReference<QueryMonitorConfig> _queryMonitorConfig = new AtomicReference<>();

  private long _usedBytes;
  private int _sleepTime;
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

    // Initialize QueryMonitorConfig with all centralized configuration
    _queryMonitorConfig.set(new QueryMonitorConfig(_config, ResourceUsageUtils.getMaxHeapSize()));

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
    _cancelSentQueries = ConcurrentHashMap.newKeySet();
    _queryCancelCallbacks = CacheBuilder.newBuilder()
        .maximumSize(_config.getProperty(CommonConstants.Accounting.CONFIG_OF_CANCEL_CALLBACK_CACHE_MAX_SIZE,
            CommonConstants.Accounting.DEFAULT_CANCEL_CALLBACK_CACHE_MAX_SIZE))
        .expireAfterWrite(_config.getProperty(CommonConstants.Accounting.CONFIG_OF_CANCEL_CALLBACK_CACHE_EXPIRY_SECONDS,
            CommonConstants.Accounting.DEFAULT_CANCEL_CALLBACK_CACHE_EXPIRY_SECONDS), TimeUnit.SECONDS)
        .build();

    LOGGER.info("Starting accountant task for QueryAggregator.");
    logQueryMonitorConfig();
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

  /**
   * Get the current QueryMonitorConfig
   */
  public QueryMonitorConfig getQueryMonitorConfig() {
    return _queryMonitorConfig.get();
  }

  /**
   * Log the current QueryMonitorConfig settings
   */
  private void logQueryMonitorConfig() {
    QueryMonitorConfig config = getQueryMonitorConfig();
    LOGGER.info("Updated Configuration for Query Monitor");
    LOGGER.info("Xmx is {}", config.getMaxHeapSize());
    LOGGER.info("_instanceType is {}", _instanceType);
    LOGGER.info("_alarmingLevel of on heap memory is {}", config.getAlarmingLevel());
    LOGGER.info("_criticalLevel of on heap memory is {}", config.getCriticalLevel());
    LOGGER.info("_panicLevel of on heap memory is {}", config.getPanicLevel());
    LOGGER.info("_normalSleepTime is {}", config.getNormalSleepTime());
    LOGGER.info("_alarmingSleepTime is {}", config.getAlarmingSleepTime());
    LOGGER.info("_oomKillQueryEnabled: {}", config.isOomKillQueryEnabled());
    LOGGER.info("_minMemoryFootprintForKill: {}", config.getMinMemoryFootprintForKill());
    LOGGER.info("_isCPUTimeBasedKillingEnabled: {}, _cpuTimeBasedKillingThresholdNS: {}",
        config.isCpuTimeBasedKillingEnabled(), config.getCpuTimeBasedKillingThresholdNS());
  }

  /**
   * Register MSE cancel callback for graceful query termination
   */
  public void registerMseCancelCallback(String queryId, MseCancelCallback callback) {
    _queryCancelCallbacks.put(queryId, callback);
  }

  @Nullable
  public MseCancelCallback getQueryCancelCallback(String queryId) {
    return _queryCancelCallbacks.getIfPresent(queryId);
  }

  // Implement getQueryResources

  public void preAggregate(List<CPUMemThreadLevelAccountingObjects.ThreadEntry> anchorThreadEntries) {
    LOGGER.debug("Running pre-aggregate for QueryAggregator.");
    QueryMonitorConfig config = getQueryMonitorConfig();
    _sleepTime = config.getNormalSleepTime();
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
    QueryMonitorConfig config = getQueryMonitorConfig();
    switch (_triggeringLevel) {
      case HeapMemoryCritical:
        LOGGER.warn("Heap used bytes {} exceeds critical level {}", _usedBytes, config.getCriticalLevel());
        killMostExpensiveQuery();
        break;
      case CPUTimeBasedKilling:
        killCPUTimeExceedQueries();
        break;
      case HeapMemoryAlarmingVerbose:
        LOGGER.debug("Query usage aggregation results: {}", _aggregatedUsagePerActiveQuery);
        break;
      default:
        break;
    }
  }

  /**
   * Kill the query with the highest cost (memory footprint/cpu time/...)
   * Will trigger gc when killing a consecutive number of queries
   * use XX:+ExplicitGCInvokesConcurrent to avoid a full gc when system.gc is triggered
   */
  private void killMostExpensiveQuery() {
    if (!_isThreadMemorySamplingEnabled) {
      return;
    }
    if (!_aggregatedUsagePerActiveQuery.isEmpty()) {
      AggregatedStats maxUsageTuple = _aggregatedUsagePerActiveQuery.values()
          .stream()
          .filter(stats -> !_cancelSentQueries.contains(stats.getQueryId()))
          .max(Comparator.comparing(AggregatedStats::getAllocatedBytes))
          .orElse(null);
      if (maxUsageTuple != null) {
        String queryId = maxUsageTuple.getQueryId();
        long allocatedBytes = maxUsageTuple.getAllocatedBytes();
        QueryMonitorConfig config = getQueryMonitorConfig();
        if (allocatedBytes > config.getMinMemoryFootprintForKill()) {
          if (config.isOomKillQueryEnabled()) {
            maxUsageTuple._exceptionAtomicReference.set(new RuntimeException(
                String.format("Query: %s got killed on %s: %s because it allocated: %d bytes of memory", queryId,
                    _instanceType, _instanceId, allocatedBytes)));
            boolean hasCallBack = _queryCancelCallbacks.getIfPresent(maxUsageTuple.getQueryId()) != null;
            terminateQuery(maxUsageTuple);
            logTerminatedQuery(maxUsageTuple, _usedBytes, hasCallBack);
          } else {
            LOGGER.warn("Query: {} got picked because it allocated: {} bytes of memory, "
                + "not killing it because OOM kill is not enabled", queryId, allocatedBytes);
          }
        } else {
          LOGGER.debug(
              "Query: {} has most allocated bytes: {}, but below the minimum memory footprint for kill: {}, "
                  + "skipping query kill", queryId, allocatedBytes, config.getMinMemoryFootprintForKill());
        }
      }
      logQueryResourceUsage(_aggregatedUsagePerActiveQuery);
    } else {
      LOGGER.debug("No active queries to kill");
    }
  }

  protected void logQueryResourceUsage(Map<String, ? extends QueryResourceTracker> aggregatedUsagePerActiveQuery) {
    LOGGER.warn("Query aggregation results {} for the previous kill.", aggregatedUsagePerActiveQuery);
  }

  protected void logTerminatedQuery(QueryResourceTracker queryResourceTracker, long totalHeapMemoryUsage) {
    logTerminatedQuery(queryResourceTracker, totalHeapMemoryUsage, false);
  }

  protected void logTerminatedQuery(QueryResourceTracker queryResourceTracker, long totalHeapMemoryUsage, boolean hasCallback) {
    LOGGER.warn("Query {} terminated. Memory Usage: {}. Cpu Usage: {}. Total Heap Usage: {}. Used Callback: {}",
        queryResourceTracker.getQueryId(), queryResourceTracker.getAllocatedBytes(),
        queryResourceTracker.getCpuTimeNs(), totalHeapMemoryUsage, hasCallback);
  }

  private void killCPUTimeExceedQueries() {
    if (!_isThreadCPUSamplingEnabled) {
      return;
    }
    if (!_aggregatedUsagePerActiveQuery.isEmpty()) {
      QueryMonitorConfig config = getQueryMonitorConfig();
      for (Map.Entry<String, AggregatedStats> entry : _aggregatedUsagePerActiveQuery.entrySet()) {
        AggregatedStats stats = entry.getValue();
        String queryId = stats.getQueryId();
        if (_cancelSentQueries.contains(queryId)) {
          continue;
        }
        long cpuTimeNs = stats.getCpuTimeNs();
        if (cpuTimeNs > config.getCpuTimeBasedKillingThresholdNS()) {

          LOGGER.debug("Query {} got picked because using {} ns of cpu time, greater than threshold {}",
              queryId, cpuTimeNs, config.getCpuTimeBasedKillingThresholdNS());

          stats._exceptionAtomicReference.set(new RuntimeException(String.format(
              "Query: %s got killed on %s: %s because it used: %d ns of CPU time (exceeding threshold: %d)",
              queryId, _instanceType, _instanceId, cpuTimeNs, config.getCpuTimeBasedKillingThresholdNS())));
          boolean hasCallBack = _queryCancelCallbacks.getIfPresent(queryId) != null;
          terminateQuery(stats);
          logTerminatedQuery(stats, _usedBytes, hasCallBack);
        }
      }
      logQueryResourceUsage(_aggregatedUsagePerActiveQuery);
    } else {
      LOGGER.debug("No active queries to kill");
    }
  }

  /**
   * Enhanced query termination with MSE callback support
   */
  private void terminateQuery(AggregatedStats queryResourceTracker) {
    cancelQuery(queryResourceTracker.getQueryId(), queryResourceTracker.getAnchorThread());
    if (getQueryMonitorConfig().isQueryKilledMetricEnabled()) {
      _metrics.addMeteredGlobalValue(_queryKilledMeter, 1);
    }
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

  void killAllQueries(List<CPUMemThreadLevelAccountingObjects.ThreadEntry> anchorThreadEntries) {
    QueryMonitorConfig config = getQueryMonitorConfig();

    if (config.isOomKillQueryEnabled()) {
      int killedCount = 0;
      for (CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry : anchorThreadEntries) {
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

  private void evalTriggers() {
    TriggeringLevel previousTriggeringLevel = _triggeringLevel;

    // Compute the new triggering level based on current conditions
    QueryMonitorConfig config = getQueryMonitorConfig();
    _triggeringLevel
        = config.isCpuTimeBasedKillingEnabled() ? TriggeringLevel.CPUTimeBasedKilling : TriggeringLevel.Normal;

    // Memory based triggers (higher priority)
    if (_usedBytes >= config.getPanicLevel()) {
      _triggeringLevel = TriggeringLevel.HeapMemoryPanic;
      _metrics.addMeteredGlobalValue(_heapMemoryPanicExceededMeter, 1);
    } else if (_usedBytes > config.getCriticalLevel()) {
      _triggeringLevel = TriggeringLevel.HeapMemoryCritical;
      _metrics.addMeteredGlobalValue(_heapMemoryCriticalExceededMeter, 1);
    } else if (_usedBytes > config.getAlarmingLevel()) {
      _sleepTime = config.getAlarmingSleepTime();
      // For debugging - only set verbose mode if debug is enabled and we were in normal mode
      if (IS_DEBUG_MODE_ENABLED && _triggeringLevel == TriggeringLevel.Normal) {
        _triggeringLevel = TriggeringLevel.HeapMemoryAlarmingVerbose;
      }
    }

    // Log triggering level changes for better observability
    if (previousTriggeringLevel != _triggeringLevel) {
      switch (_triggeringLevel) {
        case HeapMemoryPanic:
          LOGGER.error("Heap used bytes: {} exceeds panic level: {}, killing all queries", _usedBytes,
              config.getPanicLevel());
          break;
        case HeapMemoryCritical:
          LOGGER.warn("Heap used bytes: {} exceeds critical level: {}, killing most expensive query", _usedBytes,
              config.getCriticalLevel());
          if (!_isThreadMemorySamplingEnabled) {
            LOGGER.error("Unable to terminate queries as memory tracking is not enabled");
          }
          break;
        case CPUTimeBasedKilling:
          LOGGER.info("Entering CPU time based killing mode, threshold (ns): {}",
              config.getCpuTimeBasedKillingThresholdNS());
          if (!_isThreadCPUSamplingEnabled) {
            LOGGER.error("Unable to terminate queries as CPU time tracking is not enabled");
          }
          break;
        case HeapMemoryAlarmingVerbose:
          LOGGER.debug("Heap used bytes: {} exceeds alarming level: {}", _usedBytes, config.getAlarmingLevel());
          break;
        case Normal:
          LOGGER.info("Heap used bytes: {} drops to safe zone, entering normal mode", _usedBytes);
          break;
        default:
          throw new IllegalStateException("Unsupported triggering level: " + _triggeringLevel);
      }
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
    Set<String> cancellingQueries = new HashSet<>();

    for (String inactiveQueryId : _inactiveQuery) {
      if (_isThreadCPUSamplingEnabled) {
        _finishedTaskCPUStatsAggregator.remove(inactiveQueryId);
        _concurrentTaskCPUStatsAggregator.remove(inactiveQueryId);
      }
      if (_isThreadMemorySamplingEnabled) {
        _finishedTaskMemStatsAggregator.remove(inactiveQueryId);
        _concurrentTaskMemStatsAggregator.remove(inactiveQueryId);
      }
      // Clean up cancel callbacks for inactive queries
      _queryCancelCallbacks.invalidate(inactiveQueryId);
    }

    // Retain queries that are currently being cancelled
    for (String queryId : _cancelSentQueries) {
      if (_finishedTaskCPUStatsAggregator.containsKey(queryId) ||
          _finishedTaskMemStatsAggregator.containsKey(queryId) ||
          _concurrentTaskCPUStatsAggregator.containsKey(queryId) ||
          _concurrentTaskMemStatsAggregator.containsKey(queryId)) {
        cancellingQueries.add(queryId);
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

    _cancelSentQueries = cancellingQueries;
  }

  public void cleanUpPostAggregation() {
    LOGGER.debug(_aggregatedUsagePerActiveQuery == null ? "_aggregatedUsagePerActiveQuery : null"
        : _aggregatedUsagePerActiveQuery.toString());
    QueryMonitorConfig config = getQueryMonitorConfig();
    if (config.isPublishHeapUsageMetric()) {
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
    for (CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry : threadEntriesMap.values()) {
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

  public long getHeapUsageBytes() {
    return _usedBytes;
  }

  private void collectTriggerMetrics() {
    _usedBytes = ResourceUsageUtils.getUsedHeapSize();
    LOGGER.debug("Heap used bytes {}", _usedBytes);
  }

  private boolean isTriggered() {
    return _triggeringLevel.ordinal() > TriggeringLevel.Normal.ordinal();
  }

  public boolean isQueryTerminated(String queryId) {
    QueryMonitorConfig config = getQueryMonitorConfig();
    if (config.isThreadSelfTerminate() && getHeapUsageBytes() > config.getPanicLevel()) {
      logSelfTerminatedQuery(queryId, Thread.currentThread());
      return true;
    }
    return false;
  }

  protected void logSelfTerminatedQuery(String queryId, Thread queryThread) {
    if (_cancelSentQueries.add(queryId)) {
      LOGGER.warn("Query: {} self-terminated. Total Heap Usage: {}. Query Thread: {}", queryId, _usedBytes,
          queryThread.getName());
    }
  }
}
