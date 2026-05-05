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

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.longs.LongLongMutablePair;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ResourceUsageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Aggregator that computes resource aggregation for queries. Most of the logic from PerQueryCPUMemAccountantFactory is
 * retained here for backward compatibility.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class QueryResourceAggregator implements ResourceAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryResourceAggregator.class);

  enum TriggeringLevel {
    Normal, HeapMemoryAlarmingVerbose, CPUTimeBasedKilling, HeapMemoryCritical, HeapMemoryPanic
  }

  // Tracks the CPU and memory usage for queries not tracked by any thread. Key is query id.
  // E.g. request ser/de where thread execution context cannot be set up beforehand; tasks already finished.
  private final ConcurrentHashMap<String, LongLongMutablePair> _untrackedCpuMemUsage = new ConcurrentHashMap<>();

  // Tracks the query id of the inactive queries, which is used to clean up entries in _untrackedMemCpuUsage.
  private final Set<String> _inactiveQueries = new HashSet<>();

  private final String _instanceId;
  private final InstanceType _instanceType;
  private final boolean _cpuSamplingEnabled;
  private final boolean _memorySamplingEnabled;
  private final AtomicReference<QueryMonitorConfig> _queryMonitorConfig;

  private volatile long _usedBytes;
  private int _sleepTime;
  // Key is query id.
  private Map<String, QueryResourceTrackerImpl> _queryResourceUsages;
  private TriggeringLevel _triggeringLevel = TriggeringLevel.Normal;

  // Track previous triggering level to detect transitions into critical
  private TriggeringLevel _previousTriggeringLevel = TriggeringLevel.Normal;

  // OOM pause state - lock+condition for blocking query threads at critical heap level.
  // _pauseLock / _pauseCondition are used to coordinate blocking query threads and signaling them from the watcher
  // thread when the pause is cleared.
  private final ReentrantLock _pauseLock = new ReentrantLock();
  private final Condition _pauseCondition = _pauseLock.newCondition();
  // Shared between the watcher thread (writer) and query threads (readers). Volatile enables the fast-path in
  // waitIfPaused() to avoid acquiring the lock when no pause is active.
  private volatile boolean _pauseActive = false;
  // Local state for the watcher thread only (similar to _triggeringLevel) - read/written only from preAggregate and
  // postAggregate, never by query threads.
  private long _pauseDeadlineMs = 0;

  // metrics class
  private final AbstractMetrics _metrics;
  private final AbstractMetrics.Meter _queryKilledMeter;
  private final AbstractMetrics.Meter _heapMemoryCriticalExceededMeter;
  private final AbstractMetrics.Meter _heapMemoryPanicExceededMeter;
  private final AbstractMetrics.Gauge _memoryUsageGauge;

  public QueryResourceAggregator(String instanceId, InstanceType instanceType, boolean cpuSamplingEnabled,
      boolean memorySamplingEnabled, AtomicReference<QueryMonitorConfig> queryMonitorConfig) {
    _instanceId = instanceId;
    _instanceType = instanceType;
    _cpuSamplingEnabled = cpuSamplingEnabled;
    _memorySamplingEnabled = memorySamplingEnabled;
    _queryMonitorConfig = queryMonitorConfig;

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
  public void updateUntrackedResourceUsage(String queryId, long cpuTimeNs, long allocatedBytes) {
    _untrackedCpuMemUsage.compute(queryId, (k, v) -> v == null ? new LongLongMutablePair(cpuTimeNs, allocatedBytes)
        : v.left(v.leftLong() + cpuTimeNs).right(v.rightLong() + allocatedBytes));
  }

  @Override
  public int getAggregationSleepTimeMs() {
    return _sleepTime;
  }

  @Override
  public void preAggregate(Iterable<ThreadResourceTrackerImpl> threadTrackers) {
    LOGGER.debug("Running pre-aggregate for QueryResourceAggregator.");
    QueryMonitorConfig config = _queryMonitorConfig.get();
    _sleepTime = config.getNormalSleepTime();
    _queryResourceUsages = null;
    _previousTriggeringLevel = _triggeringLevel;
    collectTriggerMetrics();
    evalTriggers();
    // Prioritize the panic check
    if (_triggeringLevel == TriggeringLevel.HeapMemoryPanic) {
      boolean pauseOnPanic = config.isOomPreQueryKillPauseEnabled() && config.isOomPanicPreQueryKillPauseEnabled();
      if (_pauseActive) {
        if (System.currentTimeMillis() >= _pauseDeadlineMs || !pauseOnPanic) {
          // Grace period expired, or panic-pause disabled — kill all and clear
          killAllQueries(threadTrackers);
          clearPause();
        }
        // else: still in grace period and panic-pause is enabled
      } else if (pauseOnPanic
          && _previousTriggeringLevel.ordinal() < TriggeringLevel.HeapMemoryCritical.ordinal()) {
        // Fresh jump to panic bypassing critical — activate pause before killing
        activatePause(config.getOomPreQueryKillPauseDurationMs());
      } else {
        // Pause-on-panic disabled, or already been through a pause cycle — kill immediately
        killAllQueries(threadTrackers);
      }
      return;
    }
    // If heap recovered below critical while an OOM pause is active, clear it
    if (_pauseActive && _triggeringLevel.ordinal() < TriggeringLevel.HeapMemoryCritical.ordinal()) {
      clearPause();
    }
    // Track aggregated query resource usage if triggered
    if (_triggeringLevel.ordinal() > TriggeringLevel.Normal.ordinal()) {
      _queryResourceUsages = new HashMap<>();
    }
  }

  @Override
  public void aggregate(ThreadResourceTrackerImpl threadTracker) {
    QueryThreadContext threadContext = threadTracker.getThreadContext();
    if (threadContext == null) {
      return;
    }
    QueryExecutionContext executionContext = threadContext.getExecutionContext();
    String queryId = executionContext.getCid();
    _inactiveQueries.remove(queryId);
    if (_queryResourceUsages == null) {
      return;
    }
    long cpuTimeNs = threadTracker.getCpuTimeNs();
    long allocatedBytes = threadTracker.getAllocatedBytes();
    _queryResourceUsages.compute(queryId,
        (k, v) -> v == null ? new QueryResourceTrackerImpl(executionContext, cpuTimeNs, allocatedBytes)
            : v.merge(cpuTimeNs, allocatedBytes));
  }

  @Override
  public void postAggregate() {
    if (_queryResourceUsages == null) {
      return;
    }
    for (Map.Entry<String, QueryResourceTrackerImpl> entry : _queryResourceUsages.entrySet()) {
      String activeQueryId = entry.getKey();
      LongLongMutablePair cpuMemUsage = _untrackedCpuMemUsage.get(activeQueryId);
      if (cpuMemUsage != null) {
        entry.getValue().merge(cpuMemUsage.leftLong(), cpuMemUsage.rightLong());
      }
    }
    switch (_triggeringLevel) {
      case HeapMemoryCritical:
        QueryMonitorConfig config = _queryMonitorConfig.get();
        if (_pauseActive) {
          // Pause is active - check if grace period expired
          if (System.currentTimeMillis() >= _pauseDeadlineMs) {
            killMostExpensiveQuery();
            clearPause();
          }
          // else: still in grace period, skip kill this cycle
        } else if (config.isOomPreQueryKillPauseEnabled()
            && _previousTriggeringLevel.ordinal() < TriggeringLevel.HeapMemoryCritical.ordinal()) {
          // Just transitioned to critical - activate pause instead of killing
          activatePause(config.getOomPreQueryKillPauseDurationMs());
        } else {
          // Pause disabled, or already been through a pause cycle - kill
          killMostExpensiveQuery();
        }
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

  private void collectTriggerMetrics() {
    _usedBytes = ResourceUsageUtils.getUsedHeapSize();
    LOGGER.debug("Heap used bytes {}", _usedBytes);
  }

  private void evalTriggers() {
    // Compute the new triggering level based on the current heap usage
    QueryMonitorConfig config = _queryMonitorConfig.get();
    if (_usedBytes > config.getPanicLevel()) {
      // PANIC
      _sleepTime = config.getAlarmingSleepTime();
      _triggeringLevel = TriggeringLevel.HeapMemoryPanic;
      _metrics.addMeteredGlobalValue(_heapMemoryPanicExceededMeter, 1);
    } else if (_usedBytes > config.getCriticalLevel()) {
      // CRITICAL
      _sleepTime = config.getAlarmingSleepTime();
      _triggeringLevel = TriggeringLevel.HeapMemoryCritical;
      _metrics.addMeteredGlobalValue(_heapMemoryCriticalExceededMeter, 1);
    } else if (_usedBytes > config.getAlarmingLevel()) {
      // ALARMING
      _sleepTime = config.getAlarmingSleepTime();
      if (config.isCpuTimeBasedKillingEnabled()) {
        _triggeringLevel = TriggeringLevel.CPUTimeBasedKilling;
      } else {
        if (LOGGER.isDebugEnabled()) {
          _triggeringLevel = TriggeringLevel.HeapMemoryAlarmingVerbose;
        } else {
          _triggeringLevel = TriggeringLevel.Normal;
        }
      }
    } else {
      // NORMAL
      if (config.isCpuTimeBasedKillingEnabled()) {
        _triggeringLevel = TriggeringLevel.CPUTimeBasedKilling;
      } else {
        _triggeringLevel = TriggeringLevel.Normal;
      }
    }

    // Log the triggering level change
    if (_previousTriggeringLevel != _triggeringLevel) {
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

  /**
   * Activates an OOM pause. Query threads calling {@link #waitIfPaused()} will block until the pause is cleared or the
   * deadline expires. Also hints the JVM to run garbage collection.
   */
  private void activatePause(long timeoutMs) {
    LOGGER.warn("Activating OOM pause for {}ms to allow garbage collection before killing queries. "
        + "Heap used bytes: {}", timeoutMs, _usedBytes);
    _pauseDeadlineMs = System.currentTimeMillis() + timeoutMs;
    _pauseActive = true;
    // Hint the JVM to run GC while query threads are pausing
    System.gc();
  }

  /**
   * Clears the OOM pause and wakes all blocked query threads.
   */
  @VisibleForTesting
  void clearPause() {
    LOGGER.info("Clearing OOM pause. Heap used bytes: {}", _usedBytes);
    _pauseLock.lock();
    try {
      _pauseActive = false;
      _pauseCondition.signalAll();
    } finally {
      _pauseLock.unlock();
    }
  }

  /**
   * Called by query threads at sampling checkpoints. Blocks if an OOM pause is active. Fast-path: single volatile read
   * when no pause is active.
   * @return {@code true} if the thread entered the slow path (i.e. a pause was active when called), {@code false} if
   *         the fast-path was taken and the thread did not block.
   */
  @VisibleForTesting
  boolean waitIfPaused() {
    if (!_pauseActive) {
      return false;
    }
    _pauseLock.lock();
    try {
      while (_pauseActive) {
        _pauseCondition.await();
      }
    } catch (InterruptedException e) {
      // Query was killed during pause - restore interrupt flag so the next checkTermination() call detects it
      Thread.currentThread().interrupt();
    } finally {
      _pauseLock.unlock();
    }
    return true;
  }

  private void killAllQueries(Iterable<ThreadResourceTrackerImpl> threadTrackers) {
    QueryMonitorConfig config = _queryMonitorConfig.get();
    if (!config.isOomKillQueryEnabled()) {
      LOGGER.error("Trying to kill all queries, not killing them because OOM kill is not enabled");
      return;
    }

    String errorMessage =
        "Panic OOM killed on " + _instanceType + ": " + _instanceId + " as heap used bytes: " + _usedBytes
            + " exceeds panic level: " + config.getPanicLevel();
    Set<String> killedQueries = new HashSet<>();
    for (ThreadResourceTrackerImpl threadTracker : threadTrackers) {
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
  private void killMostExpensiveQuery() {
    if (!_memorySamplingEnabled) {
      return;
    }
    if (!_queryResourceUsages.isEmpty()) {
      QueryResourceTrackerImpl maxUsageQueryTracker = _queryResourceUsages.values()
          .stream()
          .filter(tracker -> tracker.getExecutionContext().getTerminateException() == null)
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
          LOGGER.warn("Query: {} has most allocated bytes: {}, but below the minimum memory footprint for kill: {}, "
              + "skipping query kill", queryId, allocatedBytes, config.getMinMemoryFootprintForKill());
        }
      }
      logQueryResourceUsage(_queryResourceUsages);
    } else {
      LOGGER.debug("Critical level memory usage query killing - No active query to kill");
    }
  }

  private void killCPUTimeExceedQueries() {
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
          LOGGER.debug("Query {} got picked because using {} ns of cpu time, greater than threshold {}", queryId,
              cpuTimeNs, config.getCpuTimeBasedKillingThresholdNs());
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

  private void logQueryResourceUsage(Map<String, QueryResourceTrackerImpl> queryResourceUsages) {
    LOGGER.debug("Query resource usage: {} for the previous kill", queryResourceUsages);
  }

  private void logTerminatedQuery(QueryResourceTracker queryTracker, long totalHeapUsage) {
    LOGGER.warn("Query: {} terminated. CPU Usage (ns): {}. Memory Usage (bytes): {}. Total Heap Usage (bytes): {}",
        queryTracker.getExecutionContext().getCid(), queryTracker.getCpuTimeNs(), queryTracker.getAllocatedBytes(),
        totalHeapUsage);
  }

  @Override
  public void cleanUpPostAggregation() {
    LOGGER.debug(
        _queryResourceUsages == null ? "_aggregatedUsagePerActiveQuery : null" : _queryResourceUsages.toString());
    if (_queryMonitorConfig.get().isPublishHeapUsageMetric()) {
      _metrics.setValueOfGlobalGauge(_memoryUsageGauge, _usedBytes);
    }
    cleanInactive();
  }

  private void cleanInactive() {
    for (String inactiveQueryId : _inactiveQueries) {
      _untrackedCpuMemUsage.remove(inactiveQueryId);
    }
    _inactiveQueries.clear();
    _inactiveQueries.addAll(_untrackedCpuMemUsage.keySet());
  }

  @VisibleForTesting
  boolean isPauseActive() {
    return _pauseActive;
  }

  @VisibleForTesting
  TriggeringLevel getTriggeringLevel() {
    return _triggeringLevel;
  }

  @VisibleForTesting
  TriggeringLevel getPreviousTriggeringLevel() {
    return _previousTriggeringLevel;
  }

  public long getHeapUsageBytes() {
    return _usedBytes;
  }

  public Map<String, ? extends QueryResourceTracker> getQueryResources(
      Map<Thread, ThreadResourceTrackerImpl> threadTrackers) {
    Map<String, QueryResourceTrackerImpl> queryTrackers = new HashMap<>();

    for (ThreadResourceTrackerImpl threadTracker : threadTrackers.values()) {
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
}
