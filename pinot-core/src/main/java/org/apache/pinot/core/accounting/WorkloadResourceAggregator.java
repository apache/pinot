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

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.longs.LongLongMutablePair;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WorkloadResourceAggregator implements ResourceAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadResourceAggregator.class);

  // Tracks the CPU and memory usage for workloads not tracked by any thread.
  // E.g. request ser/de where thread execution context cannot be set up beforehand; tasks already finished.
  private final ConcurrentHashMap<String, LongLongMutablePair> _untrackedCpuMemUsage = new ConcurrentHashMap<>();

  // Tracks the queries under each workload.
  private final Map<String, Set<QueryExecutionContext>> _workloadQueriesMap = new HashMap<>();

  private final String _instanceId;
  private final InstanceType _instanceType;
  private final boolean _cpuSamplingEnabled;
  private final boolean _memorySamplingEnabled;
  private final AtomicReference<QueryMonitorConfig> _queryMonitorConfig;
  private final WorkloadBudgetManager _workloadBudgetManager;

  // Tracks the CPU and memory usage for workloads.
  private Map<String, LongLongMutablePair> _previousCpuMemUsage = new HashMap<>();
  private Map<String, LongLongMutablePair> _currentCpuMemUsage = new HashMap<>();

  public WorkloadResourceAggregator(String instanceId, InstanceType instanceType, boolean cpuSamplingEnabled,
      boolean memorySamplingEnabled, AtomicReference<QueryMonitorConfig> queryMonitorConfig) {
    _instanceId = instanceId;
    _instanceType = instanceType;
    _cpuSamplingEnabled = cpuSamplingEnabled;
    _memorySamplingEnabled = memorySamplingEnabled;
    _queryMonitorConfig = queryMonitorConfig;
    _workloadBudgetManager = WorkloadBudgetManager.get();
  }

  @Override
  public void updateUntrackedResourceUsage(String workloadName, long cpuTimeNs, long allocatedBytes) {
    aggregateCpuMemUsage(_untrackedCpuMemUsage, workloadName, cpuTimeNs, allocatedBytes);
  }

  private void aggregateCpuMemUsage(Map<String, LongLongMutablePair> cpuMemUsageMap, String workloadName,
      long cpuTimeNs, long allocatedBytes) {
    cpuMemUsageMap.compute(workloadName, (k, v) -> v == null ? new LongLongMutablePair(cpuTimeNs, allocatedBytes)
        : v.left(v.leftLong() + cpuTimeNs).right(v.rightLong() + allocatedBytes));
  }

  @Override
  public int getAggregationSleepTimeMs() {
    return _queryMonitorConfig.get().getWorkloadSleepTimeMs();
  }

  @Override
  public void preAggregate(Iterable<ThreadResourceTrackerImpl> threadTrackers) {
  }

  @Override
  public void aggregate(ThreadResourceTrackerImpl threadTracker) {
    QueryThreadContext threadContext = threadTracker.getThreadContext();
    if (threadContext == null) {
      return;
    }
    QueryExecutionContext executionContext = threadContext.getExecutionContext();
    String workloadName = executionContext.getWorkloadName();
    _workloadQueriesMap.computeIfAbsent(workloadName, k -> Sets.newIdentityHashSet()).add(executionContext);
    aggregateCpuMemUsage(_currentCpuMemUsage, workloadName, threadTracker.getCpuTimeNs(),
        threadTracker.getAllocatedBytes());
  }

  @Override
  public void postAggregate() {
    for (Map.Entry<String, Set<QueryExecutionContext>> entry : _workloadQueriesMap.entrySet()) {
      String workloadName = entry.getKey();
      LongLongMutablePair untrackCpuMemUsage = _untrackedCpuMemUsage.get(workloadName);
      if (untrackCpuMemUsage != null) {
        aggregateCpuMemUsage(_currentCpuMemUsage, workloadName, untrackCpuMemUsage.leftLong(),
            untrackCpuMemUsage.rightLong());
      }
      LongLongMutablePair cpuMemUsage = _currentCpuMemUsage.get(workloadName);
      long cpuTimeNs = cpuMemUsage.leftLong();
      long allocatedBytes = cpuMemUsage.rightLong();
      LongLongMutablePair previousCpuMemUsage = _previousCpuMemUsage.get(workloadName);
      if (previousCpuMemUsage != null) {
        cpuTimeNs -= previousCpuMemUsage.leftLong();
        allocatedBytes -= previousCpuMemUsage.rightLong();
      }
      WorkloadBudgetManager.BudgetStats budgetStats =
          _workloadBudgetManager.tryCharge(workloadName, cpuTimeNs, allocatedBytes);
      LOGGER.debug("Workload: {}. Remaining budget CPU: {}, Memory: {}", workloadName, budgetStats._cpuRemaining,
          budgetStats._memoryRemaining);

      if (!_queryMonitorConfig.get().isWorkloadCostEnforcementEnabled()) {
        // Nothing to do.
        LOGGER.debug("Workload Cost Enforcement is disabled. Skipping enforcement for workload: {}", workloadName);
        continue;
      }

      if (budgetStats._cpuRemaining <= 0 || budgetStats._memoryRemaining <= 0) {
        // Kill all active queries in the workload
        String errorMessage =
            "Killed on " + _instanceType + ": " + _instanceId + " as workload cost exceeded. Remaining budget CPU: "
                + budgetStats._cpuRemaining + ", Memory: " + budgetStats._memoryRemaining;
        for (QueryExecutionContext executionContext : entry.getValue()) {
          executionContext.terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, errorMessage);
        }
      }
    }
  }

  @Override
  public void cleanUpPostAggregation() {
    _workloadQueriesMap.clear();
    // Add unactive workloads from previous sample to current sample
    for (Map.Entry<String, LongLongMutablePair> entry : _previousCpuMemUsage.entrySet()) {
      _currentCpuMemUsage.putIfAbsent(entry.getKey(), entry.getValue());
    }
    _previousCpuMemUsage = _currentCpuMemUsage;
    _currentCpuMemUsage = new HashMap<>();
  }
}
