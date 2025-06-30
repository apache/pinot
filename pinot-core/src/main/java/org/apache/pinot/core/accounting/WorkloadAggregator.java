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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WorkloadAggregator implements ResourceAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadAggregator.class);

  private final boolean _isThreadCPUSamplingEnabled;
  private final boolean _isThreadMemorySamplingEnabled;
  private final PinotConfiguration _config;
  private final InstanceType _instanceType;
  private final String _instanceId;

  // For one time concurrent update of stats. This is to provide stats collection for parts that are not
  // performance sensitive and workload_name is not known earlier (eg: broker inbound netty request)
  private final ConcurrentHashMap<String, Long> _concurrentTaskCPUStatsAggregator = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> _concurrentTaskMemStatsAggregator = new ConcurrentHashMap<>();

  private final int _sleepTimeMs;
  private final boolean _enableEnforcement;

  WorkloadBudgetManager _workloadBudgetManager;

  // Aggregation state
  Map<String, List<CPUMemThreadLevelAccountingObjects.ThreadEntry>> _allWorkloads = new HashMap<>();
  HashMap<String, Long> _finishedTaskCPUCost = new HashMap<>();
  HashMap<String, Long> _finishedTaskMemCost = new HashMap<>();

  public WorkloadAggregator(boolean isThreadCPUSamplingEnabled, boolean isThreadMemSamplingEnabled,
      PinotConfiguration config, InstanceType instanceType, String instanceId) {
    _isThreadCPUSamplingEnabled = isThreadCPUSamplingEnabled;
    _isThreadMemorySamplingEnabled = isThreadMemSamplingEnabled;
    _config = config;
    _instanceType = instanceType;
    _instanceId = instanceId;

    _workloadBudgetManager = Tracing.ThreadAccountantOps.getWorkloadBudgetManager();
    _sleepTimeMs = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_SLEEP_TIME_MS,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_SLEEP_TIME_MS);
    _enableEnforcement = _config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_ENABLE_COST_ENFORCEMENT);

    LOGGER.info("WorkloadAggregator initialized with isThreadCPUSamplingEnabled: {}, isThreadMemorySamplingEnabled: {}",
        _isThreadCPUSamplingEnabled, _isThreadMemorySamplingEnabled);
  }

  @Override
  public void updateConcurrentCpuUsage(String workload, long cpuUsageNS) {
    _concurrentTaskCPUStatsAggregator.compute(workload,
        (key, value) -> (value == null) ? cpuUsageNS : (value + cpuUsageNS));
  }

  @Override
  public void updateConcurrentMemUsage(String workload, long memBytes) {
    _concurrentTaskMemStatsAggregator.compute(workload,
        (key, value) -> (value == null) ? memBytes : (value + memBytes));
  }

  @Override
  public void cleanUpPostAggregation() {
    // No-op.
  }

  @Override
  public int getAggregationSleepTimeMs() {
    return _sleepTimeMs;
  }

  @Override
  public void preAggregate(List<CPUMemThreadLevelAccountingObjects.ThreadEntry> anchorThreadEntries) {
    _allWorkloads.clear();
    _finishedTaskCPUCost.clear();
    _finishedTaskMemCost.clear();
  }

  @Override
  public void aggregate(Thread thread, CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry) {
    CPUMemThreadLevelAccountingObjects.TaskEntry currentQueryTask = threadEntry.getCurrentThreadTaskStatus();
    if (currentQueryTask == null) {
      // This means that task has finished or the task doesn't have a workload.
      return;
    }

    String currentTaskWorkload = currentQueryTask.getWorkloadName();
    long currentCPUCost = threadEntry._currentThreadCPUTimeSampleMS;
    long currentMemoryCost = threadEntry._currentThreadMemoryAllocationSampleBytes;

    CPUMemThreadLevelAccountingObjects.TaskEntry prevQueryTask = threadEntry._previousThreadTaskStatus;

    // The cost to charge against the workloadBudgetManager.
    long deltaCPUCost = currentCPUCost;
    long deltaMemCost = currentMemoryCost;

    if (prevQueryTask != null) {
      if (currentQueryTask == prevQueryTask) {
        deltaCPUCost = currentCPUCost - threadEntry._previousThreadCPUTimeSampleMS;
        deltaMemCost = currentMemoryCost - threadEntry._previousThreadMemoryAllocationSampleBytes;
      }
    }

    _finishedTaskCPUCost.merge(currentTaskWorkload, deltaCPUCost, Long::sum);
    _finishedTaskMemCost.merge(currentTaskWorkload, deltaMemCost, Long::sum);


    // Only add the anchor threads. These will be used to interrupt the thread.
    if (currentQueryTask.isAnchorThread()) {
      if (!_allWorkloads.containsKey(currentTaskWorkload)) {
        _allWorkloads.put(currentTaskWorkload, new ArrayList<>());
      }
      _allWorkloads.get(currentTaskWorkload).add(threadEntry);
    }
  }

  @Override
  public void postAggregate() {
    for (Map.Entry<String, List<CPUMemThreadLevelAccountingObjects.ThreadEntry>>
        workloadEntry : _allWorkloads.entrySet()) {
      String workloadName = workloadEntry.getKey();
      List<CPUMemThreadLevelAccountingObjects.ThreadEntry> threadEntries = workloadEntry.getValue();
      long finishedCPUCost = _finishedTaskCPUCost.getOrDefault(workloadName, 0L);
      long finishedMemCost = _finishedTaskMemCost.getOrDefault(workloadName, 0L);

      WorkloadBudgetManager.BudgetStats budgetStats =
          _workloadBudgetManager.tryCharge(workloadName, finishedCPUCost, finishedMemCost);
      LOGGER.debug("Workload: {}. Remaining budget CPU: {}, Memory: {}", workloadName, budgetStats._cpuRemaining,
          budgetStats._memoryRemaining);

      if (!_enableEnforcement) {
        // Nothing to do.
        LOGGER.debug("Workload Cost Enforcement is disabled. Skipping enforcement for workload: {}", workloadName);
        continue;
      }

      if (budgetStats._cpuRemaining <= 0 || budgetStats._memoryRemaining <= 0) {
        // Interrupt all queries in the list
        for (CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry : threadEntries) {
          String queryId = threadEntry.getQueryId();
          CPUMemThreadLevelAccountingObjects.TaskEntry taskEntry = threadEntry.getCurrentThreadTaskStatus();
          Thread anchorThread = taskEntry.getAnchorThread();
          if (!anchorThread.isInterrupted()) {
            LOGGER.info("Killing query: {} and anchorThread:{}, Remaining budget CPU: {}, Memory: {}", queryId,
                anchorThread.getName(), budgetStats._cpuRemaining, budgetStats._memoryRemaining);
            String expMsg = String.format("Query %s on instance %s (type: %s) killed. Workload Cost exceeded.", queryId,
                _instanceId, _instanceType);
            threadEntry._errorStatus.set(new RuntimeException(expMsg));
            anchorThread.interrupt();
          }
        }
      }
    }
  }
}
