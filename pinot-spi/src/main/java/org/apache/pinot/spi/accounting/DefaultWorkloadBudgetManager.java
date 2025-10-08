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
package org.apache.pinot.spi.accounting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.annotations.accounting.WorkloadBudgetManagerAnnotation;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@WorkloadBudgetManagerAnnotation
public class DefaultWorkloadBudgetManager implements WorkloadBudgetManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultWorkloadBudgetManager.class);

  private long _enforcementWindowMs;
  private ConcurrentHashMap<String, WorkloadBudgetManager.Budget> _workloadBudgets;
  private final ScheduledExecutorService _resetScheduler = Executors.newSingleThreadScheduledExecutor();
  private volatile boolean _isCostCollectionEnabled;
  private volatile boolean _isCostEmissionEnabled;

  public DefaultWorkloadBudgetManager(PinotConfiguration config) {
    _isCostCollectionEnabled = config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_ENABLE_COST_COLLECTION);
    // Return an object even if disabled. All functionalities of this class will be noops.
    if (!_isCostCollectionEnabled) {
      LOGGER.info("WorkloadBudgetManager is disabled. Creating a no-op instance.");
      return;
    }
    _isCostEmissionEnabled = config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_EMISSION,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_ENABLE_COST_EMISSION);
    _workloadBudgets = new ConcurrentHashMap<>();
    _enforcementWindowMs = config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENFORCEMENT_WINDOW_MS,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_ENFORCEMENT_WINDOW_MS);
    initSecondaryWorkloadBudget(config);
    startBudgetResetTask();
    LOGGER.info("WorkloadBudgetManager initialized with enforcement window: {}ms", _enforcementWindowMs);
  }

  /**
   * This budget is primarily meant to be used for queries that need to be issued in a low priority manner.
   * This is fixed budget allocated during host startup and used across all secondary queries.
   */
  private void initSecondaryWorkloadBudget(PinotConfiguration config) {
    double secondaryCpuPercentage = config.getProperty(
        CommonConstants.Accounting.CONFIG_OF_SECONDARY_WORKLOAD_CPU_PERCENTAGE,
        CommonConstants.Accounting.DEFAULT_SECONDARY_WORKLOAD_CPU_PERCENTAGE);

    // Don't create a secondary workload if cpu percentage is non-zero.
    if (secondaryCpuPercentage <= 0.0) {
      return;
    }

    String secondaryWorkloadName = config.getProperty(
        CommonConstants.Accounting.CONFIG_OF_SECONDARY_WORKLOAD_NAME,
        CommonConstants.Accounting.DEFAULT_SECONDARY_WORKLOAD_NAME);

    // The Secondary CPU budget is based on the CPU percentage allocated for secondary workload.
    // The memory budget is set to Long.MAX_VALUE for now, since we do not have a specific memory budget for
    // secondary queries.
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    // Total CPU capacity available in one enforcement window:
    // window(ms) × 1_000_000 (ns per ms) × number of logical processors
    long totalCpuCapacityNs = _enforcementWindowMs * 1_000_000L * availableProcessors;
    long secondaryCpuBudget = (long) (secondaryCpuPercentage * totalCpuCapacityNs);
    // TODO: Add memory budget for secondary workload queries
    addOrUpdateWorkload(secondaryWorkloadName, secondaryCpuBudget, Long.MAX_VALUE);
  }

  public void shutdown() {
    if (!_isCostCollectionEnabled) {
      return;
    }
    _isCostCollectionEnabled = false;
    _resetScheduler.shutdownNow();
    try {
      if (!_resetScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        LOGGER.warn("Reset scheduler did not terminate in time");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    LOGGER.info("WorkloadBudgetManager has been shut down.");
  }

  public void addOrUpdateWorkload(String workload, long cpuBudgetNs, long memoryBudgetBytes) {
    if (!_isCostCollectionEnabled) {
      LOGGER.info("WorkloadBudgetManager is disabled. Not adding/updating workload: {}", workload);
      return;
    }

    _workloadBudgets.compute(workload, (key, existingBudget) -> new Budget(cpuBudgetNs, memoryBudgetBytes));
    LOGGER.info("Updated budget for workload: {} -> CPU: {}ns, Memory: {} bytes", workload, cpuBudgetNs,
        memoryBudgetBytes);
  }

  public void deleteWorkload(String workload) {
    if (!_isCostCollectionEnabled) {
      LOGGER.info("WorkloadBudgetManager is disabled. Not deleting workload: {}", workload);
      return;
    }
    _workloadBudgets.remove(workload);
    LOGGER.info("Removed workload: {}", workload);
  }

  /**
   * Collects workload stats for CPU and memory usage.
   * Could be overridden for custom implementations
   */
  public void collectWorkloadStats(String workload, WorkloadBudgetManager.BudgetStats stats) {
    // Default implementation does nothing.
  }

  public String getWorkloadTypeName() {
    return CommonConstants.Accounting.DEFAULT_WORKLOAD_BUDGET_MANAGER_TYPE_NAME;
  }

  public BudgetStats tryCharge(String workload, long cpuUsedNs, long memoryUsedBytes) {
    if (!_isCostCollectionEnabled) {
      return new BudgetStats(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    Budget budget = _workloadBudgets.get(workload);
    if (budget == null) {
      LOGGER.warn("No budget found for workload: {}", workload);
      return new BudgetStats(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    }
    return budget.tryCharge(cpuUsedNs, memoryUsedBytes);
  }

  /**
   * Retrieves the initial and remaining budget for a workload.
   */
  public BudgetStats getBudgetStats(String workload) {
    if (!_isCostCollectionEnabled) {
      return null;
    }
    Budget budget = _workloadBudgets.get(workload);
    return budget != null ? budget.getStats() : null;
  }

  /**
   * Retrieves the total remaining budget across all workloads (Thread-Safe).
   */
  public BudgetStats getRemainingBudgetAcrossAllWorkloads() {
    if (!_isCostCollectionEnabled) {
      return new BudgetStats(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    }
    long totalCpuBudget =
        _workloadBudgets.values().stream().mapToLong(budget -> budget.getStats()._initialCpuBudget).sum();
    long totalMemoryBudget =
        _workloadBudgets.values().stream().mapToLong(budget -> budget.getStats()._initialMemoryBudget).sum();
    long totalCpuRemaining =
        _workloadBudgets.values().stream().mapToLong(budget -> budget.getStats()._cpuRemaining).sum();
    long totalMemRemaining =
        _workloadBudgets.values().stream().mapToLong(budget -> budget.getStats()._memoryRemaining).sum();
    return new BudgetStats(totalCpuBudget, totalMemoryBudget, totalCpuRemaining, totalMemRemaining);
  }

  /**
   * Periodically resets budgets at the end of each enforcement window (Thread-Safe).
   */
  private void startBudgetResetTask() {
    LOGGER.debug("Starting budget reset task with enforcement window: {}ms", _enforcementWindowMs);
    _resetScheduler.scheduleAtFixedRate(() -> {
      LOGGER.debug("Resetting all workload budgets.");
      // Also print the budget used in the last enforcement window.
      _workloadBudgets.forEach((workload, budget) -> {
        BudgetStats stats = budget.getStats();
        LOGGER.debug("Workload: {} -> CPU: {}ns, Memory: {} bytes", workload, stats._cpuRemaining,
            stats._memoryRemaining);
        if (_isCostEmissionEnabled) {
          collectWorkloadStats(workload, stats);
        }
        // Reset the budget.
        budget.reset();
      });
    }, _enforcementWindowMs, _enforcementWindowMs, TimeUnit.MILLISECONDS);
  }

  public boolean canAdmitQuery(String workload) {
    // If disabled or no budget configured, always admit
    if (!_isCostCollectionEnabled) {
      return true;
    }
    Budget budget = _workloadBudgets.get(workload);
    if (budget == null) {
      LOGGER.debug("No budget found for workload: {}", workload);
      return true;
    }
    BudgetStats stats = budget.getStats();
    return stats._cpuRemaining > 0 && stats._memoryRemaining > 0;
  }

  public Map<String, BudgetStats> getAllBudgetStats() {
    if (!_isCostCollectionEnabled) {
      return null;
    }
    Map<String, BudgetStats> allStats = new ConcurrentHashMap<>();
    _workloadBudgets.forEach((workload, budget) -> allStats.put(workload, budget.getStats()));
    return allStats;
  }
}
