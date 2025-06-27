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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WorkloadBudgetManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadBudgetManager.class);

  private long _enforcementWindowMs;
  private final ConcurrentHashMap<String, Budget> _workloadBudgets = new ConcurrentHashMap<>();
  private final ScheduledExecutorService _resetScheduler = Executors.newSingleThreadScheduledExecutor();
  private volatile boolean _isEnabled;

  public WorkloadBudgetManager(PinotConfiguration config) {
    _isEnabled = config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_ENABLE_COST_COLLECTION);
    // Return an object even if disabled. All functionalities of this class will be noops.
    if (!_isEnabled) {
      LOGGER.info("WorkloadBudgetManager is disabled. Creating a no-op instance.");
      return;
    }
    _enforcementWindowMs = config.getProperty(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENFORCEMENT_WINDOW_MS,
        CommonConstants.Accounting.DEFAULT_WORKLOAD_ENFORCEMENT_WINDOW_MS);
    startBudgetResetTask();
    LOGGER.info("WorkloadBudgetManager initialized with enforcement window: {}ms", _enforcementWindowMs);
  }

  public void shutdown() {
    if (!_isEnabled) {
      return;
    }
    _isEnabled = false;
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


  /**
   * Adds or updates budget for a workload (Thread-Safe).
   */
  public void addOrUpdateWorkload(String workload, long cpuBudgetNs, long memoryBudgetBytes) {
    if (!_isEnabled) {
      LOGGER.info("WorkloadBudgetManager is disabled. Not adding/updating workload: {}", workload);
      return;
    }

    _workloadBudgets.compute(workload, (key, existingBudget) -> new Budget(cpuBudgetNs, memoryBudgetBytes));
    LOGGER.info("Updated budget for workload: {} -> CPU: {}ns, Memory: {} bytes", workload, cpuBudgetNs,
        memoryBudgetBytes);
  }

  /**
   * Attempts to charge CPU and memory usage against the workload budget (Thread-Safe).
   * Returns the remaining budget for CPU and memory after charge.
   */
  public BudgetStats tryCharge(String workload, long cpuUsedNs, long memoryUsedBytes) {
    if (!_isEnabled) {
      return new BudgetStats(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    Budget budget = _workloadBudgets.get(workload);
    if (budget == null) {
      LOGGER.warn("No budget found for workload: {}", workload);
      return new BudgetStats(Long.MAX_VALUE, Long.MAX_VALUE);
    }
    return budget.tryCharge(cpuUsedNs, memoryUsedBytes);
  }

  /**
   * Retrieves the remaining budget for a specific workload.
   */
  public BudgetStats getRemainingBudgetForWorkload(String workload) {
    if (!_isEnabled) {
      return new BudgetStats(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    Budget budget = _workloadBudgets.get(workload);
    return budget != null ? budget.getStats() : new BudgetStats(0, 0);
  }

  /**
   * Retrieves the total remaining budget across all workloads (Thread-Safe).
   */
  public BudgetStats getRemainingBudgetAcrossAllWorkloads() {
    if (!_isEnabled) {
      return new BudgetStats(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    long totalCpuRemaining =
        _workloadBudgets.values().stream().mapToLong(budget -> budget.getStats()._cpuRemaining).sum();
    long totalMemRemaining =
        _workloadBudgets.values().stream().mapToLong(budget -> budget.getStats()._memoryRemaining).sum();
    return new BudgetStats(totalCpuRemaining, totalMemRemaining);
  }

  /**
   * Periodically resets budgets at the end of each enforcement window (Thread-Safe).
   */
  private void startBudgetResetTask() {
    // TODO(Vivek): Reduce logging verbosity. Maybe make it debug logs.
    LOGGER.info("Starting budget reset task with enforcement window: {}ms", _enforcementWindowMs);
    _resetScheduler.scheduleAtFixedRate(() -> {
      LOGGER.debug("Resetting all workload budgets.");
      // Also print the budget used in the last enforcement window.
      _workloadBudgets.forEach((workload, budget) -> {
        BudgetStats stats = budget.getStats();
        LOGGER.debug("Workload: {} -> CPU: {}ns, Memory: {} bytes", workload, stats._cpuRemaining,
            stats._memoryRemaining);
        // Reset the budget.
        budget.reset();
      });
    }, _enforcementWindowMs, _enforcementWindowMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Represents remaining budget stats.
   */
  public static class BudgetStats {
    public final long _cpuRemaining;
    public final long _memoryRemaining;

    public BudgetStats(long cpuRemaining, long memoryRemaining) {
      _cpuRemaining = cpuRemaining;
      _memoryRemaining = memoryRemaining;
    }
  }

  /**
   * Internal class representing a budget with CPU and memory constraints.
   */
  public class Budget {
    private final long _initialCpuBudget;
    private final long _initialMemoryBudget;

    private final AtomicLong _cpuRemaining;
    private final AtomicLong _memoryRemaining;

    public Budget(long cpuBudgetNs, long memoryBudgetBytes) {
      _initialCpuBudget = cpuBudgetNs;
      _initialMemoryBudget = memoryBudgetBytes;
      _cpuRemaining = new AtomicLong(cpuBudgetNs);
      _memoryRemaining = new AtomicLong(memoryBudgetBytes);
    }

    /**
     * Attempts to charge CPU and memory usage independently.
     * This method is not atomic across CPU and memory.
     * If either budget is insufficient, the caller is expected to cancel the query.
     */
    public BudgetStats tryCharge(long cpuUsedNs, long memoryUsedBytes) {
      // Charge the budget. It is possible that memory or CPU remaining goes negative.
      _memoryRemaining.addAndGet(-memoryUsedBytes);
      _cpuRemaining.addAndGet(-cpuUsedNs);

      return new BudgetStats(_cpuRemaining.get(), _memoryRemaining.get());
    }

    /**
     * Resets the budget back to its original limits.
     */
    public void reset() {
      _cpuRemaining.set(_initialCpuBudget);
      _memoryRemaining.set(_initialMemoryBudget);
    }

    /**
     * Gets the current remaining budget.
     */
    public BudgetStats getStats() {
      return new BudgetStats(_cpuRemaining.get(), _memoryRemaining.get());
    }
  }
}
