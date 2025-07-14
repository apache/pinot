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

import java.util.concurrent.atomic.AtomicLong;

public interface WorkloadBudgetManager {
  /** Shut the manager down and release resources. */
  void shutdown();

  /**
   * Add a new workload or update an existing workload’s budgets.
   *
   * @param workload           Logical workload identifier
   * @param cpuBudgetNs        CPU budget for the enforcement window, in ns
   * @param memoryBudgetBytes  Memory budget for the window, in bytes
   */
  void addOrUpdateWorkload(String workload, long cpuBudgetNs, long memoryBudgetBytes);

  /**
   * Charge the given resource usage against the workload’s remaining budget.
   *
   * @return A snapshot of the remaining CPU / memory after the charge.
   */
  WorkloadBudgetManager.BudgetStats tryCharge(String workload, long cpuUsedNs, long memoryUsedBytes);

  /**
   * @return Remaining budget for the given workload.
   */
  WorkloadBudgetManager.BudgetStats getRemainingBudgetForWorkload(String workload);

  /**
   * @return Aggregate remaining budget across **all** workloads.
   */
  WorkloadBudgetManager.BudgetStats getRemainingBudgetAcrossAllWorkloads();

  void collectWorkloadStats(String workload, long cpuUsedNs, long memoryUsedBytes);

  /**
   * Represents remaining budget stats.
   */
  class BudgetStats {
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
  class Budget {
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
