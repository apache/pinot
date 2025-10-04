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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface for managing workload budgets.
 */
public interface WorkloadBudgetManager {

  /**
   * Adds or updates budget for a workload (Thread-Safe).
   */
  void addOrUpdateWorkload(String workload, long cpuBudgetNs, long memoryBudgetBytes);

  /**
   * Deletes a workload and its associated budget (Thread-Safe).
   */
  void deleteWorkload(String workload);

  /**
   * Collects workload stats for CPU and memory usage.
   */
  void collectWorkloadStats(String workload, BudgetStats stats);

  /**
   * Gets the type name of the workload budget manager.
   */
  String getWorkloadTypeName();

  /**
   * Attempts to charge CPU and memory usage against the workload budget (Thread-Safe).
   * Returns the remaining budget for CPU and memory after charge.
   */
  BudgetStats tryCharge(String workload, long cpuUsedNs, long memoryUsedBytes);

  /**
   * Retrieves the budget stats for a specific workload
   * Returns null if the workload does not exist or if the manager is disabled.
   */
  BudgetStats getBudgetStats(String workload);

  Map<String, BudgetStats> getAllBudgetStats();

  void shutdown();

  /**
   * Determines whether a query for the given workload can be admitted under CPU-only budgets.
   *
   * <p>Admission rules:
   * <ol>
   *   <li>If the manager is disabled or no budget exists for the workload, always admit.</li>
   *   <li>If CPU budget remains above zero, admit immediately.</li>
   *   <li>Otherwise, reject (return false).</li>
   * </ol>
   *
   * <p>Note: This method currently uses a strict check, where CPU and memory budgets must be above zero.
   * This may be relaxed in the future to allow for a percentage of other remaining budget to be used. At that point,
   * we can have different admission policies like: Strict, Stealing, etc.
   *
   * @param workload the workload identifier to check budget for
   * @return true if the query may be accepted; false if budget is insufficient
   */
  boolean canAdmitQuery(String workload);

  /**
   * Internal class representing budget statistics.
   * It contains initial CPU and memory budgets that are configured during workload registration,
   * as well as the remaining CPU and memory budgets during runtime in an enforcement window.
   */
  class BudgetStats {
    public final long _initialCpuBudget;
    public final long _initialMemoryBudget;

    public final long _cpuRemaining;
    public final long _memoryRemaining;

    public BudgetStats(long cpuBudgetNs, long memoryBudgetBytes, long cpuRemaining, long memoryRemaining) {
      _initialCpuBudget = cpuBudgetNs;
      _initialMemoryBudget = memoryBudgetBytes;
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

      return new BudgetStats(_initialCpuBudget, _initialMemoryBudget, _cpuRemaining.get(), _memoryRemaining.get());
    }

    /**
     * Resets the budget back to its original limits.
     */
    public void reset() {
      _cpuRemaining.set(_initialCpuBudget);
      _memoryRemaining.set(_initialMemoryBudget);
    }

    /**
     * Gets the budget stats that provides initial and remaining CPU and memory budgets.
     */
    public BudgetStats getStats() {
      return new BudgetStats(_initialCpuBudget, _initialMemoryBudget, _cpuRemaining.get(), _memoryRemaining.get());
    }
  }
}
