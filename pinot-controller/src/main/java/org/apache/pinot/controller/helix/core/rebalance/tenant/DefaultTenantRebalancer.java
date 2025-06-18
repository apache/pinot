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
package org.apache.pinot.controller.helix.core.rebalance.tenant;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.exception.RebalanceInProgressException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSummaryResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultTenantRebalancer implements TenantRebalancer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTenantRebalancer.class);
  private final TableRebalanceManager _tableRebalanceManager;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ExecutorService _executorService;

  public DefaultTenantRebalancer(TableRebalanceManager tableRebalanceManager,
      PinotHelixResourceManager pinotHelixResourceManager, ExecutorService executorService) {
    _tableRebalanceManager = tableRebalanceManager;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _executorService = executorService;
  }

  @Override
  public TenantRebalanceResult rebalance(TenantRebalanceConfig config) {
    if (!config.getParallelWhitelist().isEmpty() || !config.getParallelBlacklist().isEmpty()) {
      // If the parallel whitelist or blacklist is set, the old tenant rebalance logic will be used
      // TODO: Deprecate the support for this in the future
      LOGGER.info("Using the old tenant rebalance logic because parallel whitelist or blacklist is set.");
      return rebalanceWithParallelAndSequential(config);
    }
    return rebalanceWithIncludeExcludeTables(config);
  }

  private TenantRebalanceResult rebalanceWithIncludeExcludeTables(TenantRebalanceConfig config) {
    Map<String, RebalanceResult> dryRunResults = new HashMap<>();
    Set<String> tables = getTenantTables(config.getTenantName());
    Set<String> includeTables = config.getIncludeTables();
    if (!includeTables.isEmpty()) {
      tables.retainAll(includeTables);
    }
    tables.removeAll(config.getExcludeTables());
    tables.forEach(table -> {
      try {
        RebalanceConfig rebalanceConfig = RebalanceConfig.copy(config);
        rebalanceConfig.setDryRun(true);
        dryRunResults.put(table,
            _tableRebalanceManager.rebalanceTable(table, rebalanceConfig, createUniqueRebalanceJobIdentifier(), false,
                false));
      } catch (TableNotFoundException | RebalanceInProgressException exception) {
        dryRunResults.put(table, new RebalanceResult(null, RebalanceResult.Status.FAILED, exception.getMessage(),
            null, null, null, null, null));
      }
    });

    if (config.isDryRun()) {
      return new TenantRebalanceResult(null, dryRunResults, config.isVerboseResult());
    }

    String tenantRebalanceJobId = createUniqueRebalanceJobIdentifier();
    TenantRebalanceObserver observer = new ZkBasedTenantRebalanceObserver(tenantRebalanceJobId, config.getTenantName(),
        tables, _pinotHelixResourceManager);
    observer.onTrigger(TenantRebalanceObserver.Trigger.START_TRIGGER, null, null);
    ConcurrentLinkedQueue<String> parallelQueue = createTableQueue(config, dryRunResults);
    // ensure atleast 1 thread is created to run the sequential table rebalance operations
    int parallelism = Math.max(config.getDegreeOfParallelism(), 1);
    try {
      for (int i = 0; i < parallelism; i++) {
        _executorService.submit(() -> {
          while (true) {
            String table = parallelQueue.poll();
            if (table == null) {
              break;
            }
            RebalanceConfig rebalanceConfig = RebalanceConfig.copy(config);
            rebalanceConfig.setDryRun(false);
            if (dryRunResults.get(table)
                .getRebalanceSummaryResult()
                .getSegmentInfo()
                .getReplicationFactor()
                .getExpectedValueAfterRebalance() == 1) {
              rebalanceConfig.setMinAvailableReplicas(0);
            }
            rebalanceTable(table, rebalanceConfig, dryRunResults.get(table).getJobId(), observer);
          }
          observer.onSuccess(String.format("Successfully rebalanced tenant %s.", config.getTenantName()));
        });
      }
    } catch (Exception exception) {
      observer.onError(String.format("Failed to rebalance the tenant %s. Cause: %s", config.getTenantName(),
          exception.getMessage()));
    }

    // Prepare tenant rebalance result to return
    Map<String, RebalanceResult> rebalanceResults = new HashMap<>();
    for (String table : dryRunResults.keySet()) {
      RebalanceResult result = dryRunResults.get(table);
      if (result.getStatus() == RebalanceResult.Status.DONE) {
        rebalanceResults.put(table, new RebalanceResult(result.getJobId(), RebalanceResult.Status.IN_PROGRESS,
            "In progress, check controller task status for the", result.getInstanceAssignment(),
            result.getTierInstanceAssignment(), result.getSegmentAssignment(), result.getPreChecksResult(),
            result.getRebalanceSummaryResult()));
      } else {
        rebalanceResults.put(table, dryRunResults.get(table));
      }
    }
    return new TenantRebalanceResult(tenantRebalanceJobId, rebalanceResults, config.isVerboseResult());
  }

  // This method implements the old logic for tenant rebalance using parallel whitelist/blacklist.
  // Usage of this method would likely be deprecated and not supported in the future.
  private TenantRebalanceResult rebalanceWithParallelAndSequential(TenantRebalanceConfig config) {
    Map<String, RebalanceResult> rebalanceResult = new HashMap<>();
    Set<String> tables = getTenantTables(config.getTenantName());
    tables.forEach(table -> {
      try {
        RebalanceConfig rebalanceConfig = RebalanceConfig.copy(config);
        rebalanceConfig.setDryRun(true);
        rebalanceResult.put(table,
            _tableRebalanceManager.rebalanceTable(table, rebalanceConfig, createUniqueRebalanceJobIdentifier(), false,
                false));
      } catch (TableNotFoundException | RebalanceInProgressException exception) {
        rebalanceResult.put(table, new RebalanceResult(null, RebalanceResult.Status.FAILED, exception.getMessage(),
            null, null, null, null, null));
      }
    });
    if (config.isDryRun()) {
      return new TenantRebalanceResult(null, rebalanceResult, config.isVerboseResult());
    } else {
      for (String table : rebalanceResult.keySet()) {
        RebalanceResult result = rebalanceResult.get(table);
        if (result.getStatus() == RebalanceResult.Status.DONE) {
          rebalanceResult.put(table, new RebalanceResult(result.getJobId(), RebalanceResult.Status.IN_PROGRESS,
              "In progress, check controller task status for the", result.getInstanceAssignment(),
              result.getTierInstanceAssignment(), result.getSegmentAssignment(), result.getPreChecksResult(),
              result.getRebalanceSummaryResult()));
        }
      }
    }

    String tenantRebalanceJobId = createUniqueRebalanceJobIdentifier();
    TenantRebalanceObserver observer = new ZkBasedTenantRebalanceObserver(tenantRebalanceJobId, config.getTenantName(),
        tables, _pinotHelixResourceManager);
    observer.onTrigger(TenantRebalanceObserver.Trigger.START_TRIGGER, null, null);
    final Deque<String> sequentialQueue = new LinkedList<>();
    final Deque<String> parallelQueue = new ConcurrentLinkedDeque<>();
    // ensure atleast 1 thread is created to run the sequential table rebalance operations
    int parallelism = Math.max(config.getDegreeOfParallelism(), 1);
    Set<String> dimTables = getDimensionalTables(config.getTenantName());
    AtomicInteger activeThreads = new AtomicInteger(parallelism);
    try {
      if (parallelism > 1) {
        Set<String> parallelTables;
        if (!config.getParallelWhitelist().isEmpty()) {
          parallelTables = new HashSet<>(config.getParallelWhitelist());
        } else {
          parallelTables = new HashSet<>(tables);
        }
        if (!config.getParallelBlacklist().isEmpty()) {
          parallelTables = Sets.difference(parallelTables, config.getParallelBlacklist());
        }
        parallelTables.forEach(table -> {
          if (dimTables.contains(table)) {
            // prioritise dimension tables
            parallelQueue.addFirst(table);
          } else {
            parallelQueue.addLast(table);
          }
        });
        Sets.difference(tables, parallelTables).forEach(table -> {
          if (dimTables.contains(table)) {
            // prioritise dimension tables
            sequentialQueue.addFirst(table);
          } else {
            sequentialQueue.addLast(table);
          }
        });
      } else {
        tables.forEach(table -> {
          if (dimTables.contains(table)) {
            // prioritise dimension tables
            sequentialQueue.addFirst(table);
          } else {
            sequentialQueue.addLast(table);
          }
        });
      }

      for (int i = 0; i < parallelism; i++) {
        _executorService.submit(() -> {
          while (true) {
            String table = parallelQueue.pollFirst();
            if (table == null) {
              break;
            }
            RebalanceConfig rebalanceConfig = RebalanceConfig.copy(config);
            rebalanceConfig.setDryRun(false);
            rebalanceTable(table, rebalanceConfig, rebalanceResult.get(table).getJobId(), observer);
          }
          // Last parallel thread to finish the table rebalance job will pick up the
          // sequential table rebalance execution
          if (activeThreads.decrementAndGet() == 0) {
            RebalanceConfig rebalanceConfig = RebalanceConfig.copy(config);
            rebalanceConfig.setDryRun(false);
            while (true) {
              String table = sequentialQueue.pollFirst();
              if (table == null) {
                break;
              }
              rebalanceTable(table, rebalanceConfig, rebalanceResult.get(table).getJobId(), observer);
            }
            observer.onSuccess(String.format("Successfully rebalanced tenant %s.", config.getTenantName()));
          }
        });
      }
    } catch (Exception exception) {
      observer.onError(String.format("Failed to rebalance the tenant %s. Cause: %s", config.getTenantName(),
          exception.getMessage()));
    }
    return new TenantRebalanceResult(tenantRebalanceJobId, rebalanceResult, config.isVerboseResult());
  }

  private Set<String> getDimensionalTables(String tenantName) {
    Set<String> dimTables = new HashSet<>();
    for (String table : _pinotHelixResourceManager.getAllTables()) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(table);
      if (tableConfig == null) {
        LOGGER.error("Unable to retrieve table config for table: {}", table);
        continue;
      }
      if (tenantName.equals(tableConfig.getTenantConfig().getServer()) && tableConfig.isDimTable()) {
        dimTables.add(table);
      }
    }
    return dimTables;
  }

  private String createUniqueRebalanceJobIdentifier() {
    return UUID.randomUUID().toString();
  }

  Set<String> getTenantTables(String tenantName) {
    Set<String> tables = new HashSet<>();
    for (String table : _pinotHelixResourceManager.getAllTables()) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(table);
      if (tableConfig == null) {
        LOGGER.error("Unable to retrieve table config for table: {}", table);
        continue;
      }
      Set<String> relevantTags = TableConfigUtils.getRelevantTags(tableConfig);
      if (relevantTags.contains(TagNameUtils.getServerTagForTenant(tenantName, tableConfig.getTableType()))) {
        tables.add(table);
      }
    }
    return tables;
  }

  private void rebalanceTable(String tableName, RebalanceConfig config, String rebalanceJobId,
      TenantRebalanceObserver observer) {
    try {
      observer.onTrigger(TenantRebalanceObserver.Trigger.REBALANCE_STARTED_TRIGGER, tableName, rebalanceJobId);
      RebalanceResult result = _tableRebalanceManager.rebalanceTable(tableName, config, rebalanceJobId, true, true);
      // TODO: For downtime=true rebalance, track if the EV-IS has converged to move on, otherwise it fundementally
      //  breaks the degree of parallelism
      if (result.getStatus().equals(RebalanceResult.Status.DONE)) {
        observer.onTrigger(TenantRebalanceObserver.Trigger.REBALANCE_COMPLETED_TRIGGER, tableName, null);
      } else {
        observer.onTrigger(TenantRebalanceObserver.Trigger.REBALANCE_ERRORED_TRIGGER, tableName,
            result.getDescription());
      }
    } catch (Throwable t) {
      observer.onTrigger(TenantRebalanceObserver.Trigger.REBALANCE_ERRORED_TRIGGER, tableName,
          String.format("Caught exception/error while rebalancing table: %s", tableName));
    }
  }

  @VisibleForTesting
  ConcurrentLinkedQueue<String> createTableQueue(TenantRebalanceConfig config,
      Map<String, RebalanceResult> dryRunResults) {
    Queue<String> firstQueue = new LinkedList<>();
    Queue<String> queue = new LinkedList<>();
    Queue<String> lastQueue = new LinkedList<>();
    Set<String> dimTables = getDimensionalTables(config.getTenantName());
    dryRunResults.forEach((table, dryRynResult) -> {
      if (dimTables.contains(table)) {
        // check if the dimension table is a pure scale out or scale in.
        // pure scale out means that only new servers are added and no servers are removed, vice versa
        RebalanceSummaryResult.ServerInfo serverInfo =
            dryRunResults.get(table).getRebalanceSummaryResult().getServerInfo();
        if (!serverInfo.getServersAdded().isEmpty() && serverInfo.getServersRemoved().isEmpty()) {
          // dimension table's pure scale OUT should be performed BEFORE other regular tables so that queries involving
          // joining with dimension table won't fail on the new servers
          firstQueue.add(table);
        } else if (serverInfo.getServersAdded().isEmpty() && !serverInfo.getServersRemoved().isEmpty()) {
          // dimension table's pure scale IN should be performed AFTER other regular tables so that queries involving
          // joining with dimension table won't fail on the old servers
          lastQueue.add(table);
        } else {
          // the dimension table is not a pure scale out or scale in, which is supposed to be rebalanced manually.
          // Pre-check should capture and warn about this case.
          firstQueue.add(table);
        }
      } else {
        queue.add(table);
      }
    });
    ConcurrentLinkedQueue<String> tableQueue = new ConcurrentLinkedQueue<>();
    tableQueue.addAll(firstQueue);
    tableQueue.addAll(queue);
    tableQueue.addAll(lastQueue);
    return tableQueue;
  }
}
