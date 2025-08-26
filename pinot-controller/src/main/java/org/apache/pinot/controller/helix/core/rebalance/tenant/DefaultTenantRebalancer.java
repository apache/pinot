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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSummaryResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceManager;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
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
    Map<String, RebalanceResult> dryRunResults = new HashMap<>();

    // Step 1: Select the tables to include in this rebalance operation

    Set<String> tables = getTenantTables(config.getTenantName());
    Set<String> includeTables = config.getIncludeTables();
    if (!includeTables.isEmpty()) {
      tables.retainAll(includeTables);
    }
    tables.removeAll(config.getExcludeTables());

    // Step 2: Dry-run over the selected tables to get the dry-run rebalance results. The result is to be sent as
    // response to the user, and their summaries are needed for scheduling the job queue later

    tables.forEach(table -> {
      try {
        RebalanceConfig rebalanceConfig = RebalanceConfig.copy(config);
        rebalanceConfig.setDryRun(true);
        dryRunResults.put(table,
            _tableRebalanceManager.rebalanceTableDryRun(table, rebalanceConfig, createUniqueRebalanceJobIdentifier()));
      } catch (TableNotFoundException exception) {
        dryRunResults.put(table, new RebalanceResult(null, RebalanceResult.Status.FAILED, exception.getMessage(),
            null, null, null, null, null));
      }
    });

    // If dry-run was set, return the dry-run results and the job is done here
    if (config.isDryRun()) {
      return new TenantRebalanceResult(null, dryRunResults, config.isVerboseResult());
    }

    // Step 3: Create two queues--parallel and sequential and schedule the tables to these queues based on the
    // parallelWhitelist and parallelBlacklist, also their dry-run results. For each table, a job context is created
    // and put in the queue for the consuming threads to pick up and run the rebalance operation

    String tenantRebalanceJobId = createUniqueRebalanceJobIdentifier();
    TenantRebalanceObserver observer = new ZkBasedTenantRebalanceObserver(tenantRebalanceJobId, config.getTenantName(),
        tables, _pinotHelixResourceManager);
    observer.onTrigger(TenantRebalanceObserver.Trigger.START_TRIGGER, null, null);
    Pair<ConcurrentLinkedQueue<TenantTableRebalanceJobContext>, Queue<TenantTableRebalanceJobContext>> queues =
        createParallelAndSequentialQueues(config, dryRunResults, config.getParallelWhitelist(),
            config.getParallelBlacklist());
    ConcurrentLinkedQueue<TenantTableRebalanceJobContext> parallelQueue = queues.getLeft();
    Queue<TenantTableRebalanceJobContext> sequentialQueue = queues.getRight();

    // Step 4: Spin up threads to consume the parallel queue and sequential queue.

    // ensure atleast 1 thread is created to run the sequential table rebalance operations
    int parallelism = Math.max(config.getDegreeOfParallelism(), 1);
    AtomicInteger activeThreads = new AtomicInteger(parallelism);
    try {
      for (int i = 0; i < parallelism; i++) {
        _executorService.submit(() -> {
          doConsumeTablesFromQueue(parallelQueue, config, observer);
          if (activeThreads.decrementAndGet() == 0) {
            doConsumeTablesFromQueue(sequentialQueue, config, observer);
            observer.onSuccess(String.format("Successfully rebalanced tenant %s.", config.getTenantName()));
          }
        });
      }
    } catch (Exception exception) {
      observer.onError(String.format("Failed to rebalance the tenant %s. Cause: %s", config.getTenantName(),
          exception.getMessage()));
    }

    // Step 5: Prepare the rebalance results to be returned to the user. The rebalance jobs are running in the
    // background asynchronously.

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

  private void doConsumeTablesFromQueue(Queue<TenantTableRebalanceJobContext> queue, RebalanceConfig config,
      TenantRebalanceObserver observer) {
    while (true) {
      TenantTableRebalanceJobContext jobContext = queue.poll();
      if (jobContext == null) {
        break;
      }
      String table = jobContext.getTableName();
      RebalanceConfig rebalanceConfig = RebalanceConfig.copy(config);
      rebalanceConfig.setDryRun(false);
      if (jobContext.shouldRebalanceWithDowntime()) {
        rebalanceConfig.setMinAvailableReplicas(0);
      }
      rebalanceTable(table, rebalanceConfig, jobContext.getJobId(), observer);
    }
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
      if (TableConfigUtils.isRelevantToTenant(tableConfig, tenantName)) {
        tables.add(table);
      }
    }
    return tables;
  }

  private void rebalanceTable(String tableName, RebalanceConfig config, String rebalanceJobId,
      TenantRebalanceObserver observer) {
    try {
      observer.onTrigger(TenantRebalanceObserver.Trigger.REBALANCE_STARTED_TRIGGER, tableName, rebalanceJobId);
      RebalanceResult result = _tableRebalanceManager.rebalanceTable(tableName, config, rebalanceJobId, true);
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

  private static Set<String> getTablesToRunInParallel(Set<String> tables,
      @Nullable Set<String> parallelWhitelist, @Nullable Set<String> parallelBlacklist) {
    Set<String> parallelTables = new HashSet<>(tables);
    if (parallelWhitelist != null && !parallelWhitelist.isEmpty()) {
      parallelTables.retainAll(parallelWhitelist);
    }
    if (parallelBlacklist != null && !parallelBlacklist.isEmpty()) {
      parallelTables.removeAll(parallelBlacklist);
    }
    return parallelTables;
  }

  @VisibleForTesting
  Pair<ConcurrentLinkedQueue<TenantTableRebalanceJobContext>, Queue<TenantTableRebalanceJobContext>>
  createParallelAndSequentialQueues(
      TenantRebalanceConfig config, Map<String, RebalanceResult> dryRunResults, @Nullable Set<String> parallelWhitelist,
      @Nullable Set<String> parallelBlacklist) {
    Set<String> parallelTables = getTablesToRunInParallel(dryRunResults.keySet(), parallelWhitelist, parallelBlacklist);
    Map<String, RebalanceResult> parallelTableDryRunResults = new HashMap<>();
    Map<String, RebalanceResult> sequentialTableDryRunResults = new HashMap<>();
    dryRunResults.forEach((table, result) -> {
      if (parallelTables.contains(table)) {
        parallelTableDryRunResults.put(table, result);
      } else {
        sequentialTableDryRunResults.put(table, result);
      }
    });
    ConcurrentLinkedQueue<TenantTableRebalanceJobContext> parallelQueue =
        createTableQueue(config, parallelTableDryRunResults);
    Queue<TenantTableRebalanceJobContext> sequentialQueue = createTableQueue(config, sequentialTableDryRunResults);
    return Pair.of(parallelQueue, sequentialQueue);
  }

  @VisibleForTesting
  ConcurrentLinkedQueue<TenantTableRebalanceJobContext> createTableQueue(TenantRebalanceConfig config,
      Map<String, RebalanceResult> dryRunResults) {
    Queue<TenantTableRebalanceJobContext> firstQueue = new LinkedList<>();
    Queue<TenantTableRebalanceJobContext> queue = new LinkedList<>();
    Queue<TenantTableRebalanceJobContext> lastQueue = new LinkedList<>();
    Set<String> dimTables = getDimensionalTables(config.getTenantName());
    dryRunResults.forEach((table, dryRunResult) -> {
      TenantTableRebalanceJobContext jobContext =
          new TenantTableRebalanceJobContext(table, dryRunResult.getJobId(), dryRunResult.getRebalanceSummaryResult()
              .getSegmentInfo()
              .getReplicationFactor()
              .getExpectedValueAfterRebalance() == 1);
      if (dimTables.contains(table)) {
        // check if the dimension table is a pure scale out or scale in.
        // pure scale out means that only new servers are added and no servers are removed, vice versa
        RebalanceSummaryResult.ServerInfo serverInfo =
            dryRunResults.get(table).getRebalanceSummaryResult().getServerInfo();
        if (!serverInfo.getServersAdded().isEmpty() && serverInfo.getServersRemoved().isEmpty()) {
          // dimension table's pure scale OUT should be performed BEFORE other regular tables so that queries involving
          // joining with dimension table won't fail on the new servers
          firstQueue.add(jobContext);
        } else if (serverInfo.getServersAdded().isEmpty() && !serverInfo.getServersRemoved().isEmpty()) {
          // dimension table's pure scale IN should be performed AFTER other regular tables so that queries involving
          // joining with dimension table won't fail on the old servers
          lastQueue.add(jobContext);
        } else {
          // the dimension table is not a pure scale out or scale in, which is supposed to be rebalanced manually.
          // Pre-check should capture and warn about this case.
          firstQueue.add(jobContext);
        }
      } else {
        queue.add(jobContext);
      }
    });
    ConcurrentLinkedQueue<TenantTableRebalanceJobContext> tableQueue = new ConcurrentLinkedQueue<>();
    tableQueue.addAll(firstQueue);
    tableQueue.addAll(queue);
    tableQueue.addAll(lastQueue);
    return tableQueue;
  }
}
