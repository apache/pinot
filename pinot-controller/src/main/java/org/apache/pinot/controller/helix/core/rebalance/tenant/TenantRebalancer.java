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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
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


public class TenantRebalancer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TenantRebalancer.class);
  private final TableRebalanceManager _tableRebalanceManager;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ExecutorService _executorService;

  public TenantRebalancer(TableRebalanceManager tableRebalanceManager,
      PinotHelixResourceManager pinotHelixResourceManager, ExecutorService executorService) {
    _tableRebalanceManager = tableRebalanceManager;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _executorService = executorService;
  }

  public static class TenantTableRebalanceJobContext {
    private final String _tableName;
    private final String _jobId;
    // Whether the rebalance should be done with downtime or minAvailableReplicas=0.
    private final boolean _withDowntime;

    /**
     * Create a context to run a table rebalance job with in a tenant rebalance operation.
     *
     * @param tableName The name of the table to rebalance.
     * @param jobId The job ID for the rebalance operation.
     * @param withDowntime Whether the rebalance should be done with downtime or minAvailableReplicas=0.
     * @return The result of the rebalance operation.
     */
    @JsonCreator
    public TenantTableRebalanceJobContext(@JsonProperty("tableName") String tableName,
        @JsonProperty("jobId") String jobId, @JsonProperty("withDowntime") boolean withDowntime) {
      _tableName = tableName;
      _jobId = jobId;
      _withDowntime = withDowntime;
    }

    public String getJobId() {
      return _jobId;
    }

    public String getTableName() {
      return _tableName;
    }

    @JsonProperty("withDowntime")
    public boolean shouldRebalanceWithDowntime() {
      return _withDowntime;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TenantTableRebalanceJobContext)) {
        return false;
      }
      TenantTableRebalanceJobContext that = (TenantTableRebalanceJobContext) o;
      return _withDowntime == that._withDowntime && Objects.equals(_tableName, that._tableName)
          && Objects.equals(_jobId, that._jobId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_tableName, _jobId, _withDowntime);
    }
  }

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
    Pair<ConcurrentLinkedDeque<TenantTableRebalanceJobContext>, Queue<TenantTableRebalanceJobContext>> queues =
        createParallelAndSequentialQueues(config, dryRunResults, config.getParallelWhitelist(),
            config.getParallelBlacklist());
    ConcurrentLinkedDeque<TenantTableRebalanceJobContext> parallelQueue = queues.getLeft();
    Queue<TenantTableRebalanceJobContext> sequentialQueue = queues.getRight();
    TenantRebalanceContext tenantRebalanceContext =
        TenantRebalanceContext.forInitialRebalance(tenantRebalanceJobId, config, parallelQueue,
            sequentialQueue);

    // ZK observer would likely to fail to update if the allowed retries is lower than the degree of parallelism,
    // because all threads would poll when the tenant rebalance job starts at the same time.
    int observerUpdaterMaxRetries =
        Math.max(config.getDegreeOfParallelism(), ZkBasedTenantRebalanceObserver.DEFAULT_ZK_UPDATE_MAX_RETRIES);
    ZkBasedTenantRebalanceObserver observer =
        new ZkBasedTenantRebalanceObserver(tenantRebalanceContext.getJobId(), config.getTenantName(),
            tables, tenantRebalanceContext, _pinotHelixResourceManager, observerUpdaterMaxRetries);
    // Step 4: Spin up threads to consume the parallel queue and sequential queue.
    rebalanceWithObserver(observer, config);

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

  /**
   * Spins up threads to rebalance the tenant with the given context and observer. The rebalance operation is performed
   * in parallel for the tables in the parallel queue, then, sequentially for the tables in the sequential queue. The
   * observer should be initiated with the tenantRebalanceContext in order to track the progress properly.
   *
   * @param observer The observer to notify about the rebalance progress and results.
   */
  public void rebalanceWithObserver(ZkBasedTenantRebalanceObserver observer, TenantRebalanceConfig config) {
    observer.onStart();

    // ensure atleast 1 thread is created to run the sequential table rebalance operations
    int parallelism = Math.max(config.getDegreeOfParallelism(), 1);
    LOGGER.info("Spinning up {} threads for tenant rebalance job: {}", parallelism, observer.getJobId());
    AtomicInteger activeThreads = new AtomicInteger(parallelism);
    try {
      for (int i = 0; i < parallelism; i++) {
        _executorService.submit(() -> {
          doConsumeTablesFromQueueAndRebalance(config, observer, true);
          // If this is the last thread to finish, start consuming the sequential queue
          if (activeThreads.decrementAndGet() == 0) {
            LOGGER.info("All parallel threads completed, starting sequential rebalance for job: {}",
                observer.getJobId());
            doConsumeTablesFromQueueAndRebalance(config, observer, false);
            observer.onSuccess(String.format("Successfully rebalanced tenant %s.", config.getTenantName()));
            LOGGER.info("Completed tenant rebalance job: {}", observer.getJobId());
          }
        });
      }
    } catch (Exception exception) {
      observer.onError(String.format("Failed to rebalance the tenant %s. Cause: %s", config.getTenantName(),
          exception.getMessage()));
      LOGGER.error("Caught exception in tenant rebalance job: {}, Cause: {}", observer.getJobId(),
          exception.getMessage(), exception);
    }
  }

  /**
   * Consumes tables from the given queue from the DefaultTenantRebalanceContext that is being monitored by the
   * observer and rebalances them using the provided config.
   * The ongoing jobs are tracked in the ongoingJobs queue, which is also from the monitored
   * DefaultTenantRebalanceContext.
   *
   * @param config The rebalance configuration to use for the rebalancing.
   * @param observer The observer to notify about the rebalance progress and results, should be initiated with the
   *                 DefaultTenantRebalanceContext that contains `queue` and `ongoingJobs`.
   */
  private void doConsumeTablesFromQueueAndRebalance(RebalanceConfig config,
      ZkBasedTenantRebalanceObserver observer, boolean isParallel) {
    while (true) {
      TenantTableRebalanceJobContext jobContext;
      try {
        jobContext = isParallel ? observer.pollParallel() : observer.pollSequential();
      } catch (Exception e) {
        LOGGER.error("Caught exception while polling from the queue in tenant rebalance job: {}",
            observer.getJobId(), e);
        break;
      }
      if (jobContext == null) {
        break;
      }
      String tableName = jobContext.getTableName();
      String rebalanceJobId = jobContext.getJobId();
      RebalanceConfig rebalanceConfig = RebalanceConfig.copy(config);
      rebalanceConfig.setDryRun(false);
      if (jobContext.shouldRebalanceWithDowntime()) {
        rebalanceConfig.setMinAvailableReplicas(0);
      }
      try {
        LOGGER.info("Starting rebalance for table: {} with table rebalance job ID: {} in tenant rebalance job: {}",
            tableName, rebalanceJobId, observer.getJobId());
        // Disallow TABLE rebalance checker to retry the rebalance job here, since we want TENANT rebalance checker
        // to do so
        RebalanceResult result =
            _tableRebalanceManager.rebalanceTable(tableName, rebalanceConfig, rebalanceJobId, false);
        // TODO: For downtime=true rebalance, track if the EV-IS has converged to move on, otherwise it fundementally
        //  breaks the degree of parallelism
        if (result.getStatus().equals(RebalanceResult.Status.DONE)) {
          LOGGER.info("Completed rebalance for table: {} with table rebalance job ID: {} in tenant rebalance job: {}",
              tableName, rebalanceJobId, observer.getJobId());
          observer.onTableJobDone(jobContext);
        } else {
          LOGGER.warn(
              "Rebalance for table: {} with table rebalance job ID: {} in tenant rebalance job: {} is not done."
                  + "Status: {}, Description: {}", tableName, rebalanceJobId, observer.getJobId(), result.getStatus(),
              result.getDescription());
          observer.onTableJobError(jobContext, result.getDescription());
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while rebalancing table: {} with table rebalance job ID: {} in tenant "
            + "rebalance job: {}", tableName, rebalanceJobId, observer.getJobId(), e);
        observer.onTableJobError(jobContext,
            String.format("Caught exception/error while rebalancing table: %s. %s", tableName, e.getMessage()));
      }
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
  Pair<ConcurrentLinkedDeque<TenantTableRebalanceJobContext>, Queue<TenantTableRebalanceJobContext>>
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
    ConcurrentLinkedDeque<TenantTableRebalanceJobContext> parallelQueue =
        createTableQueue(config, parallelTableDryRunResults);
    Queue<TenantTableRebalanceJobContext> sequentialQueue = createTableQueue(config, sequentialTableDryRunResults);
    return Pair.of(parallelQueue, sequentialQueue);
  }

  @VisibleForTesting
  ConcurrentLinkedDeque<TenantTableRebalanceJobContext> createTableQueue(TenantRebalanceConfig config,
      Map<String, RebalanceResult> dryRunResults) {
    Queue<TenantTableRebalanceJobContext> firstQueue = new LinkedList<>();
    Queue<TenantTableRebalanceJobContext> queue = new LinkedList<>();
    Queue<TenantTableRebalanceJobContext> lastQueue = new LinkedList<>();
    Set<String> dimTables = getDimensionalTables(config.getTenantName());
    dryRunResults.forEach((table, dryRunResult) -> {
      TenantTableRebalanceJobContext jobContext;
      if (dryRunResult.getStatus() == RebalanceResult.Status.FAILED) {
        jobContext = new TenantTableRebalanceJobContext(table, dryRunResult.getJobId(), false);
        LOGGER.warn("Proceeding with table rebalance: {} despite its failed dry-run", table);
      } else {
        Preconditions.checkState(dryRunResult.getRebalanceSummaryResult() != null,
            "Non-failed dry-run result missing summary");
        jobContext =
            new TenantTableRebalanceJobContext(table, dryRunResult.getJobId(), dryRunResult.getRebalanceSummaryResult()
                .getSegmentInfo()
                .getReplicationFactor()
                .getExpectedValueAfterRebalance() == 1);
      }
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
    ConcurrentLinkedDeque<TenantTableRebalanceJobContext> tableQueue = new ConcurrentLinkedDeque<>();
    tableQueue.addAll(firstQueue);
    tableQueue.addAll(queue);
    tableQueue.addAll(lastQueue);
    return tableQueue;
  }
}
