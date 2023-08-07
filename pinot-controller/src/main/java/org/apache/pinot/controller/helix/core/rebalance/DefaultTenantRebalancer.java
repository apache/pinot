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
package org.apache.pinot.controller.helix.core.rebalance;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTenantRebalancer implements TenantRebalancer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTenantRebalancer.class);
  PinotHelixResourceManager _pinotHelixResourceManager;
  ExecutorService _executorService;

  public DefaultTenantRebalancer(PinotHelixResourceManager pinotHelixResourceManager, ExecutorService executorService) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _executorService = executorService;
  }

  @Override
  public TenantRebalanceResult rebalance(TenantRebalanceContext context) {
    Map<String, RebalanceResult> rebalanceResult = new HashMap<>();
    Set<String> tables = getTenantTables(context.getTenantName());
    tables.forEach(table -> {
      try {
        Configuration config = extractRebalanceConfig(context);
        config.setProperty(RebalanceConfigConstants.DRY_RUN, true);
        rebalanceResult.put(table, _pinotHelixResourceManager.rebalanceTable(table, config, false));
      } catch (TableNotFoundException exception) {
        rebalanceResult.put(table, new RebalanceResult(null, RebalanceResult.Status.FAILED, exception.getMessage(),
            null, null, null));
      }
    });
    if (context.isDryRun() || context.isDowntime()) {
      return new TenantRebalanceResult(null, rebalanceResult, context.isVerboseResult());
    } else {
      for (String table : rebalanceResult.keySet()) {
        RebalanceResult result = rebalanceResult.get(table);
        if (result.getStatus() == RebalanceResult.Status.DONE) {
          rebalanceResult.put(table, new RebalanceResult(result.getJobId(), RebalanceResult.Status.IN_PROGRESS,
              "In progress, check controller task status for the", result.getInstanceAssignment(),
              result.getTierInstanceAssignment(), result.getSegmentAssignment()));
        }
      }
    }

    String tenantRebalanceJobId = createUniqueRebalanceJobIdentifier();
    TenantRebalanceObserver observer = new ZkBasedTenantRebalanceObserver(tenantRebalanceJobId, context.getTenantName(),
        tables, _pinotHelixResourceManager);
    observer.onTrigger(TenantRebalanceObserver.Trigger.START_TRIGGER, null, null);
    Set<String> parallelTables;
    Set<String> sequentialTables;
    final ConcurrentLinkedQueue<String> parallelQueue;
    try {
      if (context.getDegreeOfParallelism() > 1) {
        if (!context.getParallelWhitelist().isEmpty()) {
          parallelTables = new HashSet<>(context.getParallelWhitelist());
        } else {
          parallelTables = new HashSet<>(tables);
        }
        if (!context.getParallelBlacklist().isEmpty()) {
          parallelTables = Sets.difference(parallelTables, context.getParallelBlacklist());
        }
        parallelQueue = new ConcurrentLinkedQueue<>(parallelTables);
        for (int i = 0; i < context.getDegreeOfParallelism(); i++) {
          _executorService.submit(() -> {
            try {
              while (true) {
                String table = parallelQueue.remove();
                Configuration config = extractRebalanceConfig(context);
                config.setProperty(RebalanceConfigConstants.DRY_RUN, false);
                config.setProperty(RebalanceConfigConstants.JOB_ID, rebalanceResult.get(table).getJobId());
                rebalanceTable(table, config, observer);
              }
            } catch (NoSuchElementException ignore) {
            }
          });
        }
        sequentialTables = Sets.difference(tables, parallelTables);
      } else {
        sequentialTables = new HashSet<>(tables);
        parallelQueue = new ConcurrentLinkedQueue<>();
      }
      _executorService.submit(() -> {
        while (!parallelQueue.isEmpty()) {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        Configuration config = extractRebalanceConfig(context);
        config.setProperty(RebalanceConfigConstants.DRY_RUN, false);
        for (String table : sequentialTables) {
          config.setProperty(RebalanceConfigConstants.JOB_ID, rebalanceResult.get(table).getJobId());
          rebalanceTable(table, config, observer);
        }
        observer.onSuccess(String.format("Successfully rebalanced tenant %s.", context.getTenantName()));
      });
    } catch (Exception exception) {
      observer.onError(String.format("Failed to rebalance the tenant %s. Cause: %s", context.getTenantName(),
          exception.getMessage()));
    }
    return new TenantRebalanceResult(tenantRebalanceJobId, rebalanceResult, context.isVerboseResult());
  }

  private Configuration extractRebalanceConfig(TenantRebalanceContext context) {
    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.addProperty(RebalanceConfigConstants.DRY_RUN, context.isDryRun());
    rebalanceConfig.addProperty(RebalanceConfigConstants.REASSIGN_INSTANCES, context.isReassignInstances());
    rebalanceConfig.addProperty(RebalanceConfigConstants.INCLUDE_CONSUMING, context.isIncludeConsuming());
    rebalanceConfig.addProperty(RebalanceConfigConstants.BOOTSTRAP, context.isBootstrap());
    rebalanceConfig.addProperty(RebalanceConfigConstants.DOWNTIME, context.isDowntime());
    rebalanceConfig.addProperty(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME,
        context.getMinAvailableReplicas());
    rebalanceConfig.addProperty(RebalanceConfigConstants.BEST_EFFORTS, context.isBestEfforts());
    rebalanceConfig.addProperty(RebalanceConfigConstants.EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS,
        context.getExternalViewCheckIntervalInMs());
    rebalanceConfig.addProperty(RebalanceConfigConstants.EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS,
        context.getExternalViewStabilizationTimeoutInMs());
    rebalanceConfig.addProperty(RebalanceConfigConstants.UPDATE_TARGET_TIER, context.isUpdateTargetTier());
    rebalanceConfig.addProperty(RebalanceConfigConstants.JOB_ID, createUniqueRebalanceJobIdentifier());
    return rebalanceConfig;
  }

  private String createUniqueRebalanceJobIdentifier() {
    return UUID.randomUUID().toString();
  }

  private Set<String> getTenantTables(String tenantName) {
    Set<String> tables = new HashSet<>();
    for (String table : _pinotHelixResourceManager.getAllTables()) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(table);
      if (tableConfig == null) {
        LOGGER.error("Unable to retrieve table config for table: {}", table);
        continue;
      }
      String tableConfigTenant = tableConfig.getTenantConfig().getServer();
      if (tenantName.equals(tableConfigTenant)) {
        tables.add(table);
      }
    }
    return tables;
  }

  private void rebalanceTable(String tableName, Configuration config,
      TenantRebalanceObserver observer) {
    try {
      observer.onTrigger(TenantRebalanceObserver.Trigger.REBALANCE_STARTED_TRIGGER, tableName,
          config.getString(RebalanceConfigConstants.JOB_ID));
      RebalanceResult result = _pinotHelixResourceManager.rebalanceTable(tableName, config, true);
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
}
