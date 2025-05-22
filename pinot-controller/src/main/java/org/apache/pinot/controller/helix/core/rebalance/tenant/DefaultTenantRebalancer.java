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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.exception.RebalanceInProgressException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
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
    Map<String, RebalanceResult> rebalanceResult = new HashMap<>();
    Set<String> tables = getTenantTables(config.getTenantName());
    Set<String> allowTables = config.getAllowTables();
    if (!allowTables.isEmpty()) {
      tables.retainAll(allowTables);
    }
    tables.removeAll(config.getBlockTables());
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
    ConcurrentLinkedDeque<String> parallelQueue = getTableQueue(config, tables, true);
    // ensure atleast 1 thread is created to run the sequential table rebalance operations
    int parallelism = Math.max(config.getDegreeOfParallelism(), 1);
    try {
      for (int i = 0; i < parallelism; i++) {
        _executorService.submit(() -> {
          while (true) {
            String table = parallelQueue.pollFirst();
            if (table == null) {
              break;
            }
            RebalanceConfig rebalanceConfig = RebalanceConfig.copy(config);
            rebalanceConfig.setDryRun(false);
            if (rebalanceResult.get(table)
                .getRebalanceSummaryResult()
                .getSegmentInfo()
                .getReplicationFactor()
                .getExpectedValueAfterRebalance() == 1) {
              rebalanceConfig.setMinAvailableReplicas(0);
            }
            rebalanceTable(table, rebalanceConfig, rebalanceResult.get(table).getJobId(), observer);
          }
          observer.onSuccess(String.format("Successfully rebalanced tenant %s.", config.getTenantName()));
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

  private void rebalanceTable(String tableName, RebalanceConfig config, String rebalanceJobId,
      TenantRebalanceObserver observer) {
    try {
      observer.onTrigger(TenantRebalanceObserver.Trigger.REBALANCE_STARTED_TRIGGER, tableName, rebalanceJobId);
      RebalanceResult result = _tableRebalanceManager.rebalanceTable(tableName, config, rebalanceJobId, true, true);
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

  private ConcurrentLinkedDeque<String> getTableQueue(TenantRebalanceConfig config, Set<String> tables,
      boolean isScaleOut) {
    ConcurrentLinkedDeque<String> queue = new ConcurrentLinkedDeque<>();
    Set<String> dimTables = getDimensionalTables(config.getTenantName());
    tables.forEach(table -> {
      if (isScaleOut) {
        if (dimTables.contains(table)) {
          // prioritise dimension tables
          queue.addFirst(table);
        } else {
          queue.addLast(table);
        }
      } else {
        if (dimTables.contains(table)) {
          queue.addLast(table);
        } else {
          // prioritise non-dimension tables
          queue.addFirst(table);
        }
      }
    });
    return queue;
  }
}
