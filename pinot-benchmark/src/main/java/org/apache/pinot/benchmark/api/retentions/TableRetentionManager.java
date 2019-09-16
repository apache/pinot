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
package org.apache.pinot.benchmark.api.retentions;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.pinot.benchmark.PinotBenchConf;
import org.apache.pinot.benchmark.api.PinotClusterManager;
import org.apache.pinot.benchmark.api.resources.PinotBenchException;
import org.apache.pinot.benchmark.common.utils.PinotBenchConstants.PerfTableCreationParameters;
import org.apache.pinot.benchmark.common.utils.PinotBenchConstants.PinotServerType;
import org.apache.pinot.benchmark.common.utils.PinotClusterLocator;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TenantConfig;
import org.apache.pinot.common.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableRetentionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableRetentionManager.class);
  private final PinotClusterManager _pinotClusterManager;
  private final long _tableRetentionManagerInitialDelayInSeconds;
  private final long _tableRetentionManagerIntervalInSeconds;

  private ScheduledExecutorService _executorService;
  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;

  public TableRetentionManager(PinotBenchConf pinotBenchConf, PinotClusterManager pinotClusterManager) {
    _tableRetentionManagerInitialDelayInSeconds = pinotBenchConf.getTableRetentionManagerInitialDelayInSeconds();
    _tableRetentionManagerIntervalInSeconds = pinotBenchConf.getTableRetentionManagerFrequencyInSeconds();
    _pinotClusterManager = pinotClusterManager;
  }

  public void start(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixAdmin = _helixManager.getClusterManagmentTool();

    _executorService = Executors.newSingleThreadScheduledExecutor();
    _executorService.scheduleWithFixedDelay(() -> {
      try {
        List<String> perfTables = _pinotClusterManager.listTables(PinotClusterLocator.PinotClusterType.PERF.name());
        LOGGER.info("Start checking table retentions for {} raw tables", perfTables.size());
        int deletedTableCount = 0;
        for (String perfTable : perfTables) {
          String tableConfigStr =
              _pinotClusterManager.getTableConfig(PinotClusterLocator.PinotClusterType.PERF.name(), perfTable);

          JsonNode jsonNode;
          try {
            jsonNode = JsonUtils.stringToJsonNode(tableConfigStr);
          } catch (IOException e) {
            throw new PinotBenchException(LOGGER, "IOException when parsing string to json node.",
                Response.Status.INTERNAL_SERVER_ERROR);
          }

          JsonNode offlineTableConfigJsonNode = jsonNode.get(PinotServerType.OFFLINE);
          TableConfig offlineTableConfig = null;
          if (offlineTableConfigJsonNode != null) {
            offlineTableConfig = TableConfig.fromJsonConfig(offlineTableConfigJsonNode);
          }

          JsonNode realtimeTableConfigJsonNode = jsonNode.get(PinotServerType.REALTIME);
          TableConfig realtimeTableConfig = null;
          if (realtimeTableConfigJsonNode != null) {
            realtimeTableConfig = TableConfig.fromJsonConfig(realtimeTableConfigJsonNode);
          }

          boolean tableDeleted = false;
          if (shouldDeleteTable(offlineTableConfig)) {
            removeTable(offlineTableConfig);
            tableDeleted = true;
          }
          if (shouldDeleteTable(realtimeTableConfig)) {
            removeTable(realtimeTableConfig);
            tableDeleted = true;
          }
          if (tableDeleted) {
            deletedTableCount++;
          }
        }
        LOGGER.info("Finish checking table retentions. {}/{} tables deleted.", deletedTableCount, perfTables.size());
      } catch (Exception e) {
        throw new PinotBenchException(LOGGER, "Exception when cleaning up perf tables",
            Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }, _tableRetentionManagerInitialDelayInSeconds, _tableRetentionManagerIntervalInSeconds, TimeUnit.SECONDS);
  }

  public void stop() {
    if (_executorService != null) {
      LOGGER.info("Stopping table retention manager");
      _executorService.shutdown();
      _executorService = null;
    }
  }

  private boolean shouldDeleteTable(TableConfig perfTableConfig) {
    if (perfTableConfig == null) {
      return false;
    }
    Map<String, String> customConfigMap = perfTableConfig.getCustomConfig().getCustomConfigs();
    int tableRetention = Integer.parseInt(customConfigMap.get(PerfTableCreationParameters.TABLE_RETENTION_KEY));
    long creationTimeMs = Long.parseLong(customConfigMap.get(PerfTableCreationParameters.CREATION_TIME_MS_KEY));
    long expirationTimeMs = Long.parseLong(customConfigMap.get(PerfTableCreationParameters.EXPIRATION_TIME_MS_KEY));
    long currentTimeMs = System.currentTimeMillis();
    return expirationTimeMs <= currentTimeMs;
  }

  private void removeTable(@Nonnull TableConfig perfTableConfig) {
    String tableNameWithType = perfTableConfig.getTableName();
    LOGGER.info("Table: {} has reached its expiration. Dropping the table.", tableNameWithType);
    _pinotClusterManager.deleteTableConfig(tableNameWithType, perfTableConfig.getTableType().name());

    // TODO: deletes schema if not used by other tables.
    if (false) {
      _pinotClusterManager.deleteSchema(perfTableConfig.getTableName());
    }

    // releases used hosts
    TenantConfig tenantConfig = perfTableConfig.getTenantConfig();
    removeInstancesWithTag(tenantConfig.getBroker());
    removeInstancesWithTag(tenantConfig.getBroker());
  }

  private void removeInstancesWithTag(String tenantTag) {
    List<String> instances = _helixAdmin.getInstancesInClusterWithTag(_helixManager.getClusterName(), tenantTag);
    for (String instance : instances) {
      _helixAdmin.removeInstanceTag(_helixManager.getClusterName(), instance, tenantTag);
    }
  }
}
