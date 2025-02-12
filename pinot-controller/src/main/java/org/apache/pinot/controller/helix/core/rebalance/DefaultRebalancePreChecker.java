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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultRebalancePreChecker implements RebalancePreChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRebalancePreChecker.class);

  public static final String NEEDS_RELOAD_STATUS = "needsReloadStatus";
  public static final String IS_MINIMIZE_DATA_MOVEMENT = "isMinimizeDataMovement";

  private PinotHelixResourceManager _pinotHelixResourceManager;
  private ExecutorService _executorService;

  @Override
  public void init(PinotHelixResourceManager pinotHelixResourceManager, ExecutorService executorService) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _executorService = executorService;
  }

  @Override
  public Map<String, String> check(String rebalanceJobId, String tableNameWithType,
      TableConfig tableConfig) {
    LOGGER.info("Start pre-checks for table: {} with rebalanceJobId: {}", tableNameWithType, rebalanceJobId);

    Map<String, String> preCheckResult = new HashMap<>();
    // Check for reload status
    Boolean needsReload = checkReloadNeededOnServers(rebalanceJobId, tableNameWithType);
    preCheckResult.put(NEEDS_RELOAD_STATUS,
        needsReload == null ? "error" : needsReload ? String.valueOf(true) : String.valueOf(false));
    // Check whether minimizeDataMovement is set in TableConfig
    boolean isMinimizeDataMovement = checkIsMinimizeDataMovement(rebalanceJobId, tableNameWithType, tableConfig);
    preCheckResult.put(IS_MINIMIZE_DATA_MOVEMENT, String.valueOf(isMinimizeDataMovement));

    LOGGER.info("End pre-checks for table: {} with rebalanceJobId: {}", tableNameWithType, rebalanceJobId);
    return preCheckResult;
  }

  /**
   * Checks if the current segments on any servers needs a reload (table config or schema change that hasn't been
   * applied yet). This check does not guarantee that the segments in deep store are up to date.
   * TODO: Add an API to check for whether segments in deep store are up to date with the table configs and schema
   *       and add a pre-check here to call that API.
   */
  private Boolean checkReloadNeededOnServers(String rebalanceJobId, String tableNameWithType) {
    // Use at most 10 threads to get whether reload is needed from servers
    LOGGER.info("Fetching whether reload is needed for table: {} with rebalanceJobId: {}", tableNameWithType,
        rebalanceJobId);
    Boolean needsReload = null;
    try (PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager()) {
      TableMetadataReader metadataReader = new TableMetadataReader(_executorService, connectionManager,
          _pinotHelixResourceManager);
      TableMetadataReader.TableReloadJsonResponse needsReloadMetadataPair =
          metadataReader.getServerCheckSegmentsReloadMetadata(tableNameWithType, 30_000);
      Map<String, JsonNode> needsReloadMetadata = needsReloadMetadataPair.getServerReloadJsonResponses();
      int failedResponses = needsReloadMetadataPair.getNumFailedResponses();
      LOGGER.info("Received {} needs reload responses and {} failed responses from servers for table: {} with "
              + "rebalanceJobId: {}", needsReloadMetadata.size(), failedResponses, tableNameWithType, rebalanceJobId);
      needsReload =
          needsReloadMetadata.values().stream().anyMatch(value -> value.get("needReload").booleanValue());
      if (!needsReload && failedResponses > 0) {
        LOGGER.warn("Received some failed responses from servers and needs reload indicates false from servers that "
            + "returned responses. Setting return to 'null' to force a manual check");
        needsReload = null;
      }
    } catch (InvalidConfigException | IOException e) {
      LOGGER.warn("Caught exception while trying to fetch reload status from servers", e);
    }

    return needsReload;
  }

  /**
   * Checks if minimize data movement is set for the given table in the TableConfig
   */
  private boolean checkIsMinimizeDataMovement(String rebalanceJobId, String tableNameWithType,
      TableConfig tableConfig) {
    LOGGER.info("Checking whether minimizeDataMovement is set for table: {} with rebalanceJobId: {}", tableNameWithType,
        rebalanceJobId);
    try {
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        InstanceAssignmentConfig instanceAssignmentConfig =
            InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE);
        return instanceAssignmentConfig.isMinimizeDataMovement();
      } else {
        InstanceAssignmentConfig instanceAssignmentConfigConsuming =
            InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.CONSUMING);
        // For REALTIME tables need to check for both CONSUMING and COMPLETED segments if relocation is enabled
        if (!InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig)) {
          return instanceAssignmentConfigConsuming.isMinimizeDataMovement();
        }

        InstanceAssignmentConfig instanceAssignmentConfigCompleted =
            InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.COMPLETED);
        return instanceAssignmentConfigConsuming.isMinimizeDataMovement()
            && instanceAssignmentConfigCompleted.isMinimizeDataMovement();
      }
    } catch (IllegalStateException e) {
      LOGGER.warn("Error while trying to fetch instance assignment config, assuming minimizeDataMovement is false", e);
      return false;
    }
  }
}
