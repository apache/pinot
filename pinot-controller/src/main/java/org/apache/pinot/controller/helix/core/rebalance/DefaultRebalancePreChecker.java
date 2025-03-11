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
import javax.annotation.Nullable;
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

  protected PinotHelixResourceManager _pinotHelixResourceManager;
  protected ExecutorService _executorService;

  @Override
  public void init(PinotHelixResourceManager pinotHelixResourceManager, @Nullable ExecutorService executorService) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _executorService = executorService;
  }

  @Override
  public Map<String, RebalancePreCheckerResult> check(String rebalanceJobId, String tableNameWithType,
      TableConfig tableConfig) {
    LOGGER.info("Start pre-checks for table: {} with rebalanceJobId: {}", tableNameWithType, rebalanceJobId);

    Map<String, RebalancePreCheckerResult> preCheckResult = new HashMap<>();
    // Check for reload status
    preCheckResult.put(NEEDS_RELOAD_STATUS, checkReloadNeededOnServers(rebalanceJobId, tableNameWithType));
    // Check whether minimizeDataMovement is set in TableConfig
    preCheckResult.put(IS_MINIMIZE_DATA_MOVEMENT, checkIsMinimizeDataMovement(rebalanceJobId,
        tableNameWithType, tableConfig));

    LOGGER.info("End pre-checks for table: {} with rebalanceJobId: {}", tableNameWithType, rebalanceJobId);
    return preCheckResult;
  }

  /**
   * Checks if the current segments on any servers needs a reload (table config or schema change that hasn't been
   * applied yet). This check does not guarantee that the segments in deep store are up to date.
   * TODO: Add an API to check for whether segments in deep store are up to date with the table configs and schema
   *       and add a pre-check here to call that API.
   */
  private RebalancePreCheckerResult checkReloadNeededOnServers(String rebalanceJobId, String tableNameWithType) {
    LOGGER.info("Fetching whether reload is needed for table: {} with rebalanceJobId: {}", tableNameWithType,
        rebalanceJobId);
    Boolean needsReload = null;
    if (_executorService == null) {
      LOGGER.warn("Executor service is null, skipping needsReload check for table: {} rebalanceJobId: {}",
          tableNameWithType, rebalanceJobId);
      return RebalancePreCheckerResult.warning("Could not determine needReload status, run manually");
    }
    try (PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager()) {
      TableMetadataReader metadataReader = new TableMetadataReader(_executorService, connectionManager,
          _pinotHelixResourceManager);
      TableMetadataReader.TableReloadJsonResponse needsReloadMetadataPair =
          metadataReader.getServerCheckSegmentsReloadMetadata(tableNameWithType, 30_000);
      Map<String, JsonNode> needsReloadMetadata = needsReloadMetadataPair.getServerReloadJsonResponses();
      int failedResponses = needsReloadMetadataPair.getNumFailedResponses();
      LOGGER.info("Received {} needs reload responses and {} failed responses from servers for table: {} with "
              + "rebalanceJobId: {}", needsReloadMetadata.size(), failedResponses, tableNameWithType, rebalanceJobId);
      needsReload = needsReloadMetadata.values().stream().anyMatch(value -> value.get("needReload").booleanValue());
      if (!needsReload && failedResponses > 0) {
        LOGGER.warn("Received {} failed responses from servers and needsReload is false from returned responses, "
            + "check needsReload status manually", failedResponses);
        needsReload = null;
      }
    } catch (InvalidConfigException | IOException e) {
      LOGGER.warn("Caught exception while trying to fetch reload status from servers", e);
    }

    return needsReload == null
        ? RebalancePreCheckerResult.error("Could not determine needReload status, run manually")
        : !needsReload ? RebalancePreCheckerResult.pass("No need to reload")
            : RebalancePreCheckerResult.warning("Reload needed prior to running rebalance");
  }

  /**
   * Checks if minimize data movement is set for the given table in the TableConfig
   */
  private RebalancePreCheckerResult checkIsMinimizeDataMovement(String rebalanceJobId, String tableNameWithType,
      TableConfig tableConfig) {
    LOGGER.info("Checking whether minimizeDataMovement is set for table: {} with rebalanceJobId: {}", tableNameWithType,
        rebalanceJobId);
    try {
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        boolean isInstanceAssignmentAllowed = InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig,
            InstancePartitionsType.OFFLINE);
        InstanceAssignmentConfig instanceAssignmentConfig;
        RebalancePreCheckerResult rebalancePreCheckerResult;
        if (isInstanceAssignmentAllowed) {
          instanceAssignmentConfig =
              InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE);
          rebalancePreCheckerResult = instanceAssignmentConfig.isMinimizeDataMovement()
              ? RebalancePreCheckerResult.pass("minimizeDataMovement is enabled")
              : RebalancePreCheckerResult.warning("minimizeDataMovement is not enabled but instance assignment "
                  + "is allowed");
        } else {
          rebalancePreCheckerResult =
              RebalancePreCheckerResult.pass("Instance assignment not allowed, no need for minimizeDataMovement");
        }
        return rebalancePreCheckerResult;
      } else {
        boolean isInstanceAssignmentAllowedConsuming = InstanceAssignmentConfigUtils.allowInstanceAssignment(
            tableConfig, InstancePartitionsType.CONSUMING);
        InstanceAssignmentConfig instanceAssignmentConfigConsuming = null;
        if (isInstanceAssignmentAllowedConsuming) {
          instanceAssignmentConfigConsuming =
              InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.CONSUMING);
        }
        // For REALTIME tables need to check for both CONSUMING and COMPLETED segments if relocation is enabled
        if (!InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig)) {
          RebalancePreCheckerResult rebalancePreCheckerResult;
          if (isInstanceAssignmentAllowedConsuming) {
            rebalancePreCheckerResult = instanceAssignmentConfigConsuming.isMinimizeDataMovement()
                ? RebalancePreCheckerResult.pass("minimizeDataMovement is enabled")
                : RebalancePreCheckerResult.warning("minimizeDataMovement is not enabled but instance assignment "
                    + "is allowed");
          } else {
            rebalancePreCheckerResult =
                RebalancePreCheckerResult.pass("Instance assignment not allowed, no need for minimizeDataMovement");
          }
          return rebalancePreCheckerResult;
        }

        boolean isInstanceAssignmentAllowedCompleted = InstanceAssignmentConfigUtils.allowInstanceAssignment(
            tableConfig, InstancePartitionsType.COMPLETED);
        InstanceAssignmentConfig instanceAssignmentConfigCompleted = null;
        if (isInstanceAssignmentAllowedCompleted) {
          instanceAssignmentConfigCompleted =
              InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.COMPLETED);
        }

        RebalancePreCheckerResult rebalancePreCheckerResult;
        if (!isInstanceAssignmentAllowedConsuming && !isInstanceAssignmentAllowedCompleted) {
          rebalancePreCheckerResult =
              RebalancePreCheckerResult.pass("Instance assignment not allowed, no need for minimizeDataMovement");
        } else if (instanceAssignmentConfigConsuming != null && instanceAssignmentConfigCompleted != null) {
          rebalancePreCheckerResult = instanceAssignmentConfigConsuming.isMinimizeDataMovement()
              && instanceAssignmentConfigCompleted.isMinimizeDataMovement()
              ? RebalancePreCheckerResult.pass("minimizeDataMovement is enabled")
              : RebalancePreCheckerResult.warning("minimizeDataMovement may not enabled for consuming or completed "
                  + "but instance assignment is allowed for both");
        } else {
          rebalancePreCheckerResult = RebalancePreCheckerResult.warning("minimizeDataMovement may not enabled for "
              + "consuming or completed but instance assignment is allowed for at least one");
        }
        return rebalancePreCheckerResult;
      }
    } catch (IllegalStateException e) {
      LOGGER.warn("Error while trying to fetch instance assignment config, assuming minimizeDataMovement is false", e);
      return RebalancePreCheckerResult.error("Got exception when fetching instance assignment, check manually");
    }
  }
}
