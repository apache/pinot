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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.util.TableMetadataReader;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.controller.validation.ResourceUtilizationInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultRebalancePreChecker implements RebalancePreChecker {
  public static final String NEEDS_RELOAD_STATUS = "needsReloadStatus";
  public static final String IS_MINIMIZE_DATA_MOVEMENT = "isMinimizeDataMovement";
  public static final String DISK_UTILIZATION_DURING_REBALANCE = "diskUtilizationDuringRebalance";
  public static final String DISK_UTILIZATION_AFTER_REBALANCE = "diskUtilizationAfterRebalance";
  public static final String REBALANCE_CONFIG_OPTIONS = "rebalanceConfigOptions";
  public static final String REPLICA_GROUPS_INFO = "replicaGroupsInfo";

  private static double _diskUtilizationThreshold;

  protected PinotHelixResourceManager _pinotHelixResourceManager;
  protected ExecutorService _executorService;

  @Override
  public void init(PinotHelixResourceManager pinotHelixResourceManager, @Nullable ExecutorService executorService,
      double diskUtilizationThreshold) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _executorService = executorService;
    _diskUtilizationThreshold = diskUtilizationThreshold;
  }

  @Override
  public Map<String, RebalancePreCheckerResult> check(PreCheckContext preCheckContext) {
    String rebalanceJobId = preCheckContext.getRebalanceJobId();
    String tableNameWithType = preCheckContext.getTableNameWithType();
    TableConfig tableConfig = preCheckContext.getTableConfig();
    RebalanceConfig rebalanceConfig = preCheckContext.getRebalanceConfig();
    Logger tableRebalanceLogger =
        LoggerFactory.getLogger(getClass().getSimpleName() + '-' + tableNameWithType + '-' + rebalanceJobId);

    tableRebalanceLogger.info("Start pre-checks");

    Map<String, RebalancePreCheckerResult> preCheckResult = new HashMap<>();
    // Check for reload status
    preCheckResult.put(NEEDS_RELOAD_STATUS,
        checkReloadNeededOnServers(tableNameWithType, preCheckContext.getCurrentAssignment(), tableRebalanceLogger));
    // Check whether minimizeDataMovement is set in TableConfig
    preCheckResult.put(IS_MINIMIZE_DATA_MOVEMENT,
        checkIsMinimizeDataMovement(tableConfig, rebalanceConfig, tableRebalanceLogger));
    // Check if all servers involved in the rebalance have enough disk space for rebalance operation.
    // Notice this check could have false positives (disk utilization is subject to change by other operations anytime)
    preCheckResult.put(DISK_UTILIZATION_DURING_REBALANCE,
        checkDiskUtilization(preCheckContext.getCurrentAssignment(), preCheckContext.getTargetAssignment(),
            preCheckContext.getTableSubTypeSizeDetails(), _diskUtilizationThreshold, true));
    // Check if all servers involved in the rebalance will have enough disk space after the rebalance.
    // TODO: give this check a separate threshold other than the disk utilization threshold
    preCheckResult.put(DISK_UTILIZATION_AFTER_REBALANCE,
        checkDiskUtilization(preCheckContext.getCurrentAssignment(), preCheckContext.getTargetAssignment(),
            preCheckContext.getTableSubTypeSizeDetails(), _diskUtilizationThreshold, false));

    preCheckResult.put(REBALANCE_CONFIG_OPTIONS, checkRebalanceConfig(rebalanceConfig, tableConfig,
        preCheckContext.getCurrentAssignment(), preCheckContext.getTargetAssignment()));

    preCheckResult.put(REPLICA_GROUPS_INFO, checkReplicaGroups(tableConfig, rebalanceConfig));

    tableRebalanceLogger.info("End pre-checks");
    return preCheckResult;
  }

  /**
   * Checks if the current segments on any servers needs a reload (table config or schema change that hasn't been
   * applied yet). This check does not guarantee that the segments in deep store are up to date.
   * TODO: Add an API to check for whether segments in deep store are up to date with the table configs and schema
   *       and add a pre-check here to call that API.
   */
  private RebalancePreCheckerResult checkReloadNeededOnServers(String tableNameWithType,
      Map<String, Map<String, String>> currentAssignment, Logger tableRebalanceLogger) {
    tableRebalanceLogger.info("Fetching whether reload is needed");
    Boolean needsReload = null;
    if (_executorService == null) {
      tableRebalanceLogger.warn("Executor service is null, skipping needsReload check");
      return RebalancePreCheckerResult.error("Could not determine needReload status, run needReload API manually");
    }
    try (PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager()) {
      TableMetadataReader metadataReader = new TableMetadataReader(_executorService, connectionManager,
          _pinotHelixResourceManager);
      // Only send needReload request to servers that are part of the current assignment. The tagged server list may
      // include new servers which are part of target assignment but not current assignment. needReload throws an
      // exception for servers that don't contain segments for the given table
      Set<String> currentlyAssignedServers = getCurrentlyAssignedServers(currentAssignment);
      TableMetadataReader.TableReloadJsonResponse needsReloadMetadataPair =
          metadataReader.getServerSetCheckSegmentsReloadMetadata(tableNameWithType, 30_000, currentlyAssignedServers);
      Map<String, JsonNode> needsReloadMetadata = needsReloadMetadataPair.getServerReloadJsonResponses();
      int failedResponses = needsReloadMetadataPair.getNumFailedResponses();
      tableRebalanceLogger.info(
          "Received {} needs reload responses and {} failed responses from servers with "
              + "number of servers queried: {}", needsReloadMetadata.size(), failedResponses,
          currentlyAssignedServers.size());
      needsReload = needsReloadMetadata.values().stream().anyMatch(value -> value.get("needReload").booleanValue());
      if (!needsReload && failedResponses > 0) {
        tableRebalanceLogger.warn(
            "Received {} failed responses from servers and needsReload is false from returned responses, "
                + "check needsReload status manually", failedResponses);
        needsReload = null;
      }
    } catch (InvalidConfigException | IOException e) {
      tableRebalanceLogger.warn("Caught exception while trying to fetch reload status from servers", e);
    }

    return needsReload == null
        ? RebalancePreCheckerResult.error("Could not determine needReload status, run needReload API manually")
        : !needsReload ? RebalancePreCheckerResult.pass("No need to reload")
            : RebalancePreCheckerResult.warn("Reload needed prior to running rebalance");
  }

  /**
   * Checks if minimize data movement is set for the given table in the TableConfig
   */
  private RebalancePreCheckerResult checkIsMinimizeDataMovement(TableConfig tableConfig,
      RebalanceConfig rebalanceConfig, Logger tableRebalanceLogger) {
    tableRebalanceLogger.info("Checking whether minimizeDataMovement is set");
    try {
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        boolean isInstanceAssignmentAllowed = InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig,
            InstancePartitionsType.OFFLINE);
        if (isInstanceAssignmentAllowed) {
          if (rebalanceConfig.getMinimizeDataMovement() == Enablement.ENABLE) {
            return RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
          }
          InstanceAssignmentConfig instanceAssignmentConfig =
              InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.OFFLINE);
          if (instanceAssignmentConfig.isMinimizeDataMovement()) {
            return rebalanceConfig.getMinimizeDataMovement() == Enablement.DISABLE
                ? RebalancePreCheckerResult.warn("minimizeDataMovement is enabled in table config but it's overridden "
                + "with disabled") : RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
          }
          return RebalancePreCheckerResult.warn("minimizeDataMovement is not enabled but instance assignment is "
              + "allowed");
        }
        return RebalancePreCheckerResult.pass("Instance assignment not allowed, no need for minimizeDataMovement");
      }

      boolean isInstanceAssignmentAllowedConsuming = InstanceAssignmentConfigUtils.allowInstanceAssignment(
          tableConfig, InstancePartitionsType.CONSUMING);
      InstanceAssignmentConfig instanceAssignmentConfigConsuming = null;
      if (isInstanceAssignmentAllowedConsuming) {
        instanceAssignmentConfigConsuming =
            InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.CONSUMING);
      }
      // For REALTIME tables if COMPLETED segments are not to be relocated, check for only CONSUMING segment instance
      // assignment config if presents
      if (!InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig)) {
        if (isInstanceAssignmentAllowedConsuming) {
          if (rebalanceConfig.getMinimizeDataMovement() == Enablement.ENABLE) {
            return RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
          }
          if (instanceAssignmentConfigConsuming.isMinimizeDataMovement()) {
            return rebalanceConfig.getMinimizeDataMovement() == Enablement.DISABLE
                ? RebalancePreCheckerResult.warn(
                "minimizeDataMovement is enabled for CONSUMING segments in table config but it's overridden "
                    + "with disabled")
                : RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
          }
          return RebalancePreCheckerResult.warn(
              "minimizeDataMovement is not enabled for CONSUMING segments but instance assignment is allowed");
        }
        return RebalancePreCheckerResult.pass("Instance assignment not allowed, no need for minimizeDataMovement");
      }

      boolean isInstanceAssignmentAllowedCompleted = InstanceAssignmentConfigUtils.allowInstanceAssignment(
          tableConfig, InstancePartitionsType.COMPLETED);
      InstanceAssignmentConfig instanceAssignmentConfigCompleted = null;
      if (isInstanceAssignmentAllowedCompleted) {
        instanceAssignmentConfigCompleted =
            InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, InstancePartitionsType.COMPLETED);
      }

      // COMPLETED segments are to be relocated, check both COMPLETED and CONSUMING segment instance assignment config
      // that present
      if (!isInstanceAssignmentAllowedConsuming && !isInstanceAssignmentAllowedCompleted) {
        return RebalancePreCheckerResult.pass("Instance assignment not allowed, no need for minimizeDataMovement");
      } else if (instanceAssignmentConfigConsuming != null && instanceAssignmentConfigCompleted != null) {
        if (rebalanceConfig.getMinimizeDataMovement() == Enablement.ENABLE) {
          return RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        if (instanceAssignmentConfigCompleted.isMinimizeDataMovement()
            && instanceAssignmentConfigConsuming.isMinimizeDataMovement()) {
          return rebalanceConfig.getMinimizeDataMovement() == Enablement.DISABLE
              ? RebalancePreCheckerResult.warn(
              "minimizeDataMovement is enabled for both COMPLETED and CONSUMING segments in table config but it's "
                  + "overridden with disabled")
              : RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        return RebalancePreCheckerResult.warn(
            "minimizeDataMovement is not enabled for either or both COMPLETED and CONSUMING segments, but instance "
                + "assignment is allowed for both");
      } else if (instanceAssignmentConfigConsuming != null) {
        if (rebalanceConfig.getMinimizeDataMovement() == Enablement.ENABLE) {
          return RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        if (instanceAssignmentConfigConsuming.isMinimizeDataMovement()) {
          return rebalanceConfig.getMinimizeDataMovement() == Enablement.DISABLE
              ? RebalancePreCheckerResult.warn(
              "minimizeDataMovement is enabled for CONSUMING segments in table config but it's overridden with "
                  + "disabled")
              : RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        return RebalancePreCheckerResult.warn(
            "minimizeDataMovement is not enabled for CONSUMING segments, but instance assignment is allowed");
      } else {
        if (rebalanceConfig.getMinimizeDataMovement() == Enablement.ENABLE) {
          return RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        if (instanceAssignmentConfigCompleted.isMinimizeDataMovement()) {
          return rebalanceConfig.getMinimizeDataMovement() == Enablement.DISABLE
              ? RebalancePreCheckerResult.warn(
              "minimizeDataMovement is enabled for COMPLETED segments in table config but it's overridden "
                  + "with disabled")
              : RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        return RebalancePreCheckerResult.warn(
            "minimizeDataMovement is not enabled for COMPLETED segments, but instance assignment is allowed");
      }
    } catch (IllegalStateException e) {
      tableRebalanceLogger.warn(
          "Error while trying to fetch instance assignment config, assuming minimizeDataMovement is false", e);
    }
    return RebalancePreCheckerResult.error("Got exception when fetching instance assignment, check manually");
  }

  private Set<String> getCurrentlyAssignedServers(Map<String, Map<String, String>> currentAssignment) {
    Set<String> servers = new HashSet<>();
    for (Map<String, String> serverStateMap : currentAssignment.values()) {
      servers.addAll(serverStateMap.keySet());
    }
    return servers;
  }

  private RebalancePreCheckerResult checkDiskUtilization(Map<String, Map<String, String>> currentAssignment,
      Map<String, Map<String, String>> targetAssignment,
      TableSizeReader.TableSubTypeSizeDetails tableSubTypeSizeDetails, double threshold, boolean worstCase) {
    boolean isDiskUtilSafe = true;
    StringBuilder message =
        new StringBuilder("UNSAFE. Servers with unsafe disk utilization (>" + (short) (threshold * 100) + "%): ");
    String sep = "";
    Map<String, Set<String>> existingServersToSegmentMap = new HashMap<>();
    Map<String, Set<String>> newServersToSegmentMap = new HashMap<>();

    for (Map.Entry<String, Map<String, String>> entrySet : currentAssignment.entrySet()) {
      for (String instanceName : entrySet.getValue().keySet()) {
        existingServersToSegmentMap.computeIfAbsent(instanceName, k -> new HashSet<>()).add(entrySet.getKey());
      }
    }

    for (Map.Entry<String, Map<String, String>> entrySet : targetAssignment.entrySet()) {
      for (String instanceName : entrySet.getValue().keySet()) {
        newServersToSegmentMap.computeIfAbsent(instanceName, k -> new HashSet<>()).add(entrySet.getKey());
      }
    }

    long avgSegmentSize = getAverageSegmentSize(tableSubTypeSizeDetails, currentAssignment);

    for (Map.Entry<String, Set<String>> entry : newServersToSegmentMap.entrySet()) {
      String server = entry.getKey();
      DiskUsageInfo diskUsage = getDiskUsageInfoOfInstance(server);

      if (diskUsage.getTotalSpaceBytes() < 0) {
        return RebalancePreCheckerResult.warn(
            "Disk usage info has not been updated. Try later or set controller.resource.utilization.checker.initial"
                + ".delay to a"
                + " shorter period");
      }

      Set<String> segmentSet = entry.getValue();

      Set<String> newSegmentSet = new HashSet<>(segmentSet);
      Set<String> existingSegmentSet = new HashSet<>();
      Set<String> intersection = new HashSet<>();
      if (existingServersToSegmentMap.containsKey(server)) {
        Set<String> segmentSetForServer = existingServersToSegmentMap.get(server);
        existingSegmentSet.addAll(segmentSetForServer);
        intersection.addAll(segmentSetForServer);
        intersection.retainAll(newSegmentSet);
      }
      newSegmentSet.removeAll(intersection);
      Set<String> removedSegmentSet = new HashSet<>(existingSegmentSet);
      removedSegmentSet.removeAll(intersection);

      long diskUtilizationGain = newSegmentSet.size() * avgSegmentSize;
      long diskUtilizationLoss = removedSegmentSet.size() * avgSegmentSize;

      long diskUtilizationFootprint =
          diskUsage.getUsedSpaceBytes() + diskUtilizationGain - (worstCase ? 0 : diskUtilizationLoss);
      double diskUtilizationFootprintRatio =
          (double) diskUtilizationFootprint / diskUsage.getTotalSpaceBytes();

      if (diskUtilizationFootprintRatio >= threshold) {
        isDiskUtilSafe = false;
        message.append(sep)
            .append(server)
            .append(String.format(" (%d%%)", (short) (diskUtilizationFootprintRatio * 100)));
        sep = ", ";
      }
    }
    return isDiskUtilSafe ? RebalancePreCheckerResult.pass(
        String.format("Within threshold (<%d%%)", (short) (threshold * 100)))
        : RebalancePreCheckerResult.error(message.toString());
  }

  private RebalancePreCheckerResult checkRebalanceConfig(RebalanceConfig rebalanceConfig, TableConfig tableConfig,
      Map<String, Map<String, String>> currentAssignment, Map<String, Map<String, String>> targetAssignment) {
    List<String> warnings = new ArrayList<>();
    boolean pass = true;
    if (rebalanceConfig.isBestEfforts()) {
      pass = false;
      warnings.add("bestEfforts is enabled, only enable it if you know what you are doing");
    }
    List<String> segmentsToMove = SegmentAssignmentUtils.getSegmentsToMove(currentAssignment, targetAssignment);

    if (rebalanceConfig.isDowntime()) {
      int numReplicas = Integer.MAX_VALUE;
      for (String segment : segmentsToMove) {
        numReplicas = Math.min(targetAssignment.get(segment).size(), numReplicas);
      }
      if (!segmentsToMove.isEmpty() && numReplicas > 1) {
        pass = false;
        warnings.add("Number of replicas (" + numReplicas + ") is greater than 1, downtime is not recommended.");
      }
    }

    if (!rebalanceConfig.isIncludeConsuming() && tableConfig.getTableType() == TableType.REALTIME) {
      pass = false;
      warnings.add("includeConsuming is disabled for a realtime table.");
    }
    if (rebalanceConfig.isBootstrap()) {
      pass = false;
      warnings.add(
          "bootstrap is enabled which can cause a large amount of data movement, double check if this is intended");
    }

    return pass ? RebalancePreCheckerResult.pass("All rebalance parameters look good")
        : RebalancePreCheckerResult.warn(StringUtil.join("\n", warnings.toArray(String[]::new)));
  }

  private RebalancePreCheckerResult checkReplicaGroups(TableConfig tableConfig, RebalanceConfig rebalanceConfig) {
    String message;
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      message = getReplicaGroupInfo(tableConfig, InstancePartitionsType.OFFLINE);
    } else {
      // for realtime table
      message = "COMPLETED segments: " + getReplicaGroupInfo(tableConfig, InstancePartitionsType.COMPLETED) + "\n"
          + "CONSUMING segments: " + getReplicaGroupInfo(tableConfig, InstancePartitionsType.CONSUMING);
    }
    if (rebalanceConfig.isReassignInstances()) {
      return RebalancePreCheckerResult.pass(message);
    }
    return RebalancePreCheckerResult.warn("reassignInstances is disabled, replica groups will not update.\n" + message);
  }

  private String getReplicaGroupInfo(TableConfig tableConfig, InstancePartitionsType type) {
    if (!InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, type)) {
      return "Replica Groups are not enabled, replication: " + tableConfig.getReplication();
    }
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        InstanceAssignmentConfigUtils.getInstanceAssignmentConfig(tableConfig, type)
            .getReplicaGroupPartitionConfig();
    if (!instanceReplicaGroupPartitionConfig.isReplicaGroupBased()) {
      return "Replica Groups are not enabled, replication: " + tableConfig.getReplication();
    }

    int numReplicaGroups = instanceReplicaGroupPartitionConfig.getNumReplicaGroups();
    int numInstancePerReplicaGroup = instanceReplicaGroupPartitionConfig.getNumInstancesPerReplicaGroup();
    if (numInstancePerReplicaGroup == 0) {
      return "numReplicaGroups: " + numReplicaGroups
          + ", numInstancesPerReplicaGroup: 0 (using as many instances as possible)";
    }
    return "numReplicaGroups: " + numReplicaGroups
        + ", numInstancesPerReplicaGroup: " + numInstancePerReplicaGroup;
  }

  private DiskUsageInfo getDiskUsageInfoOfInstance(String instanceId) {
    // This method currently depends on the controller's periodic task that fetches disk utilization of all instances
    // every 5 minutes by default.
    return ResourceUtilizationInfo.getDiskUsageInfo(instanceId);
  }

  private long getAverageSegmentSize(TableSizeReader.TableSubTypeSizeDetails tableSubTypeSizeDetails,
      Map<String, Map<String, String>> currentAssignment) {
    long tableSizePerReplicaInBytes = tableSubTypeSizeDetails._reportedSizePerReplicaInBytes;
    return tableSizePerReplicaInBytes / ((long) currentAssignment.size());
  }
}
