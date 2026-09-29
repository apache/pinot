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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.restlet.resources.RebalancePreCheckerResult;
import org.apache.pinot.common.restlet.resources.RebalanceSummaryResult;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.controller.validation.ResourceUtilizationInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultRebalancePreChecker implements RebalancePreChecker {
  public static final String IS_MINIMIZE_DATA_MOVEMENT = "isMinimizeDataMovement";
  public static final String DISK_UTILIZATION = "diskUtilization";
  public static final String REBALANCE_CONFIG_OPTIONS = "rebalanceConfigOptions";
  public static final String REPLICA_GROUPS_INFO = "replicaGroupsInfo";

  public static final int SEGMENT_ADD_THRESHOLD = 200;
  public static final int RECOMMENDED_BATCH_SIZE = 200;

  private static double _defaultDiskUtilizationThreshold;

  protected PinotHelixResourceManager _pinotHelixResourceManager;
  protected ExecutorService _executorService;

  @Override
  public void init(PinotHelixResourceManager pinotHelixResourceManager, @Nullable ExecutorService executorService,
      double diskUtilizationThreshold) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _executorService = executorService;
    _defaultDiskUtilizationThreshold = diskUtilizationThreshold;
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

    // Right now pre-check items are done sequentially. If pre-check items are to be done in parallel, we should not
    // use linked hash map but to sort the result in the end
    Map<String, RebalancePreCheckerResult> preCheckResult = new LinkedHashMap<>();
    // Check whether minimizeDataMovement is set in TableConfig
    preCheckResult.put(IS_MINIMIZE_DATA_MOVEMENT,
        checkIsMinimizeDataMovement(tableConfig, rebalanceConfig, tableRebalanceLogger));
    // Determine the disk utilization threshold to use - either from rebalance config override or default
    double diskUtilizationThreshold = rebalanceConfig.getDiskUtilizationThreshold() >= 0.0
        ? rebalanceConfig.getDiskUtilizationThreshold() : _defaultDiskUtilizationThreshold;
    // clip the disk utilization threshold to [0.0, 1.0]
    if (diskUtilizationThreshold > 1.0) {
      tableRebalanceLogger.warn("Provided disk utilization threshold {} is greater than 1.0, clipping to 1.0",
          diskUtilizationThreshold);
      diskUtilizationThreshold = 1.0;
    }

    // Check if all servers involved in the rebalance have enough disk space, both while the rebalance is running and
    // once it is done.
    // Notice this check could have false positives (disk utilization is subject to change by other operations anytime)
    preCheckResult.put(DISK_UTILIZATION, checkDiskUtilization(preCheckContext, diskUtilizationThreshold));

    preCheckResult.put(REBALANCE_CONFIG_OPTIONS, checkRebalanceConfig(rebalanceConfig, tableConfig,
        preCheckContext.getCurrentAssignment(), preCheckContext.getTargetAssignment(),
        preCheckContext.getRebalanceSummaryResult()));

    preCheckResult.put(REPLICA_GROUPS_INFO, checkReplicaGroups(tableConfig, rebalanceConfig));

    tableRebalanceLogger.info("End pre-checks");
    return preCheckResult;
  }

  /// Checks if minimize data movement is set for the given table in the TableConfig
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
                ? RebalancePreCheckerResult.warn("minimizeDataMovement is enabled for CONSUMING segments in table "
                + "config but it's overridden with disabled")
                : RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
          }
          return RebalancePreCheckerResult.warn("minimizeDataMovement is not enabled for CONSUMING segments, but "
              + "instance assignment is allowed");
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
              ? RebalancePreCheckerResult.warn("minimizeDataMovement is enabled for both COMPLETED and CONSUMING "
              + "segments in table config but it's overridden with disabled")
              : RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        return RebalancePreCheckerResult.warn("minimizeDataMovement is not enabled for either or both COMPLETED and "
            + "CONSUMING segments, but instance assignment is allowed for both");
      } else if (instanceAssignmentConfigConsuming != null) {
        if (rebalanceConfig.getMinimizeDataMovement() == Enablement.ENABLE) {
          return RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        if (instanceAssignmentConfigConsuming.isMinimizeDataMovement()) {
          return rebalanceConfig.getMinimizeDataMovement() == Enablement.DISABLE
              ? RebalancePreCheckerResult.warn("minimizeDataMovement is enabled for CONSUMING segments in table "
              + "config but it's overridden with disabled")
              : RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        return RebalancePreCheckerResult.warn("minimizeDataMovement is not enabled for CONSUMING segments, but "
            + "instance assignment is allowed");
      } else {
        if (rebalanceConfig.getMinimizeDataMovement() == Enablement.ENABLE) {
          return RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        if (instanceAssignmentConfigCompleted.isMinimizeDataMovement()) {
          return rebalanceConfig.getMinimizeDataMovement() == Enablement.DISABLE
              ? RebalancePreCheckerResult.warn("minimizeDataMovement is enabled for COMPLETED segments in table "
              + "config but it's overridden with disabled")
              : RebalancePreCheckerResult.pass("minimizeDataMovement is enabled");
        }
        return RebalancePreCheckerResult.warn("minimizeDataMovement is not enabled for COMPLETED segments, but "
            + "instance assignment is allowed");
      }
    } catch (IllegalStateException e) {
      tableRebalanceLogger.warn("Error while trying to fetch instance assignment config, assuming minimizeDataMovement "
          + "is false", e);
    }
    return RebalancePreCheckerResult.error("Got exception when fetching instance assignment, check manually");
  }

  /// Estimates whether the servers of the target assignment stay within the disk utilization threshold, based on the
  /// average segment size and the number of segments added to and removed from each server. Two points in time are
  /// estimated:
  ///
  /// - **After the rebalance**, i.e. once every server has both added and removed all the segments it has to. Going
  ///   over the threshold there is an error whatever the rebalance config, since no way of running the rebalance
  ///   brings the end state back within the threshold.
  /// - **During the rebalance**, where a server can transiently hold the segments it is gaining on top of the ones it
  ///   is about to lose. Only servers actually gaining segments are estimated: one that merely sheds them can only be
  ///   over the threshold because it already was, which the rebalance neither causes nor can be blamed for. Only
  ///   `lowDiskMode` rules that peak out, by waiting for the segments to be deleted before adding the new ones, so
  ///   going over the threshold there is an error unless it is enabled. `downtime` does not help: it replaces the
  ///   IdealState with the target assignment in one go, without ordering the drops before the adds. Worse, that
  ///   one-shot path skips the incremental one `lowDiskMode` acts on, so `downtime` cancels `lowDiskMode` out
  ///   entirely. `bestEfforts` weakens it rather than cancelling it — the deletes are still awaited, just no longer
  ///   unconditionally — so it downgrades the result to a warning instead of an error.
  ///
  /// Every segment is assumed to take up disk space on each server it is assigned to. Downstream projects where that
  /// does not hold (e.g. because a segment can be stored outside of the server, as indicated by
  /// [TierConfig#getTierBackend()]) can override this.
  protected RebalancePreCheckerResult checkDiskUtilization(PreCheckContext preCheckContext, double threshold) {
    Map<String, Map<String, String>> currentAssignment = preCheckContext.getCurrentAssignment();
    Map<String, Map<String, String>> targetAssignment = preCheckContext.getTargetAssignment();
    TableSizeReader.TableSubTypeSizeDetails tableSubTypeSizeDetails = preCheckContext.getTableSubTypeSizeDetails();
    List<String> serversUnsafeDuringRebalance = new ArrayList<>();
    List<String> serversUnsafeAfterRebalance = new ArrayList<>();
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
                + ".delay to a shorter period");
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

      // While the rebalance is running, the segments being added can co-exist with the ones being removed. A server
      // gaining nothing never builds up that transient usage: it is only ever over the threshold because it already
      // was, which is not something this rebalance causes nor something lowDiskMode could do anything about. If it is
      // still over once the rebalance is done, the estimate below catches it
      if (diskUtilizationGain > 0) {
        addIfOverThreshold(serversUnsafeDuringRebalance, server,
            (double) (diskUsage.getUsedSpaceBytes() + diskUtilizationGain) / diskUsage.getTotalSpaceBytes(), threshold);
      }
      addIfOverThreshold(serversUnsafeAfterRebalance, server,
          (double) (diskUsage.getUsedSpaceBytes() + diskUtilizationGain - diskUtilizationLoss)
              / diskUsage.getTotalSpaceBytes(), threshold);
    }

    // A server over the threshold once the rebalance is done is over it during the rebalance as well, so the end state
    // is what to report first: it is both the more severe problem and the one that has to be solved by adding capacity
    // rather than by tuning the rebalance config
    if (!serversUnsafeAfterRebalance.isEmpty()) {
      return RebalancePreCheckerResult.error(
          getUnsafeDiskUtilizationMessage("AFTER rebalance", serversUnsafeAfterRebalance, threshold));
    }
    String withinThreshold = String.format("Within threshold (<%d%%)", (short) (threshold * 100));
    if (serversUnsafeDuringRebalance.isEmpty()) {
      return RebalancePreCheckerResult.pass(withinThreshold);
    }
    // lowDiskMode is the only way to rule the transient disk usage above out, since it waits for the segments to be
    // deleted before adding the new ones. It is however only honored by the incremental rebalance path, which downtime
    // skips altogether by replacing the IdealState with the target assignment in one go
    RebalanceConfig rebalanceConfig = preCheckContext.getRebalanceConfig();
    if (rebalanceConfig.isDowntime() || !rebalanceConfig.isLowDiskMode()) {
      return RebalancePreCheckerResult.error(
          getUnsafeDiskUtilizationMessage("DURING rebalance", serversUnsafeDuringRebalance, threshold)
              + (rebalanceConfig.isDowntime()
              ? ". lowDiskMode, which would delete segments before adding the new ones, has no effect while downtime "
              + "is enabled"
              : ". Enable lowDiskMode to delete segments before adding the new ones"));
    }
    String serversGoingOver = " Servers that would go over it DURING the rebalance: " + String.join(", ",
        serversUnsafeDuringRebalance) + ".";
    return RebalancePreCheckerResult.pass(withinThreshold + " AFTER rebalance." + serversGoingOver + " lowDiskMode "
        + "avoids that transient disk usage by deleting segments before adding the new ones");
  }

  private static void addIfOverThreshold(List<String> servers, String server, double utilizationRatio,
      double threshold) {
    if (utilizationRatio >= threshold) {
      servers.add(server + String.format(" (%d%%)", (short) (utilizationRatio * 100)));
    }
  }

  /// The threshold is rendered as `>=` because [#addIfOverThreshold] flags a server whose utilization reaches it, not
  /// only one that exceeds it.
  private static String getUnsafeDiskUtilizationMessage(String when, List<String> servers, double threshold) {
    return String.format("UNSAFE. Servers with unsafe disk utilization %s (>=%d%%): %s", when,
        (short) (threshold * 100), String.join(", ", servers));
  }

  private RebalancePreCheckerResult checkRebalanceConfig(RebalanceConfig rebalanceConfig, TableConfig tableConfig,
      Map<String, Map<String, String>> currentAssignment, Map<String, Map<String, String>> targetAssignment,
      @Nullable RebalanceSummaryResult rebalanceSummaryResult) {
    List<String> warnings = new ArrayList<>();
    boolean pass = true;
    if (rebalanceConfig.isBestEfforts()) {
      pass = false;
      warnings.add("bestEfforts is enabled, only enable it if you know what you are doing");
    }
    List<String> segmentsToMove = SegmentAssignmentUtils.getSegmentsToMove(currentAssignment, targetAssignment);

    int numReplicas = Integer.MAX_VALUE;
    String peerSegmentDownloadScheme = tableConfig.getValidationConfig().getPeerSegmentDownloadScheme();
    if (rebalanceConfig.isDowntime() || peerSegmentDownloadScheme != null) {
      for (String segment : segmentsToMove) {
        numReplicas = Math.min(targetAssignment.get(segment).size(), numReplicas);
      }
    }

    // For non-peer download enabled tables, warn if downtime is enabled but numReplicas > 1. Should only use
    // downtime=true for such tables if downtime is indeed acceptable whereas for numReplicas = 1, rebalance cannot
    // be done without downtime
    if (rebalanceConfig.isDowntime()) {
      if (!segmentsToMove.isEmpty() && numReplicas > 1) {
        pass = false;
        warnings.add("Number of replicas (" + numReplicas + ") is greater than 1, downtime is not recommended.");
      }
      // Downtime replaces the IdealState with the target assignment in one go, skipping the incremental path that is
      // the only one honoring lowDiskMode
      if (rebalanceConfig.isLowDiskMode()) {
        pass = false;
        warnings.add("lowDiskMode has no effect when downtime is enabled, disable downtime for segments to be deleted "
            + "before the new ones are added.");
      }
    }

    // Peer download enabled tables may have data loss during rebalance, when downtime=true or minAvailableReplicas=0.
    // The scenario plays out as follows:
    // 1. If the newly built consuming segment cannot be uploaded to deep store, it may set up the download URI
    //    as an empty string: ""
    // 2. When this happens, other servers expect to download the segment from a peer server that built the segment or
    //    has a copy of the segment
    // 3. With downtime rebalance (or if minAvailableReplicas=0), the IS may be updated for all the servers of a given
    //    segment
    // 4. The above may lead to dropping the existing segments from the existing servers without waiting for the newly
    //    added servers to download the segment from the peer. In this case since a deep store copy does not exist,
    //    there is no way to recover this segment without manually re-building it
    // Thus, to avoid the above data loss scenario, it is not recommended to run downtime rebalance for peer download
    // enabled tables. This pre-check is added to warn of the potential risk.
    if (peerSegmentDownloadScheme != null) {
      int minAvailableReplica = rebalanceConfig.getMinAvailableReplicas();
      if (minAvailableReplica < 0) {
        minAvailableReplica = numReplicas + minAvailableReplica;
      }
      if (numReplicas == 1) {
        pass = false;
        warnings.add("Replication of the table is 1, which is not recommended for peer-download enabled tables as it "
            + "may cause data loss during rebalance");
      } else if (rebalanceConfig.isDowntime() || minAvailableReplica <= 0) {
        pass = false;
        warnings.add("Downtime or minAvailableReplicas<=0 for peer-download enabled tables may cause data loss during "
            + "rebalance");
      }
    }

    if (!rebalanceConfig.isIncludeConsuming() && tableConfig.getTableType() == TableType.REALTIME) {
      pass = false;
      warnings.add("includeConsuming is disabled for a realtime table.");
    }
    if (rebalanceConfig.isBootstrap()) {
      pass = false;
      warnings.add("bootstrap is enabled which can cause a large amount of data movement, double check if this is "
          + "intended");
    }
    if (CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList()) && !rebalanceConfig.isUpdateTargetTier()) {
      pass = false;
      warnings.add("updateTargetTier should be enabled when tier configs are present");
    }

    // --- Batch size per server recommendation check using summary ---
    if (rebalanceSummaryResult != null) {
      int maxSegmentsToAddOnServer = rebalanceSummaryResult.getSegmentInfo().getMaxSegmentsAddedToASingleServer();
      int batchSizePerServer = rebalanceConfig.getBatchSizePerServer();
      if (maxSegmentsToAddOnServer > SEGMENT_ADD_THRESHOLD) {
        if (batchSizePerServer == RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER
            || batchSizePerServer > RECOMMENDED_BATCH_SIZE) {
          pass = false;
          warnings.add("Number of segments to add to a single server (" + maxSegmentsToAddOnServer + ") is high (>"
              + SEGMENT_ADD_THRESHOLD + "). It is recommended to set batchSizePerServer to " + RECOMMENDED_BATCH_SIZE
              + " or lower to avoid excessive load on servers.");
        }
      }
    } else {
      // Rebalance summary should not be null when pre-checks are enabled unless an exception was thrown while
      // calculating it
      pass = false;
      warnings.add("Could not assess batchSizePerServer recommendation as rebalance summary could not be calculated");
    }

    return pass ? RebalancePreCheckerResult.pass("All rebalance parameters look good")
        : RebalancePreCheckerResult.warn(StringUtil.join("\n", warnings.toArray(String[]::new)));
  }

  private RebalancePreCheckerResult checkReplicaGroups(TableConfig tableConfig, RebalanceConfig rebalanceConfig) {
    String message;
    boolean hasAnyReplicaGroup;
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      message = "OFFLINE segments - " + getReplicaGroupInfo(tableConfig, InstancePartitionsType.OFFLINE.toString());
      hasAnyReplicaGroup = isReplicaGroupEnabled(tableConfig, InstancePartitionsType.OFFLINE.toString());
    } else {
      // for realtime table
      message =
          "COMPLETED segments - " + getReplicaGroupInfo(tableConfig, InstancePartitionsType.COMPLETED.toString()) + "\n"
              + "CONSUMING segments - " + getReplicaGroupInfo(tableConfig, InstancePartitionsType.CONSUMING.toString());
      hasAnyReplicaGroup =
          isReplicaGroupEnabled(tableConfig, InstancePartitionsType.COMPLETED.toString()) || isReplicaGroupEnabled(
              tableConfig, InstancePartitionsType.CONSUMING.toString());
    }
    String tierMessage = "";
    if (tableConfig.getTierConfigsList() != null) {
      List<String> tierMessageList = new ArrayList<>();
      for (TierConfig tierConfig : tableConfig.getTierConfigsList()) {
        tierMessageList.add(tierConfig.getName() + " tier - " + getReplicaGroupInfo(tableConfig, tierConfig.getName()));
        hasAnyReplicaGroup |= isReplicaGroupEnabled(tableConfig, tierConfig.getName());
      }
      tierMessage = "\n" + StringUtil.join("\n", tierMessageList.toArray(String[]::new));
    }
    if (hasAnyReplicaGroup && !rebalanceConfig.isReassignInstances()) {
      return RebalancePreCheckerResult.warn("reassignInstances is disabled, replica groups may not be updated.\n"
          + message + tierMessage);
    }
    return RebalancePreCheckerResult.pass(message + tierMessage);
  }

  private static boolean isReplicaGroupEnabled(TableConfig tableConfig, String typeOrTier) {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
    return instanceAssignmentConfigMap != null && instanceAssignmentConfigMap.containsKey(typeOrTier)
        && instanceAssignmentConfigMap.get(typeOrTier).getReplicaGroupPartitionConfig().isReplicaGroupBased();
  }

  private static String getReplicaGroupInfo(TableConfig tableConfig, String typeOrTier) {
    if (!isReplicaGroupEnabled(tableConfig, typeOrTier)) {
      return "Replica Groups are not enabled, replication: " + tableConfig.getReplication();
    }
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        instanceAssignmentConfigMap.get(typeOrTier).getReplicaGroupPartitionConfig();

    int numReplicaGroups = instanceReplicaGroupPartitionConfig.getNumReplicaGroups();
    int numInstancePerReplicaGroup = instanceReplicaGroupPartitionConfig.getNumInstancesPerReplicaGroup();
    if (numInstancePerReplicaGroup == 0) {
      return "numReplicaGroups: " + numReplicaGroups
          + ", numInstancesPerReplicaGroup: 0 (using as many instances as possible)";
    }
    return "numReplicaGroups: " + numReplicaGroups + ", numInstancesPerReplicaGroup: " + numInstancePerReplicaGroup;
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
