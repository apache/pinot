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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.ToIntFunction;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.common.tier.PinotServerTierStorage;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentDriver;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.helix.core.assignment.segment.StrictRealtimeSegmentAssignment;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code TableRebalancer} class can be used to rebalance a table (reassign instances and segments for a table).
 *
 * <p>Running the rebalancer in {@code dry-run} mode will only return the target instance and segment assignment without
 * applying any change to the cluster. This mode returns immediately.
 *
 * <p>If instance reassignment is enabled, the rebalancer will reassign the instances based on the instance assignment
 * config from the table config, persist the instance partitions if not in {@code dry-run} mode, and reassign segments
 * based on the new instance assignment. Otherwise, the rebalancer will skip the instance reassignment and reassign
 * segments based on the existing instance assignment.
 *
 * <p>For segment reassignment, 2 modes are offered:
 * <ul>
 *   <li>
 *     With-downtime rebalance: the IdealState is replaced with the target segment assignment in one go and there are no
 *     guarantees around replica availability. This mode returns immediately without waiting for ExternalView to reach
 *     the target segment assignment. Disabled tables will always be rebalanced with downtime.
 *   </li>
 *   <li>
 *     No-downtime rebalance: care is taken to ensure that the configured number of replicas of any segment are
 *     available (ONLINE or CONSUMING) at all times. The rebalancer tracks the number of segments to be offloaded from
 *     each instance and offload the segments from the most loaded instances first to ensure segments are not moved to
 *     the already over-loaded instances. This mode returns after ExternalView reaching the target segment assignment.
 *     <p>In the following edge case scenarios, if {@code best-efforts} is disabled, rebalancer will fail the rebalance
 *     because the no-downtime contract cannot be achieved, and table might end up in a middle stage. User needs to
 *     check the rebalance result, solve the issue, and run the rebalance again if necessary. If {@code best-efforts} is
 *     enabled, rebalancer will log a warning and continue the rebalance, but the no-downtime contract will not be
 *     guaranteed.
 *     <ul>
 *       <li>
 *         Segment falls into ERROR state in ExternalView -> with best-efforts, count ERROR state as good state
 *       </li>
 *       <li>
 *         ExternalView has not converged within the maximum wait time -> with best-efforts, continue to the next stage
 *       </li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>NOTE: If the controller that handles the rebalance goes down/restarted, the rebalance isn't automatically resumed
 * by other controllers.
 */
public class TableRebalancer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableRebalancer.class);
  private final HelixManager _helixManager;
  private final HelixDataAccessor _helixDataAccessor;
  private final TableRebalanceObserver _tableRebalanceObserver;
  private final ControllerMetrics _controllerMetrics;
  private final RebalancePreChecker _rebalancePreChecker;
  private final TableSizeReader _tableSizeReader;

  public TableRebalancer(HelixManager helixManager, @Nullable TableRebalanceObserver tableRebalanceObserver,
      @Nullable ControllerMetrics controllerMetrics, @Nullable RebalancePreChecker rebalancePreChecker,
      @Nullable TableSizeReader tableSizeReader) {
    _helixManager = helixManager;
    if (tableRebalanceObserver != null) {
      _tableRebalanceObserver = tableRebalanceObserver;
    } else {
      _tableRebalanceObserver = new NoOpTableRebalanceObserver();
    }
    _helixDataAccessor = helixManager.getHelixDataAccessor();
    _controllerMetrics = controllerMetrics;
    _rebalancePreChecker = rebalancePreChecker;
    _tableSizeReader = tableSizeReader;
  }

  public TableRebalancer(HelixManager helixManager) {
    this(helixManager, null, null, null, null);
  }

  public static String createUniqueRebalanceJobIdentifier() {
    return UUID.randomUUID().toString();
  }

  public RebalanceResult rebalance(TableConfig tableConfig, RebalanceConfig rebalanceConfig,
      @Nullable String rebalanceJobId) {
    return rebalance(tableConfig, rebalanceConfig, rebalanceJobId, null);
  }

  public RebalanceResult rebalance(TableConfig tableConfig, RebalanceConfig rebalanceConfig,
      @Nullable String rebalanceJobId, @Nullable Map<String, Set<String>> providedTierToSegmentsMap) {
    long startTime = System.currentTimeMillis();
    String tableNameWithType = tableConfig.getTableName();
    RebalanceResult.Status status = RebalanceResult.Status.UNKNOWN_ERROR;
    try {
      RebalanceResult result = doRebalance(tableConfig, rebalanceConfig, rebalanceJobId, providedTierToSegmentsMap);
      status = result.getStatus();
      return result;
    } finally {
      if (_controllerMetrics != null) {
        _controllerMetrics.addTimedTableValue(String.format("%s.%s", tableNameWithType, status.toString()),
            ControllerTimer.TABLE_REBALANCE_EXECUTION_TIME_MS, System.currentTimeMillis() - startTime,
            TimeUnit.MILLISECONDS);
      }
    }
  }

  private RebalanceResult doRebalance(TableConfig tableConfig, RebalanceConfig rebalanceConfig,
      @Nullable String rebalanceJobId, @Nullable Map<String, Set<String>> providedTierToSegmentsMap) {
    long startTimeMs = System.currentTimeMillis();
    String tableNameWithType = tableConfig.getTableName();
    if (rebalanceJobId == null) {
      // If not passed along, create one.
      // TODO - Add rebalanceJobId to all log messages for easy tracking.
      rebalanceJobId = createUniqueRebalanceJobIdentifier();
    }
    boolean dryRun = rebalanceConfig.isDryRun();
    boolean preChecks = rebalanceConfig.isPreChecks();
    boolean reassignInstances = rebalanceConfig.isReassignInstances();
    boolean includeConsuming = rebalanceConfig.isIncludeConsuming();
    boolean bootstrap = rebalanceConfig.isBootstrap();
    boolean downtime = rebalanceConfig.isDowntime();
    int minReplicasToKeepUpForNoDowntime = rebalanceConfig.getMinAvailableReplicas();
    boolean lowDiskMode = rebalanceConfig.isLowDiskMode();
    boolean bestEfforts = rebalanceConfig.isBestEfforts();
    long externalViewCheckIntervalInMs = rebalanceConfig.getExternalViewCheckIntervalInMs();
    long externalViewStabilizationTimeoutInMs = rebalanceConfig.getExternalViewStabilizationTimeoutInMs();
    Boolean minimizeDataMovement;
    switch (rebalanceConfig.getMinimizeDataMovement()) {
      case DEFAULT:
        minimizeDataMovement = null;
        break;
      case ENABLE:
        minimizeDataMovement = true;
        break;
      case DISABLE:
        minimizeDataMovement = false;
        break;
      default:
        throw new IllegalStateException(
            "Invalid minimizeDataMovement option: " + rebalanceConfig.getMinimizeDataMovement());
    }
    boolean enableStrictReplicaGroup = tableConfig.getRoutingConfig() != null
        && RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equalsIgnoreCase(
        tableConfig.getRoutingConfig().getInstanceSelectorType());
    LOGGER.info(
        "Start rebalancing table: {} with dryRun: {}, preChecks: {}, reassignInstances: {}, "
            + "includeConsuming: {}, bootstrap: {}, downtime: {}, minReplicasToKeepUpForNoDowntime: {}, "
            + "enableStrictReplicaGroup: {}, lowDiskMode: {}, bestEfforts: {}, externalViewCheckIntervalInMs: {}, "
            + "externalViewStabilizationTimeoutInMs: {}, minimizeDataMovement: {}",
        tableNameWithType, dryRun, preChecks, reassignInstances, includeConsuming, bootstrap, downtime,
        minReplicasToKeepUpForNoDowntime, enableStrictReplicaGroup, lowDiskMode, bestEfforts,
        externalViewCheckIntervalInMs, externalViewStabilizationTimeoutInMs, minimizeDataMovement);

    // Dry-run must be enabled to run pre-checks
    if (preChecks && !dryRun) {
      String errorMsg = String.format("Pre-checks can only be enabled in dry-run mode, not triggering rebalance for "
          + "table: %s with rebalanceJobId: %s", tableNameWithType, rebalanceJobId);
      LOGGER.error(errorMsg);
      return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED, errorMsg, null, null, null, null,
          null);
    }

    // Fetch ideal state
    PropertyKey idealStatePropertyKey = _helixDataAccessor.keyBuilder().idealStates(tableNameWithType);
    IdealState currentIdealState;
    try {
      currentIdealState = _helixDataAccessor.getProperty(idealStatePropertyKey);
    } catch (Exception e) {
      onReturnFailure(String.format(
          "For rebalanceId: %s, caught exception while fetching IdealState for table: %s, aborting the rebalance",
          rebalanceJobId, tableNameWithType), e);
      return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
          "Caught exception while fetching IdealState: " + e, null, null, null, null, null);
    }
    if (currentIdealState == null) {
      onReturnFailure(
          String.format("For rebalanceId: %s, cannot find the IdealState for table: %s, aborting the rebalance",
              rebalanceJobId, tableNameWithType), null);
      return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED, "Cannot find the IdealState for table",
          null, null, null, null, null);
    }
    if (!currentIdealState.isEnabled() && !downtime) {
      onReturnFailure(String.format(
          "For rebalanceId: %s, cannot rebalance disabled table: %s without downtime, aborting the rebalance",
          rebalanceJobId, tableNameWithType), null);
      return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
          "Cannot rebalance disabled table without downtime", null, null, null, null, null);
    }

    LOGGER.info("For rebalanceId: {}, processing instance partitions for table: {}", rebalanceJobId, tableNameWithType);

    // Calculate instance partitions map
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap;
    boolean instancePartitionsUnchanged;
    try {
      Pair<Map<InstancePartitionsType, InstancePartitions>, Boolean> instancePartitionsMapAndUnchanged =
          getInstancePartitionsMap(tableConfig, reassignInstances, bootstrap, dryRun, minimizeDataMovement);
      instancePartitionsMap = instancePartitionsMapAndUnchanged.getLeft();
      instancePartitionsUnchanged = instancePartitionsMapAndUnchanged.getRight();
    } catch (Exception e) {
      onReturnFailure(String.format(
          "For rebalanceId: %s, caught exception while fetching/calculating instance partitions for table: %s, "
              + "aborting the rebalance", rebalanceJobId, tableNameWithType), e);
      return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
          "Caught exception while fetching/calculating instance partitions: " + e, null, null, null, null,
          null);
    }

    // Calculate instance partitions for tiers if configured
    List<Tier> sortedTiers;
    Map<String, InstancePartitions> tierToInstancePartitionsMap;
    boolean tierInstancePartitionsUnchanged;
    try {
      sortedTiers = getSortedTiers(tableConfig, providedTierToSegmentsMap);
      Pair<Map<String, InstancePartitions>, Boolean> tierToInstancePartitionsMapAndUnchanged =
          getTierToInstancePartitionsMap(tableConfig, sortedTiers, reassignInstances, bootstrap, dryRun,
              minimizeDataMovement);
      tierToInstancePartitionsMap = tierToInstancePartitionsMapAndUnchanged.getLeft();
      tierInstancePartitionsUnchanged = tierToInstancePartitionsMapAndUnchanged.getRight();
    } catch (Exception e) {
      onReturnFailure(String.format(
          "For rebalanceId: %s, caught exception while fetching/calculating tier instance partitions for table: %s, "
              + "aborting the rebalance", rebalanceJobId, tableNameWithType), e);
      return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
          "Caught exception while fetching/calculating tier instance partitions: " + e, null,
          null, null, null, null);
    }

    LOGGER.info("For rebalanceId: {}, calculating the target assignment for table: {}", rebalanceJobId,
        tableNameWithType);
    SegmentAssignment segmentAssignment =
        SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig, _controllerMetrics);
    Map<String, Map<String, String>> currentAssignment = currentIdealState.getRecord().getMapFields();
    Map<String, Map<String, String>> targetAssignment;
    try {
      targetAssignment = segmentAssignment.rebalanceTable(currentAssignment, instancePartitionsMap, sortedTiers,
          tierToInstancePartitionsMap, rebalanceConfig);
    } catch (Exception e) {
      onReturnFailure(String.format(
          "For rebalanceId: %s, caught exception while calculating target assignment for table: %s, aborting the "
              + "rebalance", rebalanceJobId, tableNameWithType), e);
      return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
          "Caught exception while calculating target assignment: " + e, instancePartitionsMap,
          tierToInstancePartitionsMap, null, null, null);
    }

    boolean segmentAssignmentUnchanged = currentAssignment.equals(targetAssignment);
    LOGGER.info("For rebalanceId: {}, instancePartitionsUnchanged: {}, tierInstancePartitionsUnchanged: {}, "
            + "segmentAssignmentUnchanged: {} for table: {}", rebalanceJobId, instancePartitionsUnchanged,
        tierInstancePartitionsUnchanged, segmentAssignmentUnchanged, tableNameWithType);

    TableSizeReader.TableSubTypeSizeDetails tableSubTypeSizeDetails = fetchTableSizeDetails(tableNameWithType);

    Map<String, RebalancePreCheckerResult> preChecksResult = null;
    if (preChecks) {
      if (_rebalancePreChecker == null) {
        LOGGER.warn("Pre-checks are enabled but the pre-checker is not set, skipping pre-checks for table: {}",
            tableNameWithType);
      } else {
        RebalancePreChecker.PreCheckContext preCheckContext =
            new RebalancePreChecker.PreCheckContext(rebalanceJobId, tableNameWithType,
                tableConfig, currentAssignment, targetAssignment, tableSubTypeSizeDetails);
        preChecksResult = _rebalancePreChecker.check(preCheckContext);
      }
    }
    // Calculate summary here itself so that even if the table is already balanced, the caller can verify whether that
    // is expected or not based on the summary results
    RebalanceSummaryResult summaryResult =
        calculateDryRunSummary(currentAssignment, targetAssignment, tableNameWithType, rebalanceJobId,
            tableSubTypeSizeDetails);

    if (segmentAssignmentUnchanged) {
      LOGGER.info("Table: {} is already balanced", tableNameWithType);
      if (instancePartitionsUnchanged && tierInstancePartitionsUnchanged) {
        _tableRebalanceObserver.onNoop(
            String.format("For rebalanceId: %s, instance unchanged and table: %s is already balanced", rebalanceJobId,
                tableNameWithType));
        return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.NO_OP, "Table is already balanced",
            instancePartitionsMap, tierToInstancePartitionsMap, targetAssignment, preChecksResult, summaryResult);
      } else {
        if (dryRun) {
          return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.DONE,
              "Instance reassigned in dry-run mode, table is already balanced",
              instancePartitionsMap, tierToInstancePartitionsMap, targetAssignment, preChecksResult, summaryResult);
        } else {
          _tableRebalanceObserver.onSuccess(
              String.format("For rebalanceId: %s, instance reassigned but table: %s is already balanced",
                  rebalanceJobId, tableNameWithType));
          return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.DONE,
              "Instance reassigned, table is already balanced", instancePartitionsMap,
              tierToInstancePartitionsMap, targetAssignment, preChecksResult, summaryResult);
        }
      }
    }

    if (dryRun) {
      LOGGER.info("For rebalanceId: {}, rebalancing table: {} in dry-run mode, returning the target assignment",
          rebalanceJobId, tableNameWithType);
      return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.DONE, "Dry-run mode", instancePartitionsMap,
          tierToInstancePartitionsMap, targetAssignment, preChecksResult, summaryResult);
    }

    if (downtime) {
      LOGGER.info("For rebalanceId: {}, rebalancing table: {} with downtime", rebalanceJobId, tableNameWithType);

      // Reuse current IdealState to update the IdealState in cluster
      ZNRecord idealStateRecord = currentIdealState.getRecord();
      idealStateRecord.setMapFields(targetAssignment);
      currentIdealState.setNumPartitions(targetAssignment.size());
      currentIdealState.setReplicas(Integer.toString(targetAssignment.values().iterator().next().size()));

      // Check version and update IdealState
      try {
        Preconditions.checkState(_helixDataAccessor.getBaseDataAccessor()
            .set(idealStatePropertyKey.getPath(), idealStateRecord, idealStateRecord.getVersion(),
                AccessOption.PERSISTENT), "Failed to update IdealState");
        String msg =
            String.format("For rebalanceId: %s, finished rebalancing table: %s with downtime in %d ms.", rebalanceJobId,
                tableNameWithType, System.currentTimeMillis() - startTimeMs);
        LOGGER.info(msg);
        _tableRebalanceObserver.onSuccess(msg);
        return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.DONE,
            "Success with downtime (replaced IdealState with the target segment assignment, ExternalView might not "
                + "reach the target segment assignment yet)", instancePartitionsMap, tierToInstancePartitionsMap,
            targetAssignment, preChecksResult, summaryResult);
      } catch (Exception e) {
        onReturnFailure(String.format(
            "For rebalanceId: %s, caught exception while updating IdealState for table: %s, aborting the rebalance",
            rebalanceJobId, tableNameWithType), e);
        return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
            "Caught exception while updating IdealState: " + e, instancePartitionsMap, tierToInstancePartitionsMap,
            targetAssignment, preChecksResult, summaryResult);
      }
    }

    // Record the beginning of rebalance
    _tableRebalanceObserver.onTrigger(TableRebalanceObserver.Trigger.START_TRIGGER, currentAssignment,
        targetAssignment);

    // Calculate the min available replicas for no-downtime rebalance
    // NOTE:
    // 1. The calculation is based on the number of replicas of the target assignment. In case of increasing the number
    //    of replicas for the current assignment, the current instance state map might not have enough replicas to reach
    //    the minimum available replicas requirement. In this scenario we don't want to fail the check, but keep all the
    //    current instances as this is the best we can do, and can help the table get out of this state.
    // 2. Only check the segments to be moved because we don't need to maintain available replicas for segments not
    //    being moved, including segments with all replicas OFFLINE (error segments during consumption).
    List<String> segmentsToMove = SegmentAssignmentUtils.getSegmentsToMove(currentAssignment, targetAssignment);

    int numReplicas = Integer.MAX_VALUE;
    for (String segment : segmentsToMove) {
      numReplicas = Math.min(targetAssignment.get(segment).size(), numReplicas);
    }
    int minAvailableReplicas;
    if (minReplicasToKeepUpForNoDowntime >= 0) {
      // For non-negative value, use it as min available replicas
      if (minReplicasToKeepUpForNoDowntime >= numReplicas) {
        onReturnFailure(String.format(
            "For rebalanceId: %s, Illegal config for minReplicasToKeepUpForNoDowntime: %d for table: %s, "
                + "must be less than number of replicas: %d, aborting the rebalance", rebalanceJobId,
            minReplicasToKeepUpForNoDowntime, tableNameWithType, numReplicas), null);
        return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
            "Illegal min available replicas config", instancePartitionsMap, tierToInstancePartitionsMap,
            targetAssignment, preChecksResult, summaryResult);
      }
      minAvailableReplicas = minReplicasToKeepUpForNoDowntime;
    } else {
      // For negative value, use it as max unavailable replicas
      minAvailableReplicas = Math.max(numReplicas + minReplicasToKeepUpForNoDowntime, 0);
    }

    LOGGER.info(
        "For rebalanceId: {}, rebalancing table: {} with minAvailableReplicas: {}, enableStrictReplicaGroup: {}, "
            + "bestEfforts: {}, externalViewCheckIntervalInMs: {}, externalViewStabilizationTimeoutInMs: {}",
        rebalanceJobId, tableNameWithType, minAvailableReplicas, enableStrictReplicaGroup, bestEfforts,
        externalViewCheckIntervalInMs, externalViewStabilizationTimeoutInMs);
    int expectedVersion = currentIdealState.getRecord().getVersion();

    // We repeat the following steps until the target assignment is reached:
    // 1. Wait for ExternalView to converge with the IdealState. Fail the rebalance if it doesn't converge within the
    //    timeout.
    // 2. When IdealState changes during step 1, re-calculate the target assignment based on the new IdealState (current
    //    assignment).
    // 3. Check if the target assignment is reached. Rebalance is done if it is reached.
    // 4. Calculate the next assignment based on the current assignment, target assignment and min available replicas.
    // 5. Update the IdealState to the next assignment. If the IdealState changes before the update, go back to step 1.
    //
    // NOTE: Monitor the segments to be moved from both the previous round and this round to ensure the moved segments
    //       in the previous round are also converged.
    Set<String> segmentsToMonitor = new HashSet<>(segmentsToMove);
    while (true) {
      // Wait for ExternalView to converge before updating the next IdealState
      IdealState idealState;
      try {
        idealState = waitForExternalViewToConverge(tableNameWithType, lowDiskMode, bestEfforts, segmentsToMonitor,
            externalViewCheckIntervalInMs, externalViewStabilizationTimeoutInMs);
      } catch (Exception e) {
        String errorMsg = String.format(
            "For rebalanceId: %s, caught exception while waiting for ExternalView to converge for table: %s, "
                + "aborting the rebalance", rebalanceJobId, tableNameWithType);
        LOGGER.warn(errorMsg, e);
        if (_tableRebalanceObserver.isStopped()) {
          return new RebalanceResult(rebalanceJobId, _tableRebalanceObserver.getStopStatus(),
              "Caught exception while waiting for ExternalView to converge: " + e, instancePartitionsMap,
              tierToInstancePartitionsMap, targetAssignment, preChecksResult, summaryResult);
        }
        _tableRebalanceObserver.onError(errorMsg);
        return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
            "Caught exception while waiting for ExternalView to converge: " + e, instancePartitionsMap,
            tierToInstancePartitionsMap, targetAssignment, preChecksResult, summaryResult);
      }

      // Re-calculate the target assignment if IdealState changed while waiting for ExternalView to converge
      ZNRecord idealStateRecord = idealState.getRecord();
      if (idealStateRecord.getVersion() != expectedVersion) {
        LOGGER.info(
            "For rebalanceId: {}, idealState version changed while waiting for ExternalView to converge for table: {}, "
                + "re-calculating the target assignment", rebalanceJobId, tableNameWithType);
        Map<String, Map<String, String>> oldAssignment = currentAssignment;
        currentAssignment = idealStateRecord.getMapFields();
        expectedVersion = idealStateRecord.getVersion();

        // If all the segments to be moved remain unchanged (same instance state map) in the new ideal state, apply the
        // same target instance state map for these segments to the new ideal state as the target assignment
        boolean segmentsToMoveChanged = false;
        if (segmentAssignment instanceof StrictRealtimeSegmentAssignment) {
          // For StrictRealtimeSegmentAssignment, we need to recompute the target assignment because the assignment for
          // new added segments is based on the existing assignment
          segmentsToMoveChanged = true;
        } else {
          for (String segment : segmentsToMove) {
            Map<String, String> oldInstanceStateMap = oldAssignment.get(segment);
            Map<String, String> currentInstanceStateMap = currentAssignment.get(segment);
            // TODO: Consider allowing segment state change from CONSUMING to ONLINE
            if (!oldInstanceStateMap.equals(currentInstanceStateMap)) {
              LOGGER.info("For rebalanceId: {}, segment state changed in IdealState from: {} to: {} for table: {}, "
                      + "segment: {}, re-calculating the target assignment based on the new IdealState", rebalanceJobId,
                  oldInstanceStateMap, currentInstanceStateMap, tableNameWithType, segment);
              segmentsToMoveChanged = true;
              break;
            }
          }
        }
        if (segmentsToMoveChanged) {
          try {
            // Re-calculate the instance partitions in case the instance configs changed during the rebalance
            instancePartitionsMap =
                getInstancePartitionsMap(tableConfig, reassignInstances, bootstrap, false,
                    minimizeDataMovement).getLeft();
            tierToInstancePartitionsMap =
                getTierToInstancePartitionsMap(tableConfig, sortedTiers, reassignInstances, bootstrap, false,
                    minimizeDataMovement).getLeft();
            targetAssignment = segmentAssignment.rebalanceTable(currentAssignment, instancePartitionsMap, sortedTiers,
                tierToInstancePartitionsMap, rebalanceConfig);
          } catch (Exception e) {
            onReturnFailure(String.format(
                "For rebalanceId: %s, caught exception while re-calculating the target assignment for table: %s, "
                    + "aborting the rebalance", rebalanceJobId, tableNameWithType), e);
            return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
                "Caught exception while re-calculating the target assignment: " + e, instancePartitionsMap,
                tierToInstancePartitionsMap, targetAssignment, preChecksResult, summaryResult);
          }
        } else {
          LOGGER.info("For rebalanceId:{}, no state change found for segments to be moved, "
                  + "re-calculating the target assignment based on the previous target assignment for table: {}",
              rebalanceJobId, tableNameWithType);
          Map<String, Map<String, String>> oldTargetAssignment = targetAssignment;
          targetAssignment = new HashMap<>(currentAssignment);
          for (String segment : segmentsToMove) {
            targetAssignment.put(segment, oldTargetAssignment.get(segment));
          }
        }
      }

      if (currentAssignment.equals(targetAssignment)) {
        String msg = String.format("For rebalanceId: %s, finished rebalancing table: %s with minAvailableReplicas: %d, "
                + "enableStrictReplicaGroup: %b, bestEfforts: %b in %d ms.", rebalanceJobId, tableNameWithType,
            minAvailableReplicas, enableStrictReplicaGroup, bestEfforts, System.currentTimeMillis() - startTimeMs);
        LOGGER.info(msg);
        // Record completion
        _tableRebalanceObserver.onSuccess(msg);
        return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.DONE,
            "Success with minAvailableReplicas: " + minAvailableReplicas
                + " (both IdealState and ExternalView should reach the target segment assignment)",
            instancePartitionsMap, tierToInstancePartitionsMap, targetAssignment, preChecksResult, summaryResult);
      }

      // Record change of current ideal state and the new target
      _tableRebalanceObserver.onTrigger(TableRebalanceObserver.Trigger.IDEAL_STATE_CHANGE_TRIGGER, currentAssignment,
          targetAssignment);
      if (_tableRebalanceObserver.isStopped()) {
        return new RebalanceResult(rebalanceJobId, _tableRebalanceObserver.getStopStatus(),
            "Rebalance has stopped already before updating the IdealState", instancePartitionsMap,
            tierToInstancePartitionsMap, targetAssignment, preChecksResult, summaryResult);
      }
      Map<String, Map<String, String>> nextAssignment =
          getNextAssignment(currentAssignment, targetAssignment, minAvailableReplicas, enableStrictReplicaGroup,
              lowDiskMode);
      LOGGER.info("For rebalanceId: {}, got the next assignment for table: {} with number of segments to be "
              + "added/removed for each instance: {}", rebalanceJobId, tableNameWithType,
          SegmentAssignmentUtils.getNumSegmentsToMovePerInstance(currentAssignment, nextAssignment));

      // Reuse current IdealState to update the IdealState in cluster
      idealStateRecord.setMapFields(nextAssignment);
      idealState.setNumPartitions(nextAssignment.size());
      idealState.setReplicas(Integer.toString(nextAssignment.values().iterator().next().size()));

      // Check version and update IdealState
      try {
        Preconditions.checkState(_helixDataAccessor.getBaseDataAccessor()
                .set(idealStatePropertyKey.getPath(), idealStateRecord, expectedVersion, AccessOption.PERSISTENT),
            "Failed to update IdealState");
        currentAssignment = nextAssignment;
        expectedVersion++;
        LOGGER.info("For rebalanceId: {}, successfully updated the IdealState for table: {}", rebalanceJobId,
            tableNameWithType);
      } catch (ZkBadVersionException e) {
        LOGGER.info("For rebalanceId: {}, version changed while updating IdealState for table: {}", rebalanceJobId,
            tableNameWithType);
      } catch (Exception e) {
        onReturnFailure(String.format("For rebalanceId: %s, caught exception while updating IdealState for table: %s, "
            + "aborting the rebalance", rebalanceJobId, tableNameWithType), e);
        return new RebalanceResult(rebalanceJobId, RebalanceResult.Status.FAILED,
            "Caught exception while updating IdealState: " + e, instancePartitionsMap, tierToInstancePartitionsMap,
            targetAssignment, preChecksResult, summaryResult);
      }

      segmentsToMonitor = new HashSet<>(segmentsToMove);
      segmentsToMove = SegmentAssignmentUtils.getSegmentsToMove(currentAssignment, targetAssignment);
      segmentsToMonitor.addAll(segmentsToMove);
    }
  }

  private TableSizeReader.TableSubTypeSizeDetails fetchTableSizeDetails(String tableNameWithType) {
    if (_tableSizeReader == null) {
      LOGGER.warn("tableSizeReader is null, cannot calculate table size for table {}!", tableNameWithType);
      return null;
    }
    LOGGER.info("Fetching the table size for table: {}", tableNameWithType);
    try {
      // TODO: Consider making the timeoutMs for fetching table size via table rebalancer configurable
      TableSizeReader.TableSubTypeSizeDetails sizeDetails =
          _tableSizeReader.getTableSubtypeSize(tableNameWithType, 30_000);
      LOGGER.info("Fetched the table size details for table: {}", tableNameWithType);
      return sizeDetails;
    } catch (InvalidConfigException e) {
      LOGGER.error("Caught exception while trying to fetch table size details for table: {}", tableNameWithType, e);
    }
    return null;
  }

  private long calculateTableSizePerReplicaInBytes(TableSizeReader.TableSubTypeSizeDetails tableSizeDetails) {
    return tableSizeDetails == null ? -1 : tableSizeDetails._reportedSizePerReplicaInBytes;
  }

  private RebalanceSummaryResult calculateDryRunSummary(Map<String, Map<String, String>> currentAssignment,
      Map<String, Map<String, String>> targetAssignment, String tableNameWithType, String rebalanceJobId,
      TableSizeReader.TableSubTypeSizeDetails tableSubTypeSizeDetails) {
    LOGGER.info("Calculating rebalance summary for table: {} with rebalanceJobId: {}",
        tableNameWithType, rebalanceJobId);
    int existingReplicationFactor = 0;
    int newReplicationFactor = 0;
    Map<String, Set<String>> existingServersToSegmentMap = new HashMap<>();
    Map<String, Set<String>> newServersToSegmentMap = new HashMap<>();

    for (Map.Entry<String, Map<String, String>> entrySet : currentAssignment.entrySet()) {
      existingReplicationFactor = entrySet.getValue().size();
      for (String segmentKey : entrySet.getValue().keySet()) {
        existingServersToSegmentMap.computeIfAbsent(segmentKey, k -> new HashSet<>()).add(entrySet.getKey());
      }
    }

    for (Map.Entry<String, Map<String, String>> entrySet : targetAssignment.entrySet()) {
      newReplicationFactor = entrySet.getValue().size();
      for (String segmentKey : entrySet.getValue().keySet()) {
        newServersToSegmentMap.computeIfAbsent(segmentKey, k -> new HashSet<>()).add(entrySet.getKey());
      }
    }
    RebalanceSummaryResult.RebalanceChangeInfo replicationFactor
        = new RebalanceSummaryResult.RebalanceChangeInfo(existingReplicationFactor, newReplicationFactor);

    int existingNumServers = existingServersToSegmentMap.size();
    int newNumServers = newServersToSegmentMap.size();
    RebalanceSummaryResult.RebalanceChangeInfo numServers
        = new RebalanceSummaryResult.RebalanceChangeInfo(existingNumServers, newNumServers);

    List<InstanceConfig> instanceConfigs = _helixDataAccessor.getChildValues(
        _helixDataAccessor.keyBuilder().instanceConfigs(), true);
    Map<String, List<String>> instanceToTagsMap = new HashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      instanceToTagsMap.put(instanceConfig.getInstanceName(), instanceConfig.getTags());
    }

    Set<String> serversAdded = new HashSet<>();
    Set<String> serversRemoved = new HashSet<>();
    Set<String> serversUnchanged = new HashSet<>();
    Set<String> serversGettingNewSegments = new HashSet<>();
    Map<String, RebalanceSummaryResult.ServerSegmentChangeInfo> serverSegmentChangeInfoMap = new HashMap<>();
    int segmentsNotMoved = 0;
    int maxSegmentsAddedToServer = 0;
    for (Map.Entry<String, Set<String>> entry : newServersToSegmentMap.entrySet()) {
      String server = entry.getKey();
      Set<String> segmentSet = entry.getValue();
      int totalNewSegments = segmentSet.size();

      Set<String> newSegmentSet = new HashSet<>(segmentSet);
      Set<String> existingSegmentSet = new HashSet<>();
      int segmentsUnchanged = 0;
      int totalExistingSegments = 0;
      RebalanceSummaryResult.ServerStatus serverStatus = RebalanceSummaryResult.ServerStatus.ADDED;
      if (existingServersToSegmentMap.containsKey(server)) {
        Set<String> segmentSetForServer = existingServersToSegmentMap.get(server);
        totalExistingSegments = segmentSetForServer.size();
        existingSegmentSet.addAll(segmentSetForServer);
        Set<String> intersection = new HashSet<>(segmentSetForServer);
        intersection.retainAll(newSegmentSet);
        segmentsUnchanged = intersection.size();
        segmentsNotMoved += segmentsUnchanged;
        serverStatus = RebalanceSummaryResult.ServerStatus.UNCHANGED;
        serversUnchanged.add(server);
      } else {
        serversAdded.add(server);
      }
      newSegmentSet.removeAll(existingSegmentSet);
      int segmentsAdded = newSegmentSet.size();
      if (segmentsAdded > 0) {
        serversGettingNewSegments.add(server);
      }
      maxSegmentsAddedToServer = Math.max(maxSegmentsAddedToServer, segmentsAdded);
      int segmentsDeleted = existingSegmentSet.size() - segmentsUnchanged;

      serverSegmentChangeInfoMap.put(server, new RebalanceSummaryResult.ServerSegmentChangeInfo(serverStatus,
          totalNewSegments, totalExistingSegments, segmentsAdded, segmentsDeleted, segmentsUnchanged,
          instanceToTagsMap.getOrDefault(server, null)));
    }

    for (Map.Entry<String, Set<String>> entry : existingServersToSegmentMap.entrySet()) {
      String server = entry.getKey();
      if (!serverSegmentChangeInfoMap.containsKey(server)) {
        serversRemoved.add(server);
        serverSegmentChangeInfoMap.put(server, new RebalanceSummaryResult.ServerSegmentChangeInfo(
            RebalanceSummaryResult.ServerStatus.REMOVED, 0, entry.getValue().size(), 0, entry.getValue().size(), 0,
            instanceToTagsMap.getOrDefault(server, null)));
      }
    }

    RebalanceSummaryResult.RebalanceChangeInfo numSegmentsInSingleReplica
        = new RebalanceSummaryResult.RebalanceChangeInfo(currentAssignment.size(), targetAssignment.size());

    int existingNumberSegmentsTotal = existingReplicationFactor * currentAssignment.size();
    int newNumberSegmentsTotal = newReplicationFactor * targetAssignment.size();
    RebalanceSummaryResult.RebalanceChangeInfo numSegmentsAcrossAllReplicas
        = new RebalanceSummaryResult.RebalanceChangeInfo(existingNumberSegmentsTotal, newNumberSegmentsTotal);

    int totalSegmentsToBeMoved = newNumberSegmentsTotal - segmentsNotMoved;

    long tableSizePerReplicaInBytes = calculateTableSizePerReplicaInBytes(tableSubTypeSizeDetails);
    long averageSegmentSizeInBytes = tableSizePerReplicaInBytes <= 0 ? tableSizePerReplicaInBytes
        : tableSizePerReplicaInBytes / ((long) currentAssignment.size());
    long totalEstimatedDataToBeMovedInBytes = tableSizePerReplicaInBytes <= 0 ? tableSizePerReplicaInBytes
        : ((long) totalSegmentsToBeMoved) * averageSegmentSizeInBytes;

    // Set some of the sets to null if they are empty to ensure they don't show up in the result
    RebalanceSummaryResult.ServerInfo serverInfo = new RebalanceSummaryResult.ServerInfo(
        serversGettingNewSegments.size(), numServers, serversAdded, serversRemoved, serversUnchanged,
        serversGettingNewSegments, serverSegmentChangeInfoMap);
    // TODO: Add a metric to estimate the total time it will take to rebalance. Need some good heuristics on how
    //       rebalance time can vary with number of segments added
    RebalanceSummaryResult.SegmentInfo segmentInfo = new RebalanceSummaryResult.SegmentInfo(totalSegmentsToBeMoved,
        maxSegmentsAddedToServer, averageSegmentSizeInBytes, totalEstimatedDataToBeMovedInBytes,
        replicationFactor, numSegmentsInSingleReplica, numSegmentsAcrossAllReplicas);

    LOGGER.info("Calculated rebalance summary for table: {} with rebalanceJobId: {}", tableNameWithType,
        rebalanceJobId);
    return new RebalanceSummaryResult(serverInfo, segmentInfo);
  }

  private void onReturnFailure(String errorMsg, Exception e) {
    if (e != null) {
      LOGGER.warn(errorMsg, e);
    } else {
      LOGGER.warn(errorMsg);
    }
    _tableRebalanceObserver.onError(errorMsg);
  }

  /**
   * Gets the instance partitions for instance partition types and also returns a boolean for whether they are unchanged
   */
  public Pair<Map<InstancePartitionsType, InstancePartitions>, Boolean> getInstancePartitionsMap(
      TableConfig tableConfig, boolean reassignInstances, boolean bootstrap, boolean dryRun) {
    return getInstancePartitionsMap(tableConfig, reassignInstances, bootstrap, dryRun, false);
  }

  public Pair<Map<InstancePartitionsType, InstancePartitions>, Boolean> getInstancePartitionsMap(
      TableConfig tableConfig, boolean reassignInstances, boolean bootstrap, boolean dryRun,
      @Nullable Boolean minimizeDataMovement) {
    boolean instancePartitionsUnchanged;
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      Pair<InstancePartitions, Boolean> partitionAndUnchangedForOffline =
          getInstancePartitions(tableConfig, InstancePartitionsType.OFFLINE, reassignInstances, bootstrap, dryRun,
              minimizeDataMovement);
      instancePartitionsMap.put(InstancePartitionsType.OFFLINE, partitionAndUnchangedForOffline.getLeft());
      instancePartitionsUnchanged = partitionAndUnchangedForOffline.getRight();
    } else {
      Pair<InstancePartitions, Boolean> partitionAndUnchangedForConsuming =
          getInstancePartitions(tableConfig, InstancePartitionsType.CONSUMING, reassignInstances, bootstrap, dryRun,
              minimizeDataMovement);
      instancePartitionsMap.put(InstancePartitionsType.CONSUMING, partitionAndUnchangedForConsuming.getLeft());
      instancePartitionsUnchanged = partitionAndUnchangedForConsuming.getRight();
      String tableNameWithType = tableConfig.getTableName();
      if (InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig)) {
        Pair<InstancePartitions, Boolean> partitionAndUnchangedForCompleted =
            getInstancePartitions(tableConfig, InstancePartitionsType.COMPLETED, reassignInstances, bootstrap, dryRun,
                minimizeDataMovement);
        LOGGER.info(
            "COMPLETED segments should be relocated, fetching/computing COMPLETED instance partitions for table: {}",
            tableNameWithType);
        instancePartitionsMap.put(InstancePartitionsType.COMPLETED, partitionAndUnchangedForCompleted.getLeft());
        instancePartitionsUnchanged &= partitionAndUnchangedForCompleted.getRight();
      } else {
        LOGGER.info(
            "COMPLETED segments should not be relocated, skipping fetching/computing COMPLETED instance partitions "
                + "for table: {}", tableNameWithType);
        if (!dryRun) {
          String instancePartitionsName = InstancePartitionsUtils.getInstancePartitionsName(tableNameWithType,
              InstancePartitionsType.COMPLETED.toString());
          LOGGER.info("Removing instance partitions: {} from ZK if it exists", instancePartitionsName);
          InstancePartitionsUtils.removeInstancePartitions(_helixManager.getHelixPropertyStore(),
              instancePartitionsName);
        }
      }
    }
    return Pair.of(instancePartitionsMap, instancePartitionsUnchanged);
  }

  /**
   * Fetches/computes the instance partitions and also returns a boolean for whether they are unchanged
   */
  private Pair<InstancePartitions, Boolean> getInstancePartitions(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType, boolean reassignInstances, boolean bootstrap, boolean dryRun,
      @Nullable Boolean minimizeDataMovement) {
    String tableNameWithType = tableConfig.getTableName();
    String instancePartitionsName =
        InstancePartitionsUtils.getInstancePartitionsName(tableNameWithType, instancePartitionsType.toString());
    InstancePartitions existingInstancePartitions =
        InstancePartitionsUtils.fetchInstancePartitions(_helixManager.getHelixPropertyStore(), instancePartitionsName);

    if (reassignInstances) {
      if (InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, instancePartitionsType)) {
        boolean hasPreConfiguredInstancePartitions =
            TableConfigUtils.hasPreConfiguredInstancePartitions(tableConfig, instancePartitionsType);
        boolean isPreConfigurationBasedAssignment =
            InstanceAssignmentConfigUtils.isMirrorServerSetAssignment(tableConfig, instancePartitionsType);
        InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(tableConfig);
        InstancePartitions instancePartitions;
        boolean instancePartitionsUnchanged;
        if (!hasPreConfiguredInstancePartitions) {
          LOGGER.info("Reassigning {} instances for table: {}", instancePartitionsType, tableNameWithType);
          // Assign instances with existing instance partition to null if bootstrap mode is enabled, so that the
          // instance partition map can be fully recalculated.
          instancePartitions = instanceAssignmentDriver.assignInstances(instancePartitionsType,
              _helixDataAccessor.getChildValues(_helixDataAccessor.keyBuilder().instanceConfigs(), true),
              bootstrap ? null : existingInstancePartitions, minimizeDataMovement);
          instancePartitionsUnchanged = instancePartitions.equals(existingInstancePartitions);
          if (!dryRun && !instancePartitionsUnchanged) {
            LOGGER.info("Persisting instance partitions: {} to ZK", instancePartitions);
            InstancePartitionsUtils.persistInstancePartitions(_helixManager.getHelixPropertyStore(),
                instancePartitions);
          }
        } else {
          String referenceInstancePartitionsName = tableConfig.getInstancePartitionsMap().get(instancePartitionsType);
          if (isPreConfigurationBasedAssignment) {
            InstancePartitions preConfiguredInstancePartitions =
                InstancePartitionsUtils.fetchInstancePartitionsWithRename(_helixManager.getHelixPropertyStore(),
                    referenceInstancePartitionsName, instancePartitionsName);
            instancePartitions = instanceAssignmentDriver.assignInstances(instancePartitionsType,
                _helixDataAccessor.getChildValues(_helixDataAccessor.keyBuilder().instanceConfigs(), true),
                bootstrap ? null : existingInstancePartitions, preConfiguredInstancePartitions,
                minimizeDataMovement);
            instancePartitionsUnchanged = instancePartitions.equals(existingInstancePartitions);
            if (!dryRun && !instancePartitionsUnchanged) {
              LOGGER.info("Persisting instance partitions: {} (based on {})", instancePartitions,
                  preConfiguredInstancePartitions);
              InstancePartitionsUtils.persistInstancePartitions(_helixManager.getHelixPropertyStore(),
                  instancePartitions);
            }
          } else {
            instancePartitions =
                InstancePartitionsUtils.fetchInstancePartitionsWithRename(_helixManager.getHelixPropertyStore(),
                    referenceInstancePartitionsName, instancePartitionsName);
            instancePartitionsUnchanged = instancePartitions.equals(existingInstancePartitions);
            if (!dryRun && !instancePartitionsUnchanged) {
              LOGGER.info("Persisting instance partitions: {} (referencing {})", instancePartitions,
                  referenceInstancePartitionsName);
              InstancePartitionsUtils.persistInstancePartitions(_helixManager.getHelixPropertyStore(),
                  instancePartitions);
            }
          }
        }
        return Pair.of(instancePartitions, instancePartitionsUnchanged);
      } else {
        LOGGER.info("{} instance assignment is not allowed, using default instance partitions for table: {}",
            instancePartitionsType, tableNameWithType);
        InstancePartitions instancePartitions =
            InstancePartitionsUtils.computeDefaultInstancePartitions(_helixManager, tableConfig,
                instancePartitionsType);
        boolean noExistingInstancePartitions = existingInstancePartitions == null;
        if (!dryRun && !noExistingInstancePartitions) {
          LOGGER.info("Removing instance partitions: {} from ZK", instancePartitionsName);
          InstancePartitionsUtils.removeInstancePartitions(_helixManager.getHelixPropertyStore(),
              instancePartitionsName);
        }
        return Pair.of(instancePartitions, noExistingInstancePartitions);
      }
    } else {
      LOGGER.info("Fetching/computing {} instance partitions for table: {}", instancePartitionsType, tableNameWithType);
      return Pair.of(
          InstancePartitionsUtils.fetchOrComputeInstancePartitions(_helixManager, tableConfig, instancePartitionsType),
          true);
    }
  }

  @Nullable
  private List<Tier> getSortedTiers(TableConfig tableConfig,
      @Nullable Map<String, Set<String>> providedTierToSegmentsMap) {
    List<TierConfig> tierConfigs = tableConfig.getTierConfigsList();
    if (CollectionUtils.isNotEmpty(tierConfigs)) {
      // Get tiers with storageType = "PINOT_SERVER". This is the only type available right now.
      // Other types should be treated differently
      return TierConfigUtils.getSortedTiersForStorageType(tierConfigs, TierFactory.PINOT_SERVER_STORAGE_TYPE,
          _helixManager, providedTierToSegmentsMap);
    } else {
      return null;
    }
  }

  /**
   * Fetches/computes the instance partitions for sorted tiers and also returns a boolean for whether the
   * instance partitions are unchanged.
   */
  private Pair<Map<String, InstancePartitions>, Boolean> getTierToInstancePartitionsMap(TableConfig tableConfig,
      @Nullable List<Tier> sortedTiers, boolean reassignInstances, boolean bootstrap, boolean dryRun,
      @Nullable Boolean minimizeDataMovement) {
    if (sortedTiers == null) {
      return Pair.of(null, true);
    }
    boolean instancePartitionsUnchanged = true;
    Map<String, InstancePartitions> tierToInstancePartitionsMap = new HashMap<>();
    for (Tier tier : sortedTiers) {
      LOGGER.info("Fetching/computing instance partitions for tier: {} of table: {}", tier.getName(),
          tableConfig.getTableName());
      Pair<InstancePartitions, Boolean> partitionsAndUnchanged =
          getInstancePartitionsForTier(tableConfig, tier, reassignInstances, bootstrap, dryRun,
              minimizeDataMovement);
      tierToInstancePartitionsMap.put(tier.getName(), partitionsAndUnchanged.getLeft());
      instancePartitionsUnchanged = instancePartitionsUnchanged && partitionsAndUnchanged.getRight();
    }
    return Pair.of(tierToInstancePartitionsMap, instancePartitionsUnchanged);
  }

  /**
   * Computes the instance partitions for the given tier. If table's instanceAssignmentConfigMap has an entry for the
   * tier, it's used to calculate the instance partitions. Else default instance partitions are returned. Also returns
   * a boolean for whether the instance partition is unchanged.
   */
  private Pair<InstancePartitions, Boolean> getInstancePartitionsForTier(TableConfig tableConfig, Tier tier,
      boolean reassignInstances, boolean bootstrap, boolean dryRun, @Nullable Boolean minimizeDataMovement) {
    String tableNameWithType = tableConfig.getTableName();
    String tierName = tier.getName();
    String instancePartitionsName =
        InstancePartitionsUtils.getInstancePartitionsNameForTier(tableNameWithType, tierName);
    InstancePartitions existingInstancePartitions =
        InstancePartitionsUtils.fetchInstancePartitions(_helixManager.getHelixPropertyStore(), instancePartitionsName);

    if (reassignInstances) {
      Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
      InstanceAssignmentConfig instanceAssignmentConfig =
          instanceAssignmentConfigMap != null ? instanceAssignmentConfigMap.get(tierName) : null;
      if (instanceAssignmentConfig == null) {
        LOGGER.info(
            "Instance assignment config for tier: {} does not exist for table: {}, using default instance partitions",
            tierName, tableNameWithType);
        PinotServerTierStorage storage = (PinotServerTierStorage) tier.getStorage();
        InstancePartitions instancePartitions =
            InstancePartitionsUtils.computeDefaultInstancePartitionsForTag(_helixManager, tableConfig, tierName,
                storage.getServerTag());
        boolean noExistingInstancePartitions = existingInstancePartitions == null;
        if (!dryRun && !noExistingInstancePartitions) {
          LOGGER.info("Removing instance partitions: {} from ZK", instancePartitionsName);
          InstancePartitionsUtils.removeInstancePartitions(_helixManager.getHelixPropertyStore(),
              instancePartitionsName);
        }
        return Pair.of(instancePartitions, noExistingInstancePartitions);
      } else {
        InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(tableConfig);
        // Assign instances with existing instance partition to null if bootstrap mode is enabled, so that the instance
        // partition map can be fully recalculated.
        InstancePartitions instancePartitions = instanceAssignmentDriver.assignInstances(tierName,
            _helixDataAccessor.getChildValues(_helixDataAccessor.keyBuilder().instanceConfigs(), true),
            bootstrap ? null : existingInstancePartitions, instanceAssignmentConfig, minimizeDataMovement);
        boolean instancePartitionsUnchanged = instancePartitions.equals(existingInstancePartitions);
        if (!dryRun && !instancePartitionsUnchanged) {
          LOGGER.info("Persisting instance partitions: {} to ZK", instancePartitions);
          InstancePartitionsUtils.persistInstancePartitions(_helixManager.getHelixPropertyStore(), instancePartitions);
        }
        return Pair.of(instancePartitions, instancePartitionsUnchanged);
      }
    } else {
      if (existingInstancePartitions != null) {
        return Pair.of(existingInstancePartitions, true);
      } else {
        PinotServerTierStorage storage = (PinotServerTierStorage) tier.getStorage();
        InstancePartitions instancePartitions =
            InstancePartitionsUtils.computeDefaultInstancePartitionsForTag(_helixManager, tableConfig, tierName,
                storage.getServerTag());
        return Pair.of(instancePartitions, true);
      }
    }
  }

  private IdealState waitForExternalViewToConverge(String tableNameWithType, boolean lowDiskMode, boolean bestEfforts,
      Set<String> segmentsToMonitor, long externalViewCheckIntervalInMs, long externalViewStabilizationTimeoutInMs)
      throws InterruptedException, TimeoutException {
    long endTimeMs = System.currentTimeMillis() + externalViewStabilizationTimeoutInMs;

    IdealState idealState;
    do {
      LOGGER.debug("Start to check if ExternalView converges to IdealStates");
      idealState = _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().idealStates(tableNameWithType));
      // IdealState might be null if table got deleted, throwing exception to abort the rebalance
      Preconditions.checkState(idealState != null, "Failed to find the IdealState");

      ExternalView externalView =
          _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().externalView(tableNameWithType));
      // ExternalView might be null when table is just created, skipping check for this iteration
      if (externalView != null) {
        // Record external view and ideal state convergence status
        _tableRebalanceObserver.onTrigger(
            TableRebalanceObserver.Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER,
            externalView.getRecord().getMapFields(), idealState.getRecord().getMapFields());
        if (_tableRebalanceObserver.isStopped()) {
          throw new RuntimeException(
              String.format("Rebalance for table: %s has already stopped with status: %s", tableNameWithType,
                  _tableRebalanceObserver.getStopStatus()));
        }
        if (isExternalViewConverged(tableNameWithType, externalView.getRecord().getMapFields(),
            idealState.getRecord().getMapFields(), lowDiskMode, bestEfforts, segmentsToMonitor)) {
          LOGGER.info("ExternalView converged for table: {}", tableNameWithType);
          return idealState;
        }
      }
      LOGGER.debug("ExternalView has not converged to IdealStates. Retry after: {}ms", externalViewCheckIntervalInMs);
      Thread.sleep(externalViewCheckIntervalInMs);
    } while (System.currentTimeMillis() < endTimeMs);

    if (bestEfforts) {
      LOGGER.warn("ExternalView has not converged within: {}ms for table: {}, continuing the rebalance (best-efforts)",
          externalViewStabilizationTimeoutInMs, tableNameWithType);
      return idealState;
    } else {
      throw new TimeoutException(String.format("ExternalView has not converged within: %d ms for table: %s",
          externalViewStabilizationTimeoutInMs, tableNameWithType));
    }
  }

  /**
   * NOTE:
   * Only check the segments in the IdealState and being monitored. Extra segments in ExternalView are ignored because
   * they are not managed by the rebalancer.
   * For each segment checked:
   * - In regular mode, it is okay to have extra instances in ExternalView as long as the instance states in IdealState
   *   are reached.
   * - In low disk mode, instance states in ExternalView must match IdealState to ensure the segments are deleted from
   *   server before moving to the next assignment.
   * For ERROR state in ExternalView, if using best-efforts, log a warning and treat it as good state; if not, throw an
   * exception to abort the rebalance because we are not able to get out of the ERROR state.
   */
  @VisibleForTesting
  static boolean isExternalViewConverged(String tableNameWithType,
      Map<String, Map<String, String>> externalViewSegmentStates,
      Map<String, Map<String, String>> idealStateSegmentStates, boolean lowDiskMode, boolean bestEfforts,
      @Nullable Set<String> segmentsToMonitor) {
    for (Map.Entry<String, Map<String, String>> entry : idealStateSegmentStates.entrySet()) {
      String segmentName = entry.getKey();
      if (segmentsToMonitor != null && !segmentsToMonitor.contains(segmentName)) {
        continue;
      }
      Map<String, String> externalViewInstanceStateMap = externalViewSegmentStates.get(segmentName);
      Map<String, String> idealStateInstanceStateMap = entry.getValue();

      for (Map.Entry<String, String> instanceStateEntry : idealStateInstanceStateMap.entrySet()) {
        // Ignore OFFLINE state in IdealState
        String idealStateInstanceState = instanceStateEntry.getValue();
        if (idealStateInstanceState.equals(SegmentStateModel.OFFLINE)) {
          continue;
        }

        // ExternalView should contain the segment
        if (externalViewInstanceStateMap == null) {
          return false;
        }

        // Check whether the instance state in ExternalView matches the IdealState
        String instanceName = instanceStateEntry.getKey();
        String externalViewInstanceState = externalViewInstanceStateMap.get(instanceName);
        if (!idealStateInstanceState.equals(externalViewInstanceState)) {
          if (SegmentStateModel.ERROR.equals(externalViewInstanceState)) {
            handleErrorInstance(tableNameWithType, segmentName, instanceName, bestEfforts);
          } else {
            return false;
          }
        }
      }

      // For low disk mode, check if there are extra instances in ExternalView that are not in IdealState
      if (lowDiskMode && externalViewInstanceStateMap != null) {
        for (Map.Entry<String, String> instanceStateEntry : externalViewInstanceStateMap.entrySet()) {
          String instanceName = instanceStateEntry.getKey();
          if (idealStateInstanceStateMap.containsKey(instanceName)) {
            continue;
          }
          if (SegmentStateModel.ERROR.equals(instanceStateEntry.getValue())) {
            handleErrorInstance(tableNameWithType, segmentName, instanceName, bestEfforts);
          } else {
            return false;
          }
        }
      }
    }
    return true;
  }

  private static void handleErrorInstance(String tableNameWithType, String segmentName, String instanceName,
      boolean bestEfforts) {
    if (bestEfforts) {
      LOGGER.warn("Found ERROR instance: {} for segment: {}, table: {}, counting it as good state (best-efforts)",
          instanceName, segmentName, tableNameWithType);
    } else {
      LOGGER.warn("Found ERROR instance: {} for segment: {}, table: {}", instanceName, segmentName, tableNameWithType);
      throw new IllegalStateException("Found segments in ERROR state");
    }
  }

  /**
   * Returns the next assignment for the table based on the current assignment and the target assignment with regard to
   * the minimum available replicas requirement. For strict replica-group mode, track the available instances for all
   * the segments with the same instances in the next assignment, and ensure the minimum available replicas requirement
   * is met. If adding the assignment for a segment breaks the requirement, use the current assignment for the segment.
   */
  @VisibleForTesting
  static Map<String, Map<String, String>> getNextAssignment(Map<String, Map<String, String>> currentAssignment,
      Map<String, Map<String, String>> targetAssignment, int minAvailableReplicas, boolean enableStrictReplicaGroup,
      boolean lowDiskMode) {
    return enableStrictReplicaGroup ? getNextStrictReplicaGroupAssignment(currentAssignment, targetAssignment,
        minAvailableReplicas, lowDiskMode)
        : getNextNonStrictReplicaGroupAssignment(currentAssignment, targetAssignment, minAvailableReplicas,
            lowDiskMode);
  }

  private static Map<String, Map<String, String>> getNextStrictReplicaGroupAssignment(
      Map<String, Map<String, String>> currentAssignment, Map<String, Map<String, String>> targetAssignment,
      int minAvailableReplicas, boolean lowDiskMode) {
    Map<String, Map<String, String>> nextAssignment = new TreeMap<>();
    Map<String, Integer> numSegmentsToOffloadMap = getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    Map<Pair<Set<String>, Set<String>>, Set<String>> assignmentMap = new HashMap<>();
    Map<Set<String>, Set<String>> availableInstancesMap = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> currentInstanceStateMap = entry.getValue();
      Map<String, String> targetInstanceStateMap = targetAssignment.get(segmentName);
      SingleSegmentAssignment assignment =
          getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, minAvailableReplicas,
              lowDiskMode, numSegmentsToOffloadMap, assignmentMap);
      Set<String> assignedInstances = assignment._instanceStateMap.keySet();
      Set<String> availableInstances = assignment._availableInstances;
      availableInstancesMap.compute(assignedInstances, (k, currentAvailableInstances) -> {
        if (currentAvailableInstances == null) {
          // First segment assigned to these instances, use the new assignment and update the available instances
          nextAssignment.put(segmentName, assignment._instanceStateMap);
          updateNumSegmentsToOffloadMap(numSegmentsToOffloadMap, currentInstanceStateMap.keySet(), k);
          return availableInstances;
        } else {
          // There are other segments assigned to the same instances, check the available instances to see if adding the
          // new assignment can still hold the minimum available replicas requirement
          availableInstances.retainAll(currentAvailableInstances);
          if (availableInstances.size() >= minAvailableReplicas) {
            // New assignment can be added
            nextAssignment.put(segmentName, assignment._instanceStateMap);
            updateNumSegmentsToOffloadMap(numSegmentsToOffloadMap, currentInstanceStateMap.keySet(), k);
            return availableInstances;
          } else {
            // New assignment cannot be added, use the current instance state map
            nextAssignment.put(segmentName, currentInstanceStateMap);
            return currentAvailableInstances;
          }
        }
      });
    }
    return nextAssignment;
  }

  private static Map<String, Map<String, String>> getNextNonStrictReplicaGroupAssignment(
      Map<String, Map<String, String>> currentAssignment, Map<String, Map<String, String>> targetAssignment,
      int minAvailableReplicas, boolean lowDiskMode) {
    Map<String, Map<String, String>> nextAssignment = new TreeMap<>();
    Map<String, Integer> numSegmentsToOffloadMap = getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    Map<Pair<Set<String>, Set<String>>, Set<String>> assignmentMap = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> currentInstanceStateMap = entry.getValue();
      Map<String, String> targetInstanceStateMap = targetAssignment.get(segmentName);
      Map<String, String> nextInstanceStateMap =
          getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, minAvailableReplicas,
              lowDiskMode, numSegmentsToOffloadMap, assignmentMap)._instanceStateMap;
      nextAssignment.put(segmentName, nextInstanceStateMap);
      updateNumSegmentsToOffloadMap(numSegmentsToOffloadMap, currentInstanceStateMap.keySet(),
          nextInstanceStateMap.keySet());
    }
    return nextAssignment;
  }

  /**
   * Returns the map from instance to number of segments to be offloaded from the instance based on the current and
   * target assignment.
   */
  @VisibleForTesting
  static Map<String, Integer> getNumSegmentsToOffloadMap(Map<String, Map<String, String>> currentAssignment,
      Map<String, Map<String, String>> targetAssignment) {
    Map<String, Integer> numSegmentsToOffloadMap = new HashMap<>();
    for (Map<String, String> currentInstanceStateMap : currentAssignment.values()) {
      for (String currentInstance : currentInstanceStateMap.keySet()) {
        numSegmentsToOffloadMap.merge(currentInstance, 1, Integer::sum);
      }
    }
    for (Map<String, String> targetInstanceStateMap : targetAssignment.values()) {
      for (String targetInstance : targetInstanceStateMap.keySet()) {
        numSegmentsToOffloadMap.merge(targetInstance, -1, Integer::sum);
      }
    }
    return numSegmentsToOffloadMap;
  }

  private static void updateNumSegmentsToOffloadMap(Map<String, Integer> numSegmentsToOffloadMap,
      Set<String> currentInstances, Set<String> newInstances) {
    for (String currentInstance : currentInstances) {
      numSegmentsToOffloadMap.merge(currentInstance, -1, Integer::sum);
    }
    for (String newInstance : newInstances) {
      numSegmentsToOffloadMap.merge(newInstance, 1, Integer::sum);
    }
  }

  /**
   * Returns the next assignment for a segment based on the current instance state map and the target instance state map
   * with regard to the minimum available replicas requirement.
   * It is possible that the current instance state map does not have enough replicas to reach the minimum available
   * replicas requirement, and in this scenario we will keep all the current instances as this is the best we can do.
   */
  @VisibleForTesting
  static SingleSegmentAssignment getNextSingleSegmentAssignment(Map<String, String> currentInstanceStateMap,
      Map<String, String> targetInstanceStateMap, int minAvailableReplicas, boolean lowDiskMode,
      Map<String, Integer> numSegmentsToOffloadMap, Map<Pair<Set<String>, Set<String>>, Set<String>> assignmentMap) {
    Map<String, String> nextInstanceStateMap = new TreeMap<>();

    // Assign the segment the same way as other segments if the current and target instances are the same. We need this
    // to guarantee the mirror servers for replica-group based routing strategies.
    Set<String> currentInstances = currentInstanceStateMap.keySet();
    Set<String> targetInstances = targetInstanceStateMap.keySet();
    Pair<Set<String>, Set<String>> assignmentKey = Pair.of(currentInstances, targetInstances);
    Set<String> instancesToAssign = assignmentMap.get(assignmentKey);
    if (instancesToAssign != null) {
      Set<String> availableInstances = new TreeSet<>();
      for (String instanceName : instancesToAssign) {
        String currentInstanceState = currentInstanceStateMap.get(instanceName);
        String targetInstanceState = targetInstanceStateMap.get(instanceName);
        if (currentInstanceState != null) {
          availableInstances.add(instanceName);
          // Use target instance state if available in case the state changes
          nextInstanceStateMap.put(instanceName,
              targetInstanceState != null ? targetInstanceState : currentInstanceState);
        } else {
          nextInstanceStateMap.put(instanceName, targetInstanceState);
        }
      }
      return new SingleSegmentAssignment(nextInstanceStateMap, availableInstances);
    }

    // Add all the common instances
    // Use target instance state in case the state changes
    for (Map.Entry<String, String> entry : targetInstanceStateMap.entrySet()) {
      String instanceName = entry.getKey();
      if (currentInstanceStateMap.containsKey(instanceName)) {
        nextInstanceStateMap.put(instanceName, entry.getValue());
      }
    }

    // Add current instances until the min available replicas achieved
    int numInstancesToKeep = minAvailableReplicas - nextInstanceStateMap.size();
    if (numInstancesToKeep > 0) {
      // Sort instances by number of segments to offload, and keep the ones with the least segments to offload
      List<Triple<String, String, Integer>> instancesInfo =
          getSortedInstancesOnNumSegmentsToOffload(currentInstanceStateMap, nextInstanceStateMap,
              numSegmentsToOffloadMap);
      numInstancesToKeep = Integer.min(numInstancesToKeep, instancesInfo.size());
      for (int i = 0; i < numInstancesToKeep; i++) {
        Triple<String, String, Integer> instanceInfo = instancesInfo.get(i);
        nextInstanceStateMap.put(instanceInfo.getLeft(), instanceInfo.getMiddle());
      }
    }
    Set<String> availableInstances = new TreeSet<>(nextInstanceStateMap.keySet());

    // After achieving the min available replicas, when low disk mode is enabled, only add new instances when all
    // current instances exist in the next assignment.
    // We want to first drop the extra instances as one step, then add the target instances as another step to avoid the
    // case where segments are first added to the instance before other segments are dropped from the instance, which
    // might cause server running out of disk. Note that even if segment addition and drop happen in the same step,
    // there is no guarantee that server process the segment drop before the segment addition.
    if (!lowDiskMode || currentInstanceStateMap.size() == nextInstanceStateMap.size()) {
      int numInstancesToAdd = targetInstanceStateMap.size() - nextInstanceStateMap.size();
      if (numInstancesToAdd > 0) {
        // Sort instances by number of segments to offload, and add the ones with the least segments to offload
        List<Triple<String, String, Integer>> instancesInfo =
            getSortedInstancesOnNumSegmentsToOffload(targetInstanceStateMap, nextInstanceStateMap,
                numSegmentsToOffloadMap);
        for (int i = 0; i < numInstancesToAdd; i++) {
          Triple<String, String, Integer> instanceInfo = instancesInfo.get(i);
          nextInstanceStateMap.put(instanceInfo.getLeft(), instanceInfo.getMiddle());
        }
      }
    }

    assignmentMap.put(assignmentKey, nextInstanceStateMap.keySet());
    return new SingleSegmentAssignment(nextInstanceStateMap, availableInstances);
  }

  /**
   * Returns the sorted instances by number of segments to offload. If there is a tie, sort the instances in
   * alphabetical order to get deterministic result.
   * The Triple stores {@code <instanceName, instanceState, numSegmentsToOffload>}.
   */
  private static List<Triple<String, String, Integer>> getSortedInstancesOnNumSegmentsToOffload(
      Map<String, String> instanceStateMap, Map<String, String> nextInstanceStateMap,
      Map<String, Integer> numSegmentsToOffloadMap) {
    List<Triple<String, String, Integer>> instancesInfo = new ArrayList<>(instanceStateMap.size());
    for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
      String instanceName = entry.getKey();
      if (!nextInstanceStateMap.containsKey(instanceName)) {
        instancesInfo.add(Triple.of(instanceName, entry.getValue(), numSegmentsToOffloadMap.get(instanceName)));
      }
    }
    instancesInfo.sort(Comparator.comparingInt((ToIntFunction<Triple<String, String, Integer>>) Triple::getRight)
        .thenComparing(Triple::getLeft));
    return instancesInfo;
  }

  /**
   * Assignment result for a single segment.
   */
  @VisibleForTesting
  static class SingleSegmentAssignment {
    final Map<String, String> _instanceStateMap;
    // Instances that are common in both current instance state and next instance state of the segment
    final Set<String> _availableInstances;

    SingleSegmentAssignment(Map<String, String> instanceStateMap, Set<String> availableInstances) {
      _instanceStateMap = instanceStateMap;
      _availableInstances = availableInstances;
    }
  }
}
