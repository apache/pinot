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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;
import java.util.function.ToIntFunction;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.tier.PinotServerTierStorage;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentDriver;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
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

  // TODO: make them configurable
  private static final long EXTERNAL_VIEW_CHECK_INTERVAL_MS = 1_000L; // 1 second
  private static final long EXTERNAL_VIEW_STABILIZATION_MAX_WAIT_MS = 60 * 60_000L; // 1 hour

  private final HelixManager _helixManager;
  private final HelixDataAccessor _helixDataAccessor;

  public TableRebalancer(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixDataAccessor = helixManager.getHelixDataAccessor();
  }

  public RebalanceResult rebalance(TableConfig tableConfig, Configuration rebalanceConfig) {
    long startTimeMs = System.currentTimeMillis();
    String tableNameWithType = tableConfig.getTableName();

    boolean dryRun =
        rebalanceConfig.getBoolean(RebalanceConfigConstants.DRY_RUN, RebalanceConfigConstants.DEFAULT_DRY_RUN);
    boolean reassignInstances = rebalanceConfig.getBoolean(RebalanceConfigConstants.REASSIGN_INSTANCES,
        RebalanceConfigConstants.DEFAULT_REASSIGN_INSTANCES);
    boolean includeConsuming = rebalanceConfig.getBoolean(RebalanceConfigConstants.INCLUDE_CONSUMING,
        RebalanceConfigConstants.DEFAULT_INCLUDE_CONSUMING);
    boolean bootstrap =
        rebalanceConfig.getBoolean(RebalanceConfigConstants.BOOTSTRAP, RebalanceConfigConstants.DEFAULT_BOOTSTRAP);
    boolean downtime =
        rebalanceConfig.getBoolean(RebalanceConfigConstants.DOWNTIME, RebalanceConfigConstants.DEFAULT_DOWNTIME);
    int minReplicasToKeepUpForNoDowntime =
        rebalanceConfig.getInt(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME,
            RebalanceConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME);
    boolean enableStrictReplicaGroup = tableConfig.getRoutingConfig() != null
        && RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equalsIgnoreCase(
        tableConfig.getRoutingConfig().getInstanceSelectorType());
    boolean bestEfforts = rebalanceConfig.getBoolean(RebalanceConfigConstants.BEST_EFFORTS,
        RebalanceConfigConstants.DEFAULT_BEST_EFFORTS);
    LOGGER.info(
        "Start rebalancing table: {} with dryRun: {}, reassignInstances: {}, includeConsuming: {}, bootstrap: {}, "
            + "downtime: {}, minReplicasToKeepUpForNoDowntime: {}, enableStrictReplicaGroup: {}, bestEfforts: {}",
        tableNameWithType, dryRun, reassignInstances, includeConsuming, bootstrap, downtime,
        minReplicasToKeepUpForNoDowntime, enableStrictReplicaGroup, bestEfforts);

    // Validate table config
    try {
      // Do not allow rebalancing HLC real-time table
      if (tableConfig.getTableType() == TableType.REALTIME && new StreamConfig(tableNameWithType,
          IngestionConfigUtils.getStreamConfigMap(tableConfig)).hasHighLevelConsumerType()) {
        LOGGER.warn("Cannot rebalance table: {} with high-level consumer, aborting the rebalance", tableNameWithType);
        return new RebalanceResult(RebalanceResult.Status.FAILED, "Cannot rebalance table with high-level consumer",
            null, null);
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception while validating table config for table: {}, aborting the rebalance",
          tableNameWithType, e);
      return new RebalanceResult(RebalanceResult.Status.FAILED, "Caught exception while validating table config: " + e,
          null, null);
    }

    // Fetch ideal state
    PropertyKey idealStatePropertyKey = _helixDataAccessor.keyBuilder().idealStates(tableNameWithType);
    IdealState currentIdealState;
    try {
      currentIdealState = _helixDataAccessor.getProperty(idealStatePropertyKey);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while fetching IdealState for table: {}, aborting the rebalance", tableNameWithType,
          e);
      return new RebalanceResult(RebalanceResult.Status.FAILED, "Caught exception while fetching IdealState: " + e,
          null, null);
    }
    if (currentIdealState == null) {
      LOGGER.warn("Cannot find the IdealState for table: {}, aborting the rebalance", tableNameWithType);
      return new RebalanceResult(RebalanceResult.Status.FAILED, "Cannot find the IdealState for table", null, null);
    }
    if (!currentIdealState.isEnabled() && !downtime) {
      LOGGER.warn("Cannot rebalance disabled table: {} without downtime, aborting the rebalance", tableNameWithType);
      return new RebalanceResult(RebalanceResult.Status.FAILED, "Cannot rebalance disabled table without downtime",
          null, null);
    }

    LOGGER.info("Fetching/computing instance partitions, reassigning instances if configured for table: {}",
        tableNameWithType);

    // get instancePartitions for tiers
    Map<String, InstancePartitions> tierToInstancePartitionMap = null;
    List<Tier> sortedTiers = null;
    if (TierConfigUtils.shouldRelocateToTiers(tableConfig)) {
      // get tiers with storageType = "PINOT_SERVER". This is the only type available right now.
      // Other types should be treated differently
      sortedTiers = TierConfigUtils.getSortedTiersForStorageType(tableConfig.getTierConfigsList(),
          TierFactory.PINOT_SERVER_STORAGE_TYPE, _helixManager);

      tierToInstancePartitionMap = new HashMap<>();
      for (Tier tier : sortedTiers) {
        LOGGER.info("Fetching/computing instance partitions for tier: {} of table: {}", tier.getName(),
            tableNameWithType);
        tierToInstancePartitionMap.put(tier.getName(), getInstancePartitionsForTier(tier, tableNameWithType));
      }
    }

    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();
    try {
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        instancePartitionsMap.put(InstancePartitionsType.OFFLINE,
            getInstancePartitions(tableConfig, InstancePartitionsType.OFFLINE, reassignInstances, dryRun));
      } else {
        instancePartitionsMap.put(InstancePartitionsType.CONSUMING,
            getInstancePartitions(tableConfig, InstancePartitionsType.CONSUMING, reassignInstances, dryRun));
        if (InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig)) {
          LOGGER.info(
              "COMPLETED segments should be relocated, fetching/computing COMPLETED instance partitions for table: {}",
              tableNameWithType);
          instancePartitionsMap.put(InstancePartitionsType.COMPLETED,
              getInstancePartitions(tableConfig, InstancePartitionsType.COMPLETED, reassignInstances, dryRun));
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
    } catch (Exception e) {
      LOGGER.warn(
          "Caught exception while fetching/calculating instance partitions for table: {}, aborting the rebalance",
          tableNameWithType, e);
      return new RebalanceResult(RebalanceResult.Status.FAILED,
          "Caught exception while fetching/calculating instance partitions: " + e, null, null);
    }

    LOGGER.info("Calculating the target assignment for table: {}", tableNameWithType);
    SegmentAssignment segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig);
    Map<String, Map<String, String>> currentAssignment = currentIdealState.getRecord().getMapFields();
    Map<String, Map<String, String>> targetAssignment;
    try {
      targetAssignment = segmentAssignment.rebalanceTable(currentAssignment, instancePartitionsMap, sortedTiers,
          tierToInstancePartitionMap, rebalanceConfig);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while calculating target assignment for table: {}, aborting the rebalance",
          tableNameWithType, e);
      return new RebalanceResult(RebalanceResult.Status.FAILED,
          "Caught exception while calculating target assignment: " + e, instancePartitionsMap, null);
    }

    if (currentAssignment.equals(targetAssignment)) {
      LOGGER.info("Table: {} is already balanced", tableNameWithType);
      if (reassignInstances) {
        if (dryRun) {
          return new RebalanceResult(RebalanceResult.Status.DONE,
              "Instance reassigned in dry-run mode, table is already balanced", instancePartitionsMap,
              targetAssignment);
        } else {
          return new RebalanceResult(RebalanceResult.Status.DONE, "Instance reassigned, table is already balanced",
              instancePartitionsMap, targetAssignment);
        }
      } else {
        return new RebalanceResult(RebalanceResult.Status.NO_OP, "Table is already balanced", instancePartitionsMap,
            targetAssignment);
      }
    }

    if (dryRun) {
      LOGGER.info("Rebalancing table: {} in dry-run mode, returning the target assignment", tableNameWithType);
      return new RebalanceResult(RebalanceResult.Status.DONE, "Dry-run mode", instancePartitionsMap, targetAssignment);
    }

    if (downtime) {
      LOGGER.info("Rebalancing table: {} with downtime", tableNameWithType);

      while (true) {
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
          LOGGER.info("Finished rebalancing table: {} with downtime in {}ms.", tableNameWithType,
              System.currentTimeMillis() - startTimeMs);
          return new RebalanceResult(RebalanceResult.Status.DONE,
              "Success with downtime (replaced IdealState with the target segment assignment, ExternalView might not "
                  + "reach the target segment assignment yet)", instancePartitionsMap, targetAssignment);
        } catch (ZkBadVersionException e) {
          LOGGER.info("IdealState version changed for table: {}, re-calculating the target assignment",
              tableNameWithType);
          try {
            IdealState idealState = _helixDataAccessor.getProperty(idealStatePropertyKey);
            // IdealState might be null if table got deleted, throwing exception to abort the rebalance
            Preconditions.checkState(idealState != null, "Failed to find the IdealState");
            currentIdealState = idealState;
            currentAssignment = currentIdealState.getRecord().getMapFields();
            targetAssignment = segmentAssignment.rebalanceTable(currentAssignment, instancePartitionsMap, sortedTiers,
                tierToInstancePartitionMap, rebalanceConfig);
          } catch (Exception e1) {
            LOGGER.warn(
                "Caught exception while re-calculating the target assignment for table: {}, aborting the rebalance",
                tableNameWithType, e1);
            return new RebalanceResult(RebalanceResult.Status.FAILED,
                "Caught exception while re-calculating the target assignment: " + e1, instancePartitionsMap,
                targetAssignment);
          }
        } catch (Exception e) {
          LOGGER.warn("Caught exception while updating IdealState for table: {}, aborting the rebalance",
              tableNameWithType, e);
          return new RebalanceResult(RebalanceResult.Status.FAILED, "Caught exception while updating IdealState: " + e,
              instancePartitionsMap, targetAssignment);
        }
      }
    }

    // Calculate the min available replicas for no-downtime rebalance
    // NOTE: The calculation is based on the number of replicas of the target assignment. In case of increasing the
    //       number of replicas for the current assignment, the current instance state map might not have enough
    //       replicas to reach the minimum available replicas requirement. In this scenario we don't want to fail the
    //       check, but keep all the current instances as this is the best we can do, and can help the table get out of
    //       this state.
    int numReplicas = Integer.MAX_VALUE;
    for (Map<String, String> instanceStateMap : targetAssignment.values()) {
      numReplicas = Math.min(instanceStateMap.size(), numReplicas);
    }
    int minAvailableReplicas;
    if (minReplicasToKeepUpForNoDowntime >= 0) {
      // For non-negative value, use it as min available replicas
      if (minReplicasToKeepUpForNoDowntime >= numReplicas) {
        LOGGER.warn(
            "Illegal config for minReplicasToKeepUpForNoDowntime: {} for table: {}, must be less than number of "
                + "replicas: {}, aborting the rebalance", minReplicasToKeepUpForNoDowntime, tableNameWithType,
            numReplicas);
        return new RebalanceResult(RebalanceResult.Status.FAILED, "Illegal min available replicas config",
            instancePartitionsMap, targetAssignment);
      }
      minAvailableReplicas = minReplicasToKeepUpForNoDowntime;
    } else {
      // For negative value, use it as max unavailable replicas
      minAvailableReplicas = Math.max(numReplicas + minReplicasToKeepUpForNoDowntime, 0);
    }

    LOGGER.info("Rebalancing table: {} with minAvailableReplicas: {}, enableStrictReplicaGroup: {}, bestEfforts: {}",
        tableNameWithType, minAvailableReplicas, enableStrictReplicaGroup, bestEfforts);
    int expectedVersion = currentIdealState.getRecord().getVersion();
    while (true) {
      // Wait for ExternalView to converge before updating the next IdealState
      IdealState idealState;
      try {
        idealState = waitForExternalViewToConverge(tableNameWithType, bestEfforts);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while waiting for ExternalView to converge for table: {}, aborting the rebalance",
            tableNameWithType, e);
        return new RebalanceResult(RebalanceResult.Status.FAILED,
            "Caught exception while waiting for ExternalView to converge: " + e, instancePartitionsMap,
            targetAssignment);
      }

      // Re-calculate the target assignment if IdealState changed while waiting for ExternalView to converge
      if (idealState.getRecord().getVersion() != expectedVersion) {
        LOGGER.info(
            "IdealState version changed while waiting for ExternalView to converge for table: {}, re-calculating the "
                + "target assignment", tableNameWithType);
        try {
          currentIdealState = idealState;
          currentAssignment = currentIdealState.getRecord().getMapFields();
          targetAssignment = segmentAssignment.rebalanceTable(currentAssignment, instancePartitionsMap, sortedTiers,
              tierToInstancePartitionMap, rebalanceConfig);
          expectedVersion = currentIdealState.getRecord().getVersion();
        } catch (Exception e) {
          LOGGER.warn(
              "Caught exception while re-calculating the target assignment for table: {}, aborting the rebalance",
              tableNameWithType, e);
          return new RebalanceResult(RebalanceResult.Status.FAILED,
              "Caught exception while re-calculating the target assignment: " + e, instancePartitionsMap,
              targetAssignment);
        }
      }

      if (currentAssignment.equals(targetAssignment)) {
        LOGGER.info(
            "Finished rebalancing table: {} with minAvailableReplicas: {}, enableStrictReplicaGroup: {}, bestEfforts:"
                + " {} in {}ms.", tableNameWithType, minAvailableReplicas, enableStrictReplicaGroup, bestEfforts,
            System.currentTimeMillis() - startTimeMs);
        return new RebalanceResult(RebalanceResult.Status.DONE,
            "Success with minAvailableReplicas: " + minAvailableReplicas
                + " (both IdealState and ExternalView should reach the target segment assignment)",
            instancePartitionsMap, targetAssignment);
      }

      Map<String, Map<String, String>> nextAssignment =
          getNextAssignment(currentAssignment, targetAssignment, minAvailableReplicas, enableStrictReplicaGroup);
      LOGGER.info("Got the next assignment for table: {} with number of segments to be moved to each instance: {}",
          tableNameWithType,
          SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, nextAssignment));

      // Reuse current IdealState to update the IdealState in cluster
      ZNRecord idealStateRecord = currentIdealState.getRecord();
      idealStateRecord.setMapFields(nextAssignment);
      currentIdealState.setNumPartitions(nextAssignment.size());
      currentIdealState.setReplicas(Integer.toString(nextAssignment.values().iterator().next().size()));

      // Check version and update IdealState
      try {
        Preconditions.checkState(_helixDataAccessor.getBaseDataAccessor()
                .set(idealStatePropertyKey.getPath(), idealStateRecord, expectedVersion, AccessOption.PERSISTENT),
            "Failed to update IdealState");
        currentAssignment = nextAssignment;
        expectedVersion++;
        LOGGER.info("Successfully updated the IdealState for table: {}", tableNameWithType);
      } catch (ZkBadVersionException e) {
        LOGGER.info("Version changed while updating IdealState for table: {}", tableNameWithType);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while updating IdealState for table: {}, aborting the rebalance",
            tableNameWithType, e);
        return new RebalanceResult(RebalanceResult.Status.FAILED, "Caught exception while updating IdealState: " + e,
            instancePartitionsMap, targetAssignment);
      }
    }
  }

  private InstancePartitions getInstancePartitions(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType, boolean reassignInstances, boolean dryRun) {
    String tableNameWithType = tableConfig.getTableName();
    if (InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, instancePartitionsType)) {
      if (reassignInstances) {
        LOGGER.info("Reassigning {} instances for table: {}", instancePartitionsType, tableNameWithType);
        InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(tableConfig);
        InstancePartitions instancePartitions = instanceAssignmentDriver.assignInstances(instancePartitionsType,
            _helixDataAccessor.getChildValues(_helixDataAccessor.keyBuilder().instanceConfigs(), true));
        if (!dryRun) {
          LOGGER.info("Persisting instance partitions: {} to ZK", instancePartitions);
          InstancePartitionsUtils.persistInstancePartitions(_helixManager.getHelixPropertyStore(), instancePartitions);
        }
        return instancePartitions;
      } else {
        LOGGER.info("Fetching/computing {} instance partitions for table: {}", instancePartitionsType,
            tableNameWithType);
        return InstancePartitionsUtils.fetchOrComputeInstancePartitions(_helixManager, tableConfig,
            instancePartitionsType);
      }
    } else {
      LOGGER.info("{} instance assignment is not allowed, using default instance partitions for table: {}",
          instancePartitionsType, tableNameWithType);
      if (reassignInstances) {
        LOGGER.warn("Cannot reassign {} instances (instance assignment is not allowed) for table: {}",
            instancePartitionsType, tableNameWithType);
      }
      InstancePartitions instancePartitions =
          InstancePartitionsUtils.computeDefaultInstancePartitions(_helixManager, tableConfig, instancePartitionsType);
      if (!dryRun) {
        String instancePartitionsName = instancePartitions.getInstancePartitionsName();
        LOGGER.info("Removing instance partitions: {} from ZK if it exists", instancePartitionsName);
        InstancePartitionsUtils.removeInstancePartitions(_helixManager.getHelixPropertyStore(), instancePartitionsName);
      }
      return instancePartitions;
    }
  }

  /**
   * Creates a default instance assignment for the tier.
   * TODO: We only support default server-tag based assignment currently.
   *  In next iteration, we will add InstanceAssignmentConfig to the TierConfig and also support persisting of the
   *  InstancePartitions to zk.
   *  Then we'll be able to support replica group assignment while creating InstancePartitions for tiers
   */
  private InstancePartitions getInstancePartitionsForTier(Tier tier, String tableNameWithType) {
    PinotServerTierStorage storage = (PinotServerTierStorage) tier.getStorage();
    return InstancePartitionsUtils.computeDefaultInstancePartitionsForTag(_helixManager, tableNameWithType,
        tier.getName(), storage.getTag());
  }

  private IdealState waitForExternalViewToConverge(String tableNameWithType, boolean bestEfforts)
      throws InterruptedException, TimeoutException {
    long endTimeMs = System.currentTimeMillis() + EXTERNAL_VIEW_STABILIZATION_MAX_WAIT_MS;

    IdealState idealState;
    do {
      idealState = _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().idealStates(tableNameWithType));
      // IdealState might be null if table got deleted, throwing exception to abort the rebalance
      Preconditions.checkState(idealState != null, "Failed to find the IdealState");

      ExternalView externalView =
          _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().externalView(tableNameWithType));
      // ExternalView might be null when table is just created, skipping check for this iteration
      if (externalView != null) {
        if (isExternalViewConverged(tableNameWithType, externalView.getRecord().getMapFields(),
            idealState.getRecord().getMapFields(), bestEfforts)) {
          LOGGER.info("ExternalView converged for table: {}", tableNameWithType);
          return idealState;
        }
      }

      Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
    } while (System.currentTimeMillis() < endTimeMs);

    if (bestEfforts) {
      LOGGER.warn("ExternalView has not converged within: {}ms for table: {}, continuing the rebalance (best-efforts)",
          EXTERNAL_VIEW_STABILIZATION_MAX_WAIT_MS, tableNameWithType);
      return idealState;
    } else {
      throw new TimeoutException("Timeout while waiting for ExternalView to converge");
    }
  }

  /**
   * NOTE: Only check the segments and instances in the IdealState. It is okay to have extra segments or instances in
   * ExternalView as long as the instance states for all the segments in IdealState are reached. For ERROR state in
   * ExternalView, if using best-efforts, log a warning and treat it as good state; if not, throw an exception to abort
   * the rebalance because we are not able to get out of the ERROR state.
   */
  @VisibleForTesting
  static boolean isExternalViewConverged(String tableNameWithType,
      Map<String, Map<String, String>> externalViewSegmentStates,
      Map<String, Map<String, String>> idealStateSegmentStates, boolean bestEfforts) {
    for (Map.Entry<String, Map<String, String>> entry : idealStateSegmentStates.entrySet()) {
      String segmentName = entry.getKey();
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
            if (bestEfforts) {
              LOGGER.warn(
                  "Found ERROR instance: {} for segment: {}, table: {}, counting it as good state (best-efforts)",
                  instanceName, segmentName, tableNameWithType);
            } else {
              LOGGER.warn("Found ERROR instance: {} for segment: {}, table: {}", instanceName, segmentName,
                  tableNameWithType);
              throw new IllegalStateException("Found segments in ERROR state");
            }
          } else {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Returns the next assignment for the table based on the current assignment and the target assignment with regard to
   * the minimum available replicas requirement. For strict replica-group mode, track the available instances for all
   * the segments with the same instances in the next assignment, and ensure the minimum available replicas requirement
   * is met. If adding the assignment for a segment breaks the requirement, use the current assignment for the segment.
   */
  @VisibleForTesting
  static Map<String, Map<String, String>> getNextAssignment(Map<String, Map<String, String>> currentAssignment,
      Map<String, Map<String, String>> targetAssignment, int minAvailableReplicas, boolean enableStrictReplicaGroup) {
    return enableStrictReplicaGroup ? getNextStrictReplicaGroupAssignment(currentAssignment, targetAssignment,
        minAvailableReplicas)
        : getNextNonStrictReplicaGroupAssignment(currentAssignment, targetAssignment, minAvailableReplicas);
  }

  private static Map<String, Map<String, String>> getNextStrictReplicaGroupAssignment(
      Map<String, Map<String, String>> currentAssignment, Map<String, Map<String, String>> targetAssignment,
      int minAvailableReplicas) {
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
              numSegmentsToOffloadMap, assignmentMap);
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
      int minAvailableReplicas) {
    Map<String, Map<String, String>> nextAssignment = new TreeMap<>();
    Map<String, Integer> numSegmentsToOffloadMap = getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    Map<Pair<Set<String>, Set<String>>, Set<String>> assignmentMap = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> currentInstanceStateMap = entry.getValue();
      Map<String, String> targetInstanceStateMap = targetAssignment.get(segmentName);
      Map<String, String> nextInstanceStateMap =
          getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, minAvailableReplicas,
              numSegmentsToOffloadMap, assignmentMap)._instanceStateMap;
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
      Map<String, String> targetInstanceStateMap, int minAvailableReplicas,
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

    // Add target instances until the number of instances matched
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
