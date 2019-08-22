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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsType;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.instance.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentDriver;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that handles instance and segment reassignment.
 * <p>Running the rebalancer in {@code dry-run} mode will only return the target instance and segment assignment without
 * applying any change to the cluster. This mode returns immediately.
 * <p>For segment reassignment, 2 modes are offered:
 * <ul>
 *   <li>
 *     No-downtime mode: care is taken to ensure that the configured number of replicas of any segment are available
 *     (ONLINE) at all times. This mode returns after ExternalView reaches the target assignment.
 *   </li>
 *   <li>
 *     With-downtime mode: the IdealState is replaced in one go and there are no guarantees around replica availability.
 *     This mode returns after ExternalView reaches the target assignment.
 *   </li>
 * </ul>
 * <p>NOTE: If the controller that handles the rebalance goes down/restarted, the rebalance isn't automatically resumed
 * by other controllers.
 */
public class TableRebalancer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableRebalancer.class);

  private static final long EXTERNAL_VIEW_CHECK_INTERVAL_MS = 1_000L; // 1 second
  private static final long EXTERNAL_VIEW_STABILIZATION_MAX_WAIT_MS = 20 * 60_000L; // 20 minutes

  private final HelixManager _helixManager;
  private final HelixDataAccessor _helixDataAccessor;

  public TableRebalancer(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixDataAccessor = helixManager.getHelixDataAccessor();
  }

  public RebalanceResult rebalance(TableConfig tableConfig, Configuration rebalanceConfig) {
    long startTimeMs = System.currentTimeMillis();
    String tableNameWithType = tableConfig.getTableName();

    // Validate table config
    try {
      // Do not allow rebalancing HLC real-time table
      if (tableConfig.getTableType() == TableType.REALTIME && new StreamConfig(tableNameWithType,
          tableConfig.getIndexingConfig().getStreamConfigs()).hasHighLevelConsumerType()) {
        return new RebalanceResult(RebalanceResult.Status.FAILED, "Cannot rebalance table with high-level consumer",
            null, null);
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception while validating table config for table: {}", tableNameWithType, e);
      return new RebalanceResult(RebalanceResult.Status.FAILED, "Caught exception while validating table config: " + e,
          null, null);
    }

    // Check that table exists and is enabled
    IdealState currentIdealState =
        _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().idealStates(tableNameWithType));
    if (currentIdealState == null) {
      return new RebalanceResult(RebalanceResult.Status.FAILED, "Cannot find the table", null, null);
    }
    if (!currentIdealState.isEnabled()) {
      return new RebalanceResult(RebalanceResult.Status.FAILED, "Cannot rebalance disabled table", null, null);
    }

    // Check if any segment is in ERROR state in ExternalView before rebalancing the table. ERROR state segment cannot
    // be transferred to other state, thus will cause the rebalance to never converge.
    // NOTE: ExternalView might be null when table is just created
    ExternalView externalView =
        _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().externalView(tableNameWithType));
    if (externalView != null && hasSegmentInErrorState(externalView.getRecord().getMapFields())) {
      LOGGER.warn("Found segments in ERROR state for table: {}", tableNameWithType);
      return new RebalanceResult(RebalanceResult.Status.FAILED, "Found segments in ERROR state", null, null);
    }

    boolean dryRun =
        rebalanceConfig.getBoolean(RebalanceConfigConstants.DRY_RUN, RebalanceConfigConstants.DEFAULT_DRY_RUN);
    if (dryRun) {
      LOGGER.info("Start rebalancing table: {} in dry-run mode", tableNameWithType);
    } else {
      LOGGER.info("Start rebalancing table: {}", tableNameWithType);
    }

    // Reassign instances if necessary
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();
    boolean reassignInstances = rebalanceConfig
        .getBoolean(RebalanceConfigConstants.REASSIGN_INSTANCES, RebalanceConfigConstants.DEFAULT_REASSIGN_INSTANCES);
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      instancePartitionsMap.put(InstancePartitionsType.OFFLINE,
          getInstancePartitions(tableConfig, InstancePartitionsType.OFFLINE, reassignInstances, dryRun));
    } else {
      instancePartitionsMap.put(InstancePartitionsType.CONSUMING,
          getInstancePartitions(tableConfig, InstancePartitionsType.CONSUMING, reassignInstances, dryRun));
      instancePartitionsMap.put(InstancePartitionsType.COMPLETED,
          getInstancePartitions(tableConfig, InstancePartitionsType.COMPLETED, reassignInstances, dryRun));
    }

    LOGGER.info("Calculating the target assignment for table: {}", tableNameWithType);
    SegmentAssignment segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(_helixManager, tableConfig);
    Map<String, Map<String, String>> currentAssignment = currentIdealState.getRecord().getMapFields();
    Map<String, Map<String, String>> targetAssignment =
        segmentAssignment.rebalanceTable(currentAssignment, instancePartitionsMap, rebalanceConfig);

    if (currentAssignment.equals(targetAssignment)) {
      LOGGER.info("Table: {} is already balanced", tableNameWithType);
      if (reassignInstances) {
        return new RebalanceResult(RebalanceResult.Status.DONE, "Instance reassigned, table is already balanced",
            instancePartitionsMap, targetAssignment);
      } else {
        return new RebalanceResult(RebalanceResult.Status.NO_OP, "Table is already balanced", instancePartitionsMap,
            targetAssignment);
      }
    }

    if (dryRun) {
      LOGGER.info("Rebalance table: {} in dry-run mode, returning the target assignment", tableNameWithType);
      return new RebalanceResult(RebalanceResult.Status.DONE, "Dry-run mode", instancePartitionsMap, targetAssignment);
    }

    int minAvailableReplicas;
    if (rebalanceConfig.getBoolean(RebalanceConfigConstants.DOWNTIME, RebalanceConfigConstants.DEFAULT_DOWNTIME)) {
      minAvailableReplicas = 0;
      LOGGER.info("Rebalancing table: {} with downtime", tableNameWithType);
    } else {
      minAvailableReplicas = rebalanceConfig.getInt(RebalanceConfigConstants.MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME,
          RebalanceConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEP_UP_FOR_NO_DOWNTIME);
      int numCurrentReplicas = currentAssignment.values().iterator().next().size();
      int numTargetReplicas = targetAssignment.values().iterator().next().size();
      // Use the smaller one to determine the min available replicas
      int numReplicas = Math.min(numCurrentReplicas, numTargetReplicas);
      if (minAvailableReplicas > 0) {
        if (minAvailableReplicas >= numReplicas) {
          LOGGER.warn(
              "Illegal config for min available replicas: {} for table: {}, must be less than number of replicas (current: {}, target: {})",
              minAvailableReplicas, tableNameWithType, numCurrentReplicas, numTargetReplicas);
          return new RebalanceResult(RebalanceResult.Status.FAILED, "Illegal min available replicas config",
              instancePartitionsMap, targetAssignment);
        }
      }
      // If min available replicas is negative, treat it as max unavailable replicas
      if (minAvailableReplicas < 0) {
        minAvailableReplicas = Math.max(numReplicas + minAvailableReplicas, 0);
      }
      LOGGER.info("Rebalancing table: {} with min available replicas: {}", tableNameWithType, minAvailableReplicas);
    }

    int expectedVersion = currentIdealState.getRecord().getVersion();
    while (true) {
      // Wait for ExternalView to converge before updating the next IdealState
      try {
        IdealState idealState = waitForExternalViewToConverge(tableNameWithType);
        LOGGER.info("ExternalView converged for table: {}", tableNameWithType);
        if (idealState.getRecord().getVersion() != expectedVersion) {
          LOGGER.info(
              "IdealState version changed while waiting for ExternalView to converge for table: {}, re-calculating the target assignment",
              tableNameWithType);
          currentIdealState = idealState;
          currentAssignment = currentIdealState.getRecord().getMapFields();
          targetAssignment =
              segmentAssignment.rebalanceTable(currentAssignment, instancePartitionsMap, rebalanceConfig);
          expectedVersion = currentIdealState.getRecord().getVersion();
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while waiting for ExternalView to converge for table: {}", tableNameWithType, e);
        return new RebalanceResult(RebalanceResult.Status.FAILED,
            "Caught exception while waiting for ExternalView to converge: " + e, instancePartitionsMap,
            targetAssignment);
      }

      if (currentAssignment.equals(targetAssignment)) {
        LOGGER.info("Finished rebalancing table: {} in {}ms.", tableNameWithType,
            System.currentTimeMillis() - startTimeMs);
        return new RebalanceResult(RebalanceResult.Status.DONE, "Success", instancePartitionsMap, targetAssignment);
      }

      Map<String, Map<String, String>> nextAssignment =
          getNextAssignment(currentAssignment, targetAssignment, minAvailableReplicas);
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
            .set(_helixDataAccessor.keyBuilder().idealStates(tableNameWithType).getPath(), idealStateRecord,
                expectedVersion, AccessOption.PERSISTENT), "Failed to update IdealState");
        currentAssignment = nextAssignment;
        expectedVersion++;
        LOGGER.info("Successfully updated the IdealState for table: {}", tableNameWithType);
      } catch (ZkBadVersionException e) {
        LOGGER.info("Version changed while updating IdealState for table: {}", tableNameWithType);
      } catch (Exception e) {
        LOGGER.error("Caught exception while updating IdealState for table: {}", tableNameWithType, e);
        return new RebalanceResult(RebalanceResult.Status.FAILED, "Caught exception while updating IdealState: " + e,
            instancePartitionsMap, targetAssignment);
      }
    }
  }

  private InstancePartitions getInstancePartitions(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType, boolean reassignInstances, boolean dryRun) {
    String tableNameWithType = tableConfig.getTableName();
    if (reassignInstances) {
      if (InstanceAssignmentConfigUtils.allowInstanceAssignment(tableConfig, instancePartitionsType)) {
        LOGGER.info("Reassigning {} instance partitions for table: {}", instancePartitionsType, tableNameWithType);
        InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(tableConfig);
        InstancePartitions instancePartitions = instanceAssignmentDriver.assignInstances(instancePartitionsType,
            _helixDataAccessor.getChildValues(_helixDataAccessor.keyBuilder().instanceConfigs()));
        if (!dryRun) {
          LOGGER.info("Persisting instance partitions: {}", instancePartitions);
          InstancePartitionsUtils.persistInstancePartitions(_helixManager.getHelixPropertyStore(), instancePartitions);
        }
        return instancePartitions;
      } else {
        // Use default instance partitions if reassign is enabled and instance assignment is not allowed
        InstancePartitions instancePartitions = InstancePartitionsUtils
            .computeDefaultInstancePartitions(_helixManager, tableConfig, instancePartitionsType);
        if (!dryRun) {
          String instancePartitionsName = instancePartitions.getInstancePartitionsName();
          LOGGER.info("Removing instance partitions: {}", instancePartitionsName);
          InstancePartitionsUtils
              .removeInstancePartitions(_helixManager.getHelixPropertyStore(), instancePartitionsName);
        }
        return instancePartitions;
      }
    } else {
      return InstancePartitionsUtils
          .fetchOrComputeInstancePartitions(_helixManager, tableConfig, instancePartitionsType);
    }
  }

  private IdealState waitForExternalViewToConverge(String tableNameWithType)
      throws InterruptedException, TimeoutException {
    long endTimeMs = System.currentTimeMillis() + EXTERNAL_VIEW_STABILIZATION_MAX_WAIT_MS;

    while (System.currentTimeMillis() < endTimeMs) {
      IdealState idealState =
          _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().idealStates(tableNameWithType));
      // IdealState might be null if table got deleted, throwing exception to abort the rebalance
      Preconditions.checkState(idealState != null, "Failed to find the IdealState");

      ExternalView externalView =
          _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().externalView(tableNameWithType));
      // ExternalView might be null when table is just created, skipping check for this iteration
      if (externalView != null) {
        Map<String, Map<String, String>> externalViewSegmentStates = externalView.getRecord().getMapFields();
        if (isExternalViewConverged(externalViewSegmentStates, idealState.getRecord().getMapFields())) {
          return idealState;
        }
        if (hasSegmentInErrorState(externalViewSegmentStates)) {
          throw new IllegalStateException("Found segments in ERROR state");
        }
      }

      Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
    }

    throw new TimeoutException("Timeout while waiting for ExternalView to converge");
  }

  private static Map<String, Map<String, String>> getNextAssignment(Map<String, Map<String, String>> currentAssignment,
      Map<String, Map<String, String>> targetAssignment, int minAvailableReplicas) {
    Map<String, Map<String, String>> nextAssignment = new TreeMap<>();

    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      nextAssignment.put(segmentName,
          getNextInstanceStateMap(entry.getValue(), targetAssignment.get(segmentName), minAvailableReplicas));
    }

    return nextAssignment;
  }

  @VisibleForTesting
  @SuppressWarnings("Duplicates")
  static Map<String, String> getNextInstanceStateMap(Map<String, String> currentInstanceStateMap,
      Map<String, String> targetInstanceStateMap, int minAvailableReplicas) {
    Map<String, String> nextInstanceStateMap = new TreeMap<>();

    // Add all the common instances
    for (Map.Entry<String, String> entry : targetInstanceStateMap.entrySet()) {
      String instanceName = entry.getKey();
      if (currentInstanceStateMap.containsKey(instanceName)) {
        nextInstanceStateMap.put(instanceName, entry.getValue());
      }
    }

    // Add current instances until the min available replicas achieved
    int instancesToKeep = minAvailableReplicas - nextInstanceStateMap.size();
    if (instancesToKeep > 0) {
      for (Map.Entry<String, String> entry : currentInstanceStateMap.entrySet()) {
        String instanceName = entry.getKey();
        if (!nextInstanceStateMap.containsKey(instanceName)) {
          nextInstanceStateMap.put(instanceName, entry.getValue());
          if (--instancesToKeep == 0) {
            break;
          }
        }
      }
    }

    // Add target instances until the number of instances matched
    int instancesToAdd = targetInstanceStateMap.size() - nextInstanceStateMap.size();
    if (instancesToAdd > 0) {
      for (Map.Entry<String, String> entry : targetInstanceStateMap.entrySet()) {
        String instanceName = entry.getKey();
        if (!nextInstanceStateMap.containsKey(instanceName)) {
          nextInstanceStateMap.put(instanceName, entry.getValue());
          if (--instancesToAdd == 0) {
            break;
          }
        }
      }
    }

    return nextInstanceStateMap;
  }

  private static boolean hasSegmentInErrorState(Map<String, Map<String, String>> segmentStates) {
    for (Map<String, String> instanceStateMap : segmentStates.values()) {
      if (instanceStateMap.containsValue(SegmentOnlineOfflineStateModel.ERROR)) {
        return true;
      }
    }
    return false;
  }

  /**
   * NOTE: Only check the instances in IdealState. When dropping the segment, Helix might schedule multiple
   * ONLINE->OFFLINE state transitions, and could leave segment in OFFLINE state instead of dropping the segment. In
   * such case we don't want to block forever. It is okay to have not match instance state map as long as all the states
   * in IdealState are reached.
   */
  @VisibleForTesting
  static boolean isExternalViewConverged(Map<String, Map<String, String>> externalViewSegmentStates,
      Map<String, Map<String, String>> idealStateSegmentStates) {
    for (Map.Entry<String, Map<String, String>> entry : idealStateSegmentStates.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> externalViewInstanceStateMap = externalViewSegmentStates.get(segmentName);
      if (externalViewInstanceStateMap == null) {
        return false;
      }
      Map<String, String> idealStateInstanceStateMap = entry.getValue();
      for (Map.Entry<String, String> instanceStateEntry : idealStateInstanceStateMap.entrySet()) {
        // Ignore OFFLINE state in IdealState
        String state = instanceStateEntry.getValue();
        if (!state.equals(SegmentOnlineOfflineStateModel.OFFLINE) && !state
            .equals(externalViewInstanceStateMap.get(instanceStateEntry.getKey()))) {
          return false;
        }
      }
    }
    return true;
  }
}
