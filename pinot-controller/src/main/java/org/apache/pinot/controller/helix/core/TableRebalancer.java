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
package org.apache.pinot.controller.helix.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.partition.PartitionAssignment;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategy;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that handles rebalancing and updating ideal state for a given table. 2 modes of
 * updates are offered: no-down-time rebalance and down-time rebalance.
 * In the first mode, care is taken to ensure that there is atleast one replica
 * of any segment online at all times - this mode kicks off a background thread
 * and steps through the ideal-state transformation.
 * In the down-time mode, the ideal-state is replaced in one go and there are
 * no guarantees around replica availability. This mode returns immediately,
 * however the actual rebalance by Helix can take an unbounded amount of time.
 *
 * Limitations:
 * 1. Currently, if the controller that handles the rebalance goes down/restarted
 *    the rebalance isn't automatically resumed by other controllers.
 * 2. There is no feedback to the user on the progress of the rebalance. Controller logs will
 *    provide updates on progress.
 */
public class TableRebalancer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableRebalancer.class);

  private static final int MAX_RETRIES = 10;
  private static final int EXTERNAL_VIEW_CHECK_INTERVAL_MS = 30000;

  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final String _helixClusterName;
  private final RebalancerStats _rebalancerStats;

  public TableRebalancer(HelixManager mgr, HelixAdmin admin, String helixClusterName) {
    _helixManager = mgr;
    _helixAdmin = admin;
    _helixClusterName = helixClusterName;
    _rebalancerStats = new RebalancerStats();
  }

  /**
   * Update idealstate to the target rebalanced state honoring the downtime/no-downtime configuration.
   *
   * In case of no-downtime mode, we first ensure that the ideal state is updated such that there is atleast one serving
   * replica. Once we confirm that atleast one serving replica is available, we switch to the target state in one
   * step. Since the ideal state rebalance is now broken into multiple steps, in each step we check to see if additional
   * rebalancing is required (incase there are new segments or other changes to the cluster). This is done by comparing
   * the IdealState that was used to generate the target in each step.
   *
   * Note: we don't use {@link org.apache.pinot.common.utils.helix.HelixHelper} directly as we would like to manage
   * the main logic and retries according to the rebalance algorithm. Some amount of code is duplicated from HelixHelper.
   */
  public RebalanceResult rebalance(TableConfig tableConfig, RebalanceSegmentStrategy strategy,
      Configuration rebalanceConfig)
      throws InvalidConfigException {

    RebalanceResult result = new RebalanceResult();

    String tableNameWithType = tableConfig.getTableName();
    HelixDataAccessor dataAccessor = _helixManager.getHelixDataAccessor();
    ZkBaseDataAccessor zkBaseDataAccessor = (ZkBaseDataAccessor) dataAccessor.getBaseDataAccessor();

    PropertyKey idealStateKey = dataAccessor.keyBuilder().idealStates(tableNameWithType);
    IdealState previousIdealState = dataAccessor.getProperty(idealStateKey);

    if (rebalanceConfig.getBoolean(RebalanceUserConfigConstants.DRYRUN, RebalanceUserConfigConstants.DEFAULT_DRY_RUN)) {
      PartitionAssignment partitionAssignment =
          strategy.rebalancePartitionAssignment(previousIdealState, tableConfig, rebalanceConfig);
      IdealState idealState =
          strategy.getRebalancedIdealState(previousIdealState, tableConfig, rebalanceConfig, partitionAssignment);
      result.setIdealStateMapping(idealState.getRecord().getMapFields());
      result.setPartitionAssignment(partitionAssignment);
      result.setStatus(RebalanceResult.RebalanceStatus.DONE);
      _rebalancerStats.dryRun = 1;
      return result;
    }

    long startTime = System.nanoTime();
    IdealState targetIdealState = null;
    PartitionAssignment targetPartitionAssignment = null;

    long retryDelayMs = 60000; // 1 min
    int retries = 0;
    LOGGER.info("Start rebalancing table :{}", tableNameWithType);
    while (true) {
      IdealState currentIdealState = dataAccessor.getProperty(idealStateKey);

      if (targetIdealState == null || !EqualityUtils.isEqual(previousIdealState, currentIdealState)) {
        LOGGER.info("Computing target ideal state for table {}", tableNameWithType);
        // we need to recompute target state
        // Make a copy of the the idealState above to pass it to the rebalancer
        // NOTE: new IdealState(idealState.getRecord()) does not work because it's shallow copy for map fields and
        // list fields
        IdealState idealStateCopy = HelixHelper.cloneIdealState(currentIdealState);
        // rebalance; can throw InvalidConfigException - in which case we bail out
        targetPartitionAssignment =
            strategy.rebalancePartitionAssignment(previousIdealState, tableConfig, rebalanceConfig);
        targetIdealState =
            strategy.getRebalancedIdealState(idealStateCopy, tableConfig, rebalanceConfig, targetPartitionAssignment);
      }

      if (EqualityUtils.isEqual(targetIdealState, currentIdealState)) {
        LOGGER.info("Finished rebalancing table {} in {} ms.", tableNameWithType,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
        result.setIdealStateMapping(targetIdealState.getRecord().getMapFields());
        result.setPartitionAssignment(targetPartitionAssignment);
        result.setStatus(RebalanceResult.RebalanceStatus.DONE);
        return result;
      }

      // if ideal state needs to change, get the next 'safe' state (based on whether downtime is OK or not)
      IdealState nextIdealState = getNextState(currentIdealState, targetIdealState, rebalanceConfig);

      // If the ideal state is large enough, enable compression
      if (HelixHelper.MAX_PARTITION_COUNT_IN_UNCOMPRESSED_IDEAL_STATE < nextIdealState.getPartitionSet().size()) {
        nextIdealState.getRecord().setBooleanField("enableCompression", true);
      }

      // Check version and set ideal state
      try {
        LOGGER.info("Updating IdealState for table {}", tableNameWithType);
        if (zkBaseDataAccessor
            .set(idealStateKey.getPath(), nextIdealState.getRecord(), currentIdealState.getRecord().getVersion(),
                AccessOption.PERSISTENT)) {
          LOGGER.debug("Successfully persisted the ideal state in ZK. Will wait for External view to converge");
          // if we succeeded, wait for the change to stabilize
          waitForStable(tableNameWithType);
          // clear retries as it tracks failures with each idealstate update attempt
          retries = 0;
          LOGGER.debug("External view converged for the change in ideal state. Will start the next iteration now");
          continue;
        }
        // in case of any error, we retry a bounded number of types
      } catch (ZkBadVersionException e) {
        LOGGER.warn("Version changed while updating ideal state for resource: {}", tableNameWithType);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while updating ideal state for resource: {}", tableNameWithType, e);
      }

      previousIdealState = currentIdealState;
      if (retries++ > MAX_RETRIES) {
        LOGGER.error("Unable to rebalance table {} in {} attempts. Giving up", tableNameWithType, MAX_RETRIES);
        result.setStatus(RebalanceResult.RebalanceStatus.FAILED);
        return result;
      }
      // wait before retrying
      try {
        Thread.sleep(retryDelayMs);
      } catch (InterruptedException e) {
        LOGGER.error("Got interrupted while rebalancing table {}", tableNameWithType);
        result.setStatus(RebalanceResult.RebalanceStatus.FAILED);
        Thread.currentThread().interrupt();
        return result;
      }
    }
  }

  /**
   * Gets the next ideal state based on the target (rebalanced) state. If no downtime is desired, the next state
   * is set such that there is always atleast one common replica for each segment between current and next state.
   */
  private IdealState getNextState(IdealState currentState, IdealState targetState, Configuration rebalanceUserConfig) {

    // make a copy of the ideal state so it can be updated
    IdealState idealStateCopy = HelixHelper.cloneIdealState(currentState);

    Map<String, Map<String, String>> currentMapFields = currentState.getRecord().getMapFields();
    Map<String, Map<String, String>> targetMapFields = targetState.getRecord().getMapFields();

    for (String segmentId : targetMapFields.keySet()) {
      updateSegmentIfNeeded(segmentId, currentMapFields.get(segmentId), targetMapFields.get(segmentId), idealStateCopy,
          rebalanceUserConfig);
    }

    return idealStateCopy;
  }

  /**
   * Updates a segment mapping if needed. In "downtime" mode.
   * the segment mapping is set to the target mapping directly.
   * In no-downtime mode, one element of the source mapping is replaced
   * with one from the target mapping. Secondly, with downtime mode
   * and if there are some common hosts, then we check if the number
   * of common hosts are enough to satisfy the minimum number
   * of serving replicas requirement as specified in rebalance
   * config.
   *
   * @param segmentId segment id
   * @param currentIdealStateSegmentHosts map of this segment's hosts (replicas)
   *                                     in current ideal state
   * @param targetIdealStateSegmentHosts map of this segment's hosts (replicas)
   *                                     in target ideal state
   * @param idealStateToUpdate the ideal state that is updated as (caller
   *                           passes this as a copy of current ideal state)
   * @param rebalanceUserConfig rebalance configuration
   */
  @VisibleForTesting
  public void updateSegmentIfNeeded(String segmentId, Map<String, String> currentIdealStateSegmentHosts,
      Map<String, String> targetIdealStateSegmentHosts, IdealState idealStateToUpdate,
      Configuration rebalanceUserConfig) {

    LOGGER.info("Will update instance map of segment: {}", segmentId);

    // dump additional detailed info for debugging
    if (LOGGER.isDebugEnabled()) {
      // check the debug level beforehand and write everything at once
      // else we will have to check repeatedly in a loop=
      StringBuilder sb = new StringBuilder("Current segment hosts and states:\n");
      for (Map.Entry<String, String> entry : currentIdealStateSegmentHosts.entrySet()) {
        sb.append("HOST: ").append(entry.getKey()).append("STATE: ").append(entry.getValue()).append("\n");
      }
      LOGGER.debug(sb.toString());
      sb = new StringBuilder("Target segment hosts and states:\n");
      for (Map.Entry<String, String> entry : targetIdealStateSegmentHosts.entrySet()) {
        sb.append("HOST: ").append(entry.getKey()).append("STATE: ").append(entry.getValue()).append("\n");
      }
      LOGGER.debug(sb.toString());
    }

    if (currentIdealStateSegmentHosts == null) {
      //segment can be missing if retention manager has deleted it
      LOGGER.info("Segment " + segmentId + " missing from current idealState. Skipping it.");
      return;
    }

    if (rebalanceUserConfig
        // in downtime mode, set the current ideal state to target at one go
        .getBoolean(RebalanceUserConfigConstants.DOWNTIME, RebalanceUserConfigConstants.DEFAULT_DOWNTIME)) {
      LOGGER.debug("Downtime mode is enabled. Will set to target state at one go");
      setTargetState(idealStateToUpdate, segmentId, targetIdealStateSegmentHosts);
      ++_rebalancerStats.directTransitions;
      return;
    }

    // we are in no-downtime mode
    int minReplicasToKeepUp = rebalanceUserConfig
        .getInt(RebalanceUserConfigConstants.MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME,
            RebalanceUserConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME);
    LOGGER.debug("No downtime mode is enabled. Need to keep {} serving replicas up while rebalancing",
        minReplicasToKeepUp);

    if (minReplicasToKeepUp >= currentIdealStateSegmentHosts.size()) {
      // if the minimum number of serving replicas in rebalance config is
      // greater than or equal to number of replicas of a segment, then it
      // is impossible to honor the request. so we use the default number
      // of minimum serving replicas we will keep for no downtime mode
      // (currently 1)
      minReplicasToKeepUp = RebalanceUserConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME;
    }
    MapDifference difference = Maps.difference(targetIdealStateSegmentHosts, currentIdealStateSegmentHosts);
    if (difference.entriesInCommon().size() >= minReplicasToKeepUp) {
      // if there are enough hosts in common between current and target ideal states
      // to satisfy the min replicas condition, then there won't be any downtime
      // and we can directly set the current ideal state to target ideal state
      LOGGER.debug("Current and target ideal states have common hosts. Will set to target state at one go");
      setTargetState(idealStateToUpdate, segmentId, targetIdealStateSegmentHosts);
      ++_rebalancerStats.directTransitions;
    } else {
      // remove one entry
      String hostToRemove = "";
      for (String host : currentIdealStateSegmentHosts.keySet()) {
        // the common host between current and target ideal
        // states should be ignored
        if (!targetIdealStateSegmentHosts.containsKey(host)) {
          hostToRemove = host;
          break;
        }
      }

      if (!hostToRemove.equals("")) {
        idealStateToUpdate.getInstanceStateMap(segmentId).remove(hostToRemove);
        LOGGER.info("Removing host: {} for segment: {}", hostToRemove, segmentId);
      }

      // add an entry from the target state to ensure there is no downtime
      final Map<String, String> updatedSegmentInstancesMap = idealStateToUpdate.getInstanceStateMap(segmentId);
      String hostToAdd = "";
      for (String host : targetIdealStateSegmentHosts.keySet()) {
        // the hosts host that has already been added from target
        // ideal state to new state should be ignored
        if (!updatedSegmentInstancesMap.containsKey(host)) {
          hostToAdd = host;
          break;
        }
      }

      if (!hostToAdd.equals("")) {
        idealStateToUpdate.setPartitionState(segmentId, hostToAdd, targetIdealStateSegmentHosts.get(hostToAdd));
        LOGGER.info("Adding " + hostToAdd + " to serve segment " + segmentId);
      }

      ++_rebalancerStats.incrementalTransitions;

      // dump additional detailed info for debugging
      if (LOGGER.isDebugEnabled()) {
        // check the debug level beforehand and write everything at once
        // else we will have to check repeatedly in a loop
        StringBuilder sb = new StringBuilder("Updated segment hosts and states:\n");
        final Map<String, String> updatedSegmentHosts = idealStateToUpdate.getInstanceStateMap(segmentId);
        for (Map.Entry<String, String> entry : updatedSegmentHosts.entrySet()) {
          sb.append("HOST: ").append(entry.getKey()).append("STATE: ").append(entry.getValue()).append("\n");
        }
        LOGGER.debug(sb.toString());
      }
    }
  }

  /**
   * Sets the idealstate for a given segment to the target mapping
   */
  private void setTargetState(IdealState idealState, String segmentId, Map<String, String> targetMap) {
    idealState.getInstanceStateMap(segmentId).clear();
    idealState.setInstanceStateMap(segmentId, targetMap);
  }

  /**
   * Check if IdealState = ExternalView. If its not equal, return the number of differing segments.
   */
  public int isStable(String tableName) {
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
    Map<String, Map<String, String>> mapFieldsIS = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> mapFieldsEV = externalView.getRecord().getMapFields();
    int numDiff = 0;
    for (String segment : mapFieldsIS.keySet()) {
      Map<String, String> mapIS = mapFieldsIS.get(segment);
      Map<String, String> mapEV = mapFieldsEV.get(segment);

      for (String server : mapIS.keySet()) {
        String state = mapIS.get(server);
        if (mapEV == null || mapEV.get(server) == null || !mapEV.get(server).equals(state)) {
          LOGGER.debug("Mismatch: segment" + segment + " server:" + server + " state:" + state);
          numDiff = numDiff + 1;
        }
      }
    }
    return numDiff;
  }

  /**
   * Wait till state has stabilized {@link #isStable(String)}
   */
  private void waitForStable(String resourceName)
      throws InterruptedException {
    int diff;
    int INITIAL_WAIT_MS = 3000;
    Thread.sleep(INITIAL_WAIT_MS);
    do {
      diff = isStable(resourceName);
      if (diff == 0) {
        break;
      } else {
        LOGGER.info(
            "Waiting for externalView to match idealstate for table:" + resourceName + " Num segments difference:"
                + diff);
        Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
      }
    } while (diff > 0);
  }

  /**
   * Helper class that maintains stats that
   * are later checked in tests to verify
   * the behavior of the algorithm here
   * that takes from current ideal state
   * to a target ideal state
   */
  public static class RebalancerStats {
    private int dryRun;
    private int directTransitions;
    private int incrementalTransitions;

    RebalancerStats() {
    }

    /**
     * Number of dry runs. Can only be 1
     * @return
     */
    public int getDryRun() {
      return dryRun;
    }

    /**
     * Get the number of times we updated the ideal
     * state at one go. This happens in downtime
     * rebalancing and can also happen in no-downtime
     * rebalancing if there are sufficient common
     * hosts between current and target ideal states
     * @return direct transitions
     */
    public int getDirectTransitions() {
      return directTransitions;
    }

    /**
     * Get the number of times we updated the ideal
     * states incrementally to keep the requirement
     * of having some number of serving replicas up
     * for each segment
     * @return incremental transitions
     */
    public int getIncrementalTransitions() {
      return incrementalTransitions;
    }
  }

  public RebalancerStats getRebalancerStats() {
    return _rebalancerStats;
  }
}
