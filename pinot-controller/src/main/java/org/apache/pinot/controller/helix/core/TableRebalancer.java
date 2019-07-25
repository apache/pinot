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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.HashMap;
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
      throws InvalidConfigException, IllegalStateException {

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
      LOGGER.debug("Ideal state after making changes to partition map of all segments");
      prettyPrintIdealState(nextIdealState);

      // If the ideal state is large enough, enable compression
      if (HelixHelper.MAX_PARTITION_COUNT_IN_UNCOMPRESSED_IDEAL_STATE < nextIdealState.getPartitionSet().size()) {
        nextIdealState.getRecord().setBooleanField("enableCompression", true);
      }

      // Check version and set ideal state to nextIdealState
      try {
        LOGGER.info("Going to update current IdealState in ZK for table {}, current version {} and creation time {}",
            tableNameWithType, currentIdealState.getRecord().getVersion(), currentIdealState.getRecord().getCreationTime());
        if (zkBaseDataAccessor
            .set(idealStateKey.getPath(), nextIdealState.getRecord(), currentIdealState.getRecord().getVersion(),
                AccessOption.PERSISTENT)) {
          LOGGER.debug("Successfully persisted the ideal state for table {} in ZK. Will wait for External view to converge",
              tableNameWithType);
          ++_rebalancerStats.updatestoIdealStateInZK;
          // if we succeeded, wait for the change to stabilize
          waitForStable(tableNameWithType);
          // clear retries as it tracks failures with each idealstate update attempt
          retries = 0;
          LOGGER.debug("External view converged for the change in ideal state for table {}. Will start the next iteration now",
              tableNameWithType);
          continue;
        }
        // in case of any error, we retry a bounded number of times
      } catch (ZkBadVersionException e) {
        // we will go back in the loop and reattempt by recomputing the target ideal state
        LOGGER.warn("Version changed while updating ideal state for resource: {}", tableNameWithType);
      } catch (Exception e) {
        if (e instanceof IllegalStateException && e.getCause() instanceof ExternalViewErrored) {
          LOGGER.error("External view reported error for table {} after updating ideal state", tableNameWithType);
          // remove the cause as it is private and is only used to detect exact error within this class
          throw new IllegalStateException(e.getMessage());
        } else {
          LOGGER.warn("Caught exception while or after updating ideal state for resource: {}", tableNameWithType, e);
        }
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
   * Helper method to dump ideal state
   * @param idealState ideal state to dump
   */
  private static void prettyPrintIdealState(final IdealState idealState) {
    if (LOGGER.isDebugEnabled()) {
      Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();
      for (String segment : mapFields.keySet()) {
        LOGGER.debug("Segment: {}", segment);
        prettyPrintMapDebug(mapFields.get(segment));
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
   *
   * In no-downtime mode, we check for couple of things and
   * keep a certain number of serving replicas for the segment alive
   * as specified in the rebalance configuration
   *
   * (1) if the number of common hosts between current and target
   * mapping are enough to satisfy more than or equal to the number
   * of serving replicas we should keep alive then we set to the
   * target mapping directly (at one go) since this honors the no-downtime
   * requirement
   *
   * (2) however, if there are not enough common hosts then we don't
   * change the mapping directly as that will result in downtime. The
   * mapping is updated incrementally (only one change for a given
   * invocation of this method per segment) by removing a host
   * from the current mapping and adding a host to it from
   * target mapping.
   *
   * When the caller of this method returns (after going over all
   * segments once), it persists the new ideal state and then determines
   * if we have reached the target. If not, the process continues and we come
   * here again for each method and check for steps (1) or (2) as applicable
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

    if (currentIdealStateSegmentHosts == null) {
      //segment can be missing if retention manager has deleted it
      LOGGER.info("Segment " + segmentId + " missing from current idealState. Skipping it.");
      return;
    }

    // dump additional detailed info for debugging
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Current hosts and states for segment {}", segmentId);
      prettyPrintMapDebug(currentIdealStateSegmentHosts);
      LOGGER.debug("Target hosts and states for segment {}", segmentId);
      prettyPrintMapDebug(targetIdealStateSegmentHosts);
    }

    if (rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.DOWNTIME,
        RebalanceUserConfigConstants.DEFAULT_DOWNTIME)) {
      // in downtime mode, set the current ideal state to target at one go
      LOGGER.debug("Downtime mode is enabled. Will set to target state at one go");
      setTargetState(idealStateToUpdate, segmentId, targetIdealStateSegmentHosts);
      ++_rebalancerStats.directUpdatesToSegmentInstanceMap;
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
      LOGGER.debug("Unable to keep {} min replicas alive as the segment {} has {} replicas",
          minReplicasToKeepUp, segmentId, currentIdealStateSegmentHosts.size());
      minReplicasToKeepUp = RebalanceUserConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME;
    }
    MapDifference difference = Maps.difference(targetIdealStateSegmentHosts, currentIdealStateSegmentHosts);
    if (difference.entriesInCommon().size() >= minReplicasToKeepUp) {
      // if there are enough hosts in common between current and target ideal states
      // to satisfy the min replicas condition, then there won't be any downtime
      // and we can directly set the current ideal state to target ideal state
      LOGGER.debug("Current and target ideal states have common hosts. Will set to target state at one go");
      setTargetState(idealStateToUpdate, segmentId, targetIdealStateSegmentHosts);
      ++_rebalancerStats.directUpdatesToSegmentInstanceMap;
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

      Preconditions.checkArgument(!hostToAdd.equals(""),
          "expecting a valid hostname to add from target segment mapping");
      idealStateToUpdate.setPartitionState(segmentId, hostToAdd, targetIdealStateSegmentHosts.get(hostToAdd));
      LOGGER.info("Adding " + hostToAdd + " to serve segment " + segmentId);
      ++_rebalancerStats.incrementalUpdatesToSegmentInstanceMap;

      // dump additional detailed info for debugging
      if (LOGGER.isDebugEnabled()) {
        final Map<String, String> updatedSegmentHosts = idealStateToUpdate.getInstanceStateMap(segmentId);
        LOGGER.debug("Updated hosts and states for segment {}", segmentId);
        prettyPrintMapDebug(updatedSegmentHosts);
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
   * Check if external view has converged to ideal state
   * @param tableName name of table that we are rebalancing
   * @return true if external view is same as ideal state, false otherwise
   */
  private boolean isStable(String tableName) {
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
    Map<String, Map<String, String>> mapFieldsIS = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> mapFieldsEV = externalView.getRecord().getMapFields();

    LOGGER.info("Checking if ideal state and external view are same for table {}", tableName);

    boolean stable = true;
    for (String segment : mapFieldsIS.keySet()) {
      Map<String, String> mapIS = mapFieldsIS.get(segment);
      Map<String, String> mapEV = mapFieldsEV.get(segment);

      if (mapEV == null) {
        LOGGER.debug("Host-state mapping of segment {} not yet available in external view", segment);
        // we have found that external view hasn't yet converged to ideal state.
        // still go on to check for other segments just so that we can dump debug
        // messages or we can detect error. that's why we don't return
        // false immediately
        stable = false;
        continue;
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Hosts and states for segment {} in ideal state", segment);
        prettyPrintMapDebug(mapIS);
        LOGGER.debug("Hosts and states for segment {} in external view", segment);
        prettyPrintMapDebug(mapEV);
      }

      for (String server : mapIS.keySet()) {
        if (!mapEV.containsKey(server)) {
          LOGGER.debug("Host-state mapping of segment {} doesn't yet have server {} in external view",
              segment, server);
          // external view not yet converged
          stable = false;
        } else if (mapEV.get(server).equalsIgnoreCase("error")) {
          LOGGER.error("Detected error state for segment {} for server {}", segment, server);
          prettyPrintMapError(mapIS);
          prettyPrintMapError(mapEV);
          throw new IllegalStateException("External view reports error state for segment " + segment + " for host " + server,
              new ExternalViewErrored());
        } else {
          final String stateInIdealState = mapIS.get(server);
          final String stateInExternalView = mapEV.get(server);
          if (!stateInIdealState.equalsIgnoreCase(stateInExternalView)) {
            LOGGER.debug("Host-state mapping of segment {} has state {} in external view and state {} in ideal state",
                segment, stateInExternalView, stateInIdealState);
            // external view not yet converged
            stable = false;
          }
        }
      }
    }
    return stable;
  }

  private static void prettyPrintMapDebug(final Map<String, String> map) {
    final Joiner.MapJoiner mapJoiner = Joiner.on(",").withKeyValueSeparator(":");
    LOGGER.debug(mapJoiner.join(map));
  }

  private static void prettyPrintMapError(final Map<String, String> map) {
    final Joiner.MapJoiner mapJoiner = Joiner.on(",").withKeyValueSeparator(":");
    LOGGER.error(mapJoiner.join(map));
  }

  /**
   * Wait till state has stabilized {@link #isStable(String)}
   */
  private void waitForStable(final String resourceName) {
    int INITIAL_WAIT_MS = 3000;
    try {
      Thread.sleep(INITIAL_WAIT_MS);
      while (true) {
        if (isStable(resourceName)) {
          break;
        } else {
          LOGGER.info("Waiting for externalView to match idealstate for table:" + resourceName);
          Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
          }
        }
      } catch (InterruptedException e) {
      LOGGER.error("Rebalancer got interrupted while waiting for external view to converge");
      Thread.currentThread().interrupt();
    }
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
    private int updatestoIdealStateInZK;
    private int directUpdatesToSegmentInstanceMap;
    private int incrementalUpdatesToSegmentInstanceMap;

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
     * Get the number of times rebalancer
     * successfully updated the ideal
     * state in ZK as it progresses through
     * rebalancing.
     * @return ideal state updates in ZK
     */
    public int getUpdatestoIdealStateInZK() {
      return updatestoIdealStateInZK;
    }

    /**
     * Get the number of times we updated the instance-state
     * mapping of a segment in ideal state in one step. This
     * happens in downtime rebalancing and can also happen
     * in no-downtime rebalancing if there are sufficient common
     * hosts between current and target ideal states
     * @return direct updates
     */
    public int getDirectUpdatesToSegmentInstanceMap() {
      return directUpdatesToSegmentInstanceMap;
    }

    /**
     * Get the number of times we updated the instance-state
     * mapping of a segment in ideal state incrementally
     * to keep the requirement of having some number of
     * serving replicas up for each segment
     * @return incremental updates
     */
    public int getIncrementalUpdatesToSegmentInstanceMap() {
      return incrementalUpdatesToSegmentInstanceMap;
    }
  }

  public RebalancerStats getRebalancerStats() {
    return _rebalancerStats;
  }

  private static class ExternalViewErrored extends Throwable {

  }
}
