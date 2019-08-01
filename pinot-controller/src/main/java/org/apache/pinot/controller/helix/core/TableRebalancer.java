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
import org.apache.logging.log4j.Level;
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
  private static final int EXTERNVAL_VIEW_STABILIZATON_MAX_WAIT_MINS = 20;

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

    // before running rebalancer if external view is fine to begin with
    // if it is already in error state, we return
    if (!preCheckErrorInExternalView(tableNameWithType)) {
      result.setStatus("Will not run rebalancer as external view is already in ERROR state for table" + tableNameWithType);
      result.setStatusCode(RebalanceResult.RebalanceStatus.FAILED);
      return result;
    }

    if (rebalanceConfig.getBoolean(RebalanceUserConfigConstants.DRYRUN, RebalanceUserConfigConstants.DEFAULT_DRY_RUN)) {
      LOGGER.info("Rebalancer running in dry run mode. Will return the target ideal state");
      PartitionAssignment partitionAssignment =
          strategy.rebalancePartitionAssignment(previousIdealState, tableConfig, rebalanceConfig);
      IdealState idealState =
          strategy.getRebalancedIdealState(previousIdealState, tableConfig, rebalanceConfig, partitionAssignment);
      result.setIdealStateMapping(idealState.getRecord().getMapFields());
      result.setPartitionAssignment(partitionAssignment);
      result.setStatus("Successfully ran rebalancer in dry run mode");
      result.setStatusCode(RebalanceResult.RebalanceStatus.DONE);
      _rebalancerStats.dryRun = 1;
      return result;
    }

    long startTime = System.nanoTime();
    IdealState targetIdealState = null;
    PartitionAssignment targetPartitionAssignment = null;

    final boolean downtime = rebalanceConfig.getBoolean(RebalanceUserConfigConstants.DOWNTIME,
        RebalanceUserConfigConstants.DEFAULT_DOWNTIME);
    final int minAvailableReplicas = rebalanceConfig
        .getInt(RebalanceUserConfigConstants.MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME,
            RebalanceUserConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME);

    if (downtime) {
      LOGGER.info("Downtime mode is enabled");
    } else {
      LOGGER.info("No downtime mode is enabled with {} min available replicas", minAvailableReplicas);
      // validate the number of min replicas to keep alive
      // if using no-downtime mode
      validateMinReplicas(tableNameWithType, rebalanceConfig, previousIdealState);
    }

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
        result.setStatus("Successfully finished rebalancing");
        result.setStatusCode(RebalanceResult.RebalanceStatus.DONE);
        return result;
      }

      // if ideal state needs to change, get the next 'safe' state (based on whether downtime is OK or not)
      final int currentMovements = _rebalancerStats.numSegmentMoves;
      IdealState nextIdealState = getNextState(currentIdealState, targetIdealState, downtime, minAvailableReplicas);
      LOGGER.info("Got new ideal state after doing {} segment movements. Will attempt to persist this in ZK. Logging the difference (if any) between new and target ideal state",
          _rebalancerStats.numSegmentMoves - currentMovements);
      printIdealStateDifference(targetIdealState, nextIdealState);

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
          LOGGER.info("Successfully persisted the ideal state for table {} in ZK. Will wait for External view to converge",
              tableNameWithType);
          ++_rebalancerStats.updatestoIdealStateInZK;
          // if we succeeded, wait for the change to stabilize
          waitForStable(tableNameWithType);
          // clear retries as it tracks failures with each idealstate update attempt
          retries = 0;
          LOGGER.info("External view converged for the change in ideal state for table {}. Will start the next iteration (if any)",
              tableNameWithType);
          continue;
        }
        // in case of any error, we retry a bounded number of times
      } catch (ZkBadVersionException e) {
        // we will go back in the loop and reattempt by recomputing the target ideal state
        LOGGER.info("Version changed while updating ideal state for resource: {}, was expecting version {}",
            tableNameWithType, currentIdealState.getRecord().getVersion());
      } catch (Exception e) {
        if (e instanceof IllegalStateException && e.getCause() instanceof ExternalViewErrored) {
          if (e.getCause() instanceof ExternalViewErrored) {
            LOGGER.error("External view reported error for table {} after updating ideal state", tableNameWithType);
          } else if (e.getCause() instanceof ExternalViewConvergeTimeout) {
            LOGGER.error("Timed out while waiting for external view to converge for table {}", tableNameWithType);
          }
          // remove the cause as it is private and is only used to detect exact error within this class
          throw new IllegalStateException(e.getMessage());
        } else {
          LOGGER.error("Caught exception while or after updating ideal state for resource: {}", tableNameWithType, e);
          throw e;
        }
      }

      // if we are here, we failed to persist the updated ideal state in ZK
      // due to version mismatch, will attempt again
      previousIdealState = currentIdealState;
      if (retries++ > MAX_RETRIES) {
        LOGGER.error("Unable to rebalance table {} in {} attempts. Giving up", tableNameWithType, MAX_RETRIES);
        result.setStatus("Cancelling the rebalance operation as we have exhausted the max number of retries after ZK version mismatch");
        result.setStatusCode(RebalanceResult.RebalanceStatus.FAILED);
        return result;
      }
      // wait before retrying
      try {
        Thread.sleep(retryDelayMs);
      } catch (InterruptedException e) {
        LOGGER.error("Got interrupted while rebalancing table {}", tableNameWithType);
        result.setStatus("Rebalancer got interrupted");
        result.setStatusCode(RebalanceResult.RebalanceStatus.FAILED);
        Thread.currentThread().interrupt();
        return result;
      }
    }
  }

  private void validateMinReplicas(final String tableNameWithType,
      final Configuration rebalanceConfig, final IdealState idealState) {
    final int replicas = Integer.valueOf(idealState.getReplicas());
    if (rebalanceConfig.getBoolean(RebalanceUserConfigConstants.DOWNTIME,
        RebalanceUserConfigConstants.DEFAULT_DOWNTIME)) {
      final int minReplicasToKeepUp = rebalanceConfig
          .getInt(RebalanceUserConfigConstants.MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME,
              RebalanceUserConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME);
      if (minReplicasToKeepUp >= replicas) {
        LOGGER.error("User specified invalid number of min replicas: {} to keep alive in no-downtime mode for table {}. Replica count for table {} ",
            minReplicasToKeepUp, tableNameWithType, replicas);
        throw new IllegalArgumentException("User specified invalid number of min available replicas " + minReplicasToKeepUp);
      }
    }
  }

  /**
   * Print the difference between target and updated ideal state
   * @param target target ideal state
   * @param updated updated ideal state
   */
  private void printIdealStateDifference(final IdealState target, final IdealState updated) {
    final Map<String, Map<String, String>> targetSegments = target.getRecord().getMapFields();
    final Map<String, Map<String, String>> updatedSegments = updated.getRecord().getMapFields();
    for (String segment : targetSegments.keySet()) {
      final Map<String, String> targetInstanceMap = targetSegments.get(segment);
      final Map<String, String> updatedInstanceMap = updatedSegments.get(segment);
      if (updatedInstanceMap == null) {
        LOGGER.info("Segment {} missing from current ideal state", segment);
      } else {
        MapDifference diff = Maps.difference(targetInstanceMap, updatedInstanceMap);
        if (diff.entriesInCommon().size() > 0) {
          LOGGER.debug("Common hosts for segment {}", segment);
          prettyPrintMap(diff.entriesInCommon(), Level.DEBUG);
        }
        if (diff.entriesOnlyOnLeft().size() > 0) {
          LOGGER.info("Hosts from target state not yet added for segment {}", segment);
          prettyPrintMap(diff.entriesOnlyOnLeft(), Level.INFO);
        }
        if (diff.entriesOnlyOnRight().size() > 0) {
          LOGGER.debug("Hosts from updated state not yet removed for segment {}", segment);
          prettyPrintMap(diff.entriesOnlyOnRight(), Level.DEBUG);
        }
      }
    }
  }

  /**
   * Gets the next ideal state based on the target (rebalanced) state. If no downtime is desired, the next state
   * is set such that there is always atleast one common replica for each segment between current and next state.
   */
  private IdealState getNextState(IdealState currentState, IdealState targetState,
      boolean downtime, int minAvailableReplicas) {
    // make a copy of the ideal state so it can be updated
    IdealState idealStateToUpdate = HelixHelper.cloneIdealState(currentState);

    Map<String, Map<String, String>> currentMapFields = currentState.getRecord().getMapFields();
    Map<String, Map<String, String>> targetMapFields = targetState.getRecord().getMapFields();

    final int directUpdates = _rebalancerStats.directUpdatesToSegmentInstanceMap;
    final int incrementalUpdates = _rebalancerStats.incrementalUpdatesToSegmentInstanceMap;

    for (String segmentId : targetMapFields.keySet()) {
      updateSegmentIfNeeded(segmentId, currentMapFields.get(segmentId), targetMapFields.get(segmentId),
          idealStateToUpdate, downtime, minAvailableReplicas);
    }

    LOGGER.info("Updated instance state map of {} segments by directly setting to target ideal state mapping",
        _rebalancerStats.directUpdatesToSegmentInstanceMap - directUpdates);
    LOGGER.info("Updated instance state map of {} segments incrementally by adding a host from target ideal state mapping",
        _rebalancerStats.incrementalUpdatesToSegmentInstanceMap - incrementalUpdates);

    return idealStateToUpdate;
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
   * @param downtime true if downtime is enabled, false otherwise
   * @param minAvailableReplicas min number of replicas to be available
   *                             if downtime is false
   */
  @VisibleForTesting
  public void updateSegmentIfNeeded(String segmentId, Map<String, String> currentIdealStateSegmentHosts,
      Map<String, String> targetIdealStateSegmentHosts, IdealState idealStateToUpdate,
      boolean downtime, int minAvailableReplicas) {

    LOGGER.debug("Will update instance map of segment: {}", segmentId);

    if (currentIdealStateSegmentHosts == null) {
      //segment can be missing if retention manager has deleted it
      LOGGER.debug("Segment " + segmentId + " missing from current idealState. Skipping it.");
      return;
    }

    // dump additional detailed info for debugging
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Current hosts and states for segment {}", segmentId);
      prettyPrintMap(currentIdealStateSegmentHosts, Level.DEBUG);
      LOGGER.debug("Target hosts and states for segment {}", segmentId);
      prettyPrintMap(targetIdealStateSegmentHosts, Level.DEBUG);
    }

    MapDifference difference = Maps.difference(targetIdealStateSegmentHosts, currentIdealStateSegmentHosts);

    if (downtime) {
      // in downtime mode, set the current ideal state to target at one go
      LOGGER.debug("Downtime mode is enabled. Will set to target state at one go");
      setTargetState(idealStateToUpdate, segmentId, targetIdealStateSegmentHosts);
      ++_rebalancerStats.directUpdatesToSegmentInstanceMap;
      _rebalancerStats.numSegmentMoves += targetIdealStateSegmentHosts.size();
      return;
    }

    // we are in no-downtime mode
    if (minAvailableReplicas >= currentIdealStateSegmentHosts.size()) {
      // if the minimum number of serving replicas in rebalance config is
      // greater than or equal to number of replicas of a segment, then it
      // is impossible to honor the request. so we use the default number
      // of minimum serving replicas we will keep for no downtime mode
      // (currently 1)
      LOGGER.debug("Unable to keep {} min replicas alive as the segment {} has {} replicas",
          minAvailableReplicas, segmentId, currentIdealStateSegmentHosts.size());
      minAvailableReplicas = RebalanceUserConfigConstants.DEFAULT_MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME;
    }

    if (difference.entriesInCommon().size() >= minAvailableReplicas) {
      // if there are enough hosts in common between current and target ideal states
      // to satisfy the min replicas condition, then there won't be any downtime
      // and we can directly set the current ideal state to target ideal state
      LOGGER.debug("Current and target ideal states have sufficient common hosts. Will set to target state at one go");
      setTargetState(idealStateToUpdate, segmentId, targetIdealStateSegmentHosts);
      ++_rebalancerStats.directUpdatesToSegmentInstanceMap;
      _rebalancerStats.numSegmentMoves += difference.entriesOnlyOnLeft().size();
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
        LOGGER.debug("Removing host: {} for segment: {}", hostToRemove, segmentId);
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
      LOGGER.debug("Adding " + hostToAdd + " to serve segment " + segmentId);
      ++_rebalancerStats.incrementalUpdatesToSegmentInstanceMap;
      ++_rebalancerStats.numSegmentMoves;

      // dump additional detailed info for debugging
      if (LOGGER.isDebugEnabled()) {
        final Map<String, String> updatedSegmentHosts = idealStateToUpdate.getInstanceStateMap(segmentId);
        LOGGER.debug("Updated hosts and states for segment {}", segmentId);
        prettyPrintMap(updatedSegmentHosts, Level.DEBUG);
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

  private boolean preCheckErrorInExternalView(final String tableName) {
    ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
    LOGGER.info("Checking if external view is already in ERROR state before rebalancing table {}", tableName);

    if (externalView == null) {
      LOGGER.info("External view is null for table {}", tableName);
      return true;
    }

    Map<String, Map<String, String>> segmentsAndHosts = externalView.getRecord().getMapFields();
    for (String segment : segmentsAndHosts.keySet()) {
      Map<String, String> hostsAndStates = segmentsAndHosts.get(segment);
      for (String server : hostsAndStates.keySet()) {
        if (hostsAndStates.get(server).equalsIgnoreCase("error")) {
          LOGGER.error("Detected error state for segment {} for server {}", segment, server);
          prettyPrintMap(hostsAndStates, Level.ERROR);
          return false;
        }
      }
    }

    LOGGER.info("External view is fine for table {}", tableName);
    return true;
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
    int segmentsNotConverged = 0;

    boolean stable = true;
    for (String segment : mapFieldsIS.keySet()) {
      Map<String, String> mapIS = mapFieldsIS.get(segment);
      Map<String, String> mapEV = mapFieldsEV.get(segment);
      boolean converged = true;

      if (mapEV == null) {
        LOGGER.info("Host-state mapping of segment {} not yet available in external view", segment);
        // we have found that external view hasn't yet converged to ideal state.
        // still go on to check for other segments just so that we can dump debug
        // messages or we can detect error. that's why we don't return
        // false immediately
        stable = false;
        ++segmentsNotConverged;
        continue;
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Hosts and states for segment {} in ideal state", segment);
        prettyPrintMap(mapIS, Level.DEBUG);
        LOGGER.debug("Hosts and states for segment {} in external view", segment);
        prettyPrintMap(mapEV, Level.DEBUG);
      }

      for (String server : mapIS.keySet()) {
        if (!mapEV.containsKey(server)) {
          LOGGER.info("Host-state mapping of segment {} doesn't yet have server {} in external view",
              segment, server);
          // external view not yet converged
          stable = false;
          converged = false;
        } else if (mapEV.get(server).equalsIgnoreCase("error")) {
          LOGGER.error("Detected error state for segment {} for server {}", segment, server);
          prettyPrintMap(mapIS, Level.ERROR);
          prettyPrintMap(mapEV, Level.ERROR);
          throw new IllegalStateException("External view reports error state for segment " + segment + " for host " + server,
              new ExternalViewErrored());
        } else {
          final String stateInIdealState = mapIS.get(server);
          final String stateInExternalView = mapEV.get(server);
          if (!stateInIdealState.equalsIgnoreCase(stateInExternalView)) {
            LOGGER.info("Host-state mapping of segment {} has state {} in external view and state {} in ideal state",
                segment, stateInExternalView, stateInIdealState);
            // external view not yet converged
            stable = false;
            converged = false;
          }
        }
      }

      if (!converged) {
        segmentsNotConverged++;
      }
    }

    LOGGER.info("{} of total {} segments from ideal state don't yet have external view converged",
        segmentsNotConverged, mapFieldsIS.size());
    return stable;
  }

  /**
   * Pretty print a map
   * @param map hashmap
   * @param level logging level
   */
  private static void prettyPrintMap(final Map<String, String> map, final Level level) {
    if (map.size() > 0) {
      final Joiner.MapJoiner mapJoiner = Joiner.on(",").withKeyValueSeparator(":");
      if (level == Level.DEBUG) {
        LOGGER.debug(mapJoiner.join(map));
      } else if (level == Level.ERROR) {
        LOGGER.error(mapJoiner.join(map));
      } else if (level == Level.INFO) {
        LOGGER.info(mapJoiner.join(map));
      }
    }
  }

  /**
   * Wait till state has stabilized {@link #isStable(String)}
   */
  private void waitForStable(final String resourceName) {
    int INITIAL_WAIT_MS = 3000;
    int wait = 0;
    boolean done = false;
    final int maxWaitTimeMillis = EXTERNVAL_VIEW_STABILIZATON_MAX_WAIT_MINS * 60 * 1000;
    try {
      Thread.sleep(INITIAL_WAIT_MS);
      // the isStable method will bail out on detecting ERROR state
      // in external view. However, if the external view never converges
      // (for some reason) and we don't see ERROR state as well, then we
      // don't want to wait indefinitely for the external view to converge
      // thus, we use a fix max wait period  and bail out if external
      // view hasn't converged within that time period
      while (wait < maxWaitTimeMillis) {
        if (isStable(resourceName)) {
          done = true;
          break;
        } else {
          LOGGER.info("Waiting for externalView to match idealstate for table:" + resourceName);
          Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
          wait += EXTERNAL_VIEW_CHECK_INTERVAL_MS;
          }
        }
      } catch (InterruptedException e) {
      LOGGER.error("Rebalancer got interrupted while waiting for external view to converge");
      Thread.currentThread().interrupt();
    }

    if (!done) {
      LOGGER.error("External view didn't converge for table {} within max wait time of {} mins. Bailing out",
          resourceName, EXTERNVAL_VIEW_STABILIZATON_MAX_WAIT_MINS);
      throw new IllegalStateException("External view didn't converge for table " + resourceName +
          " within max wait period of " + EXTERNVAL_VIEW_STABILIZATON_MAX_WAIT_MINS + " mins",
          new ExternalViewConvergeTimeout());
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
    private int numSegmentMoves;

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
     * mapping acrosss all segments in ideal state to target ideal state
     * mapping in one step. This happens in downtime rebalancing
     * and can also happen in no-downtime rebalancing if there are
     * sufficient common hosts between current and target ideal states
     *
     * Note that this does not reflect the number of times we wrote
     * the changed ideal state to ZK. Use getUpdatesToIdealStateInZK
     * for that. This counter is for the direct changes (cumulative across
     * segments) to in-progress ideal state which we later attempt to
     * write in ZK
     *
     * @return direct updates
     */
    public int getDirectUpdatesToSegmentInstanceMap() {
      return directUpdatesToSegmentInstanceMap;
    }

    /**
     * Get the number of times we updated the instance-state
     * mapping across all segments in ideal state incrementally
     * by removing a host from current ideal state and adding
     * a host from target ideal state to keep the requirement of
     * having some number of serving replicas up for each segment
     * @return incremental updates
     *
     * Note that this does not reflect the number of times we wrote
     * the changed ideal state to ZK. Use getUpdatesToIdealStateInZK
     * for that. This counter is for the incremental changes (cumulative across
     * segments) to in-progress ideal state which we later attempt to
     * write in ZK
     */
    public int getIncrementalUpdatesToSegmentInstanceMap() {
      return incrementalUpdatesToSegmentInstanceMap;
    }

    /**
     * Get the total number of segment movements
     * @return number of times segments were moved
     * from one host to another
     */
    public int getNumSegmentMoves() {
      return numSegmentMoves;
    }
  }

  public RebalancerStats getRebalancerStats() {
    return _rebalancerStats;
  }

  private static class ExternalViewErrored extends Throwable {

  }

  private static class ExternalViewConvergeTimeout extends Throwable {

  }
}
