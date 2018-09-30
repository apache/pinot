/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.partition.PartitionAssignment;
import com.linkedin.pinot.common.restlet.resources.RebalanceResult;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategy;
import com.linkedin.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;
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

  public TableRebalancer(HelixManager mgr, HelixAdmin admin, String helixClusterName) {
    _helixManager = mgr;
    _helixAdmin = admin;
    _helixClusterName = helixClusterName;
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
   * Note: we don't use {@link com.linkedin.pinot.common.utils.helix.HelixHelper} directly as we would like to manage
   * the main logic and retries according to the rebalance algorithm. Some amount of code is duplicated from HelixHelper.
   */
  public RebalanceResult rebalance(TableConfig tableConfig, RebalanceSegmentStrategy strategy,
      Configuration rebalanceConfig) throws InvalidConfigException{

    RebalanceResult result = new RebalanceResult();

    String tableName = tableConfig.getTableName();
    HelixDataAccessor dataAccessor = _helixManager.getHelixDataAccessor();
    ZkBaseDataAccessor zkBaseDataAccessor = (ZkBaseDataAccessor) dataAccessor.getBaseDataAccessor();

    PropertyKey idealStateKey = dataAccessor.keyBuilder().idealStates(tableName);
    IdealState previousIdealState = dataAccessor.getProperty(idealStateKey);

    if(rebalanceConfig.getBoolean(RebalanceUserConfigConstants.DRYRUN, RebalanceUserConfigConstants.DEFAULT_DRY_RUN)) {
      PartitionAssignment partitionAssignment = strategy.rebalancePartitionAssignment(previousIdealState, tableConfig,
          rebalanceConfig );
      IdealState idealState = strategy.getRebalancedIdealState(previousIdealState, tableConfig, rebalanceConfig,
          partitionAssignment);
      result.setIdealStateMapping(idealState.getRecord().getMapFields());
      result.setPartitionAssignment(partitionAssignment);
      return result;
    }

    long startTime = System.nanoTime();
    IdealState targetIdealState = null;
    PartitionAssignment targetPartitionAssignment = null;

    long retryDelayMs = 60000; // 1 min
    int retries = 0;
    while (true) {
      IdealState currentIdealState = dataAccessor.getProperty(idealStateKey);

      if (targetIdealState == null || !EqualityUtils.isEqual(previousIdealState, currentIdealState)) {
        LOGGER.info("Computing new rebalanced state for table {}", tableName);

        // we need to recompute target state

        // Make a copy of the the idealState above to pass it to the rebalancer
        // NOTE: new IdealState(idealState.getRecord()) does not work because it's shallow copy for map fields and
        // list fields
        IdealState idealStateCopy = HelixHelper.cloneIdealState(currentIdealState);
        // rebalance; can throw InvalidConfigException - in which case we bail out
        targetPartitionAssignment = strategy.rebalancePartitionAssignment(previousIdealState, tableConfig,
            rebalanceConfig );
        targetIdealState = strategy.getRebalancedIdealState(idealStateCopy, tableConfig, rebalanceConfig,
            targetPartitionAssignment);
      }

      if (EqualityUtils.isEqual(targetIdealState, currentIdealState)) {
        LOGGER.info("Table {} is rebalanced.", tableName);

        LOGGER.info("Finished rebalancing table {} in {} ms.", tableName,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
        result.setIdealStateMapping(targetIdealState.getRecord().getMapFields());
        result.setPartitionAssignment(targetPartitionAssignment);
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
        LOGGER.info("Updating IdealState for table {}", tableName);
        if (zkBaseDataAccessor.set(idealStateKey.getPath(), nextIdealState.getRecord(),
            currentIdealState.getRecord().getVersion(), AccessOption.PERSISTENT)) {
          // if we succeeded, wait for the change to stabilize
          waitForStable(tableName);
          // clear retries as it tracks failures with each idealstate update attempt
          retries = 0;
          continue;
        }
        // in case of any error, we retry a bounded number of types
      } catch (ZkBadVersionException e) {
        LOGGER.warn("Version changed while updating ideal state for resource: {}", tableName);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while updating ideal state for resource: {}", tableName, e);
      }

      previousIdealState = currentIdealState;
      if (retries++ > MAX_RETRIES) {
        LOGGER.error("Unable to rebalance table {} in {} attempts. Giving up", tableName, MAX_RETRIES);
        return result;
      }
      // wait before retrying
      try {
        Thread.sleep(retryDelayMs);
      } catch (InterruptedException e) {
        LOGGER.error("Got interrupted while rebalancing table {}", tableName);
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
   * Updates a segment mapping if needed. In "downtime" mode or if there are common elements between source and
   * target mapping, the segment mapping is set to the target mapping directly.
   * In a no-downtime, if there are no commmon elements, one element of the source mapping is replaced with one
   * from the target mapping.
   */
  @VisibleForTesting
  public void updateSegmentIfNeeded(String segmentId, Map<String, String> srcMap, Map<String, String> targetMap,
      IdealState idealState, Configuration rebalanceUserConfig) {

    if (srcMap == null) {
      //segment can be missing if retention manager has deleted it
      LOGGER.info("Segment " + segmentId + " missing from current idealState. Skipping it.");
      return;
    }

    if (rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.DOWNTIME,
        RebalanceUserConfigConstants.DEFAULT_DOWNTIME)) {
      setTargetState(idealState, segmentId, targetMap);
      return;
    }

    MapDifference difference = Maps.difference(targetMap, srcMap);
    if (!difference.entriesInCommon().isEmpty()) {
      // if there are entries in common, there won't be downtime
      LOGGER.debug("Segment " + segmentId + " has common entries between current and expected ideal state");
      setTargetState(idealState, segmentId, targetMap);
    } else {
      // remove one entry
      idealState.getInstanceStateMap(segmentId).remove(srcMap.keySet().stream().findFirst().get());
      // add an entry from the target state to ensure there is no downtime
      String instanceId = targetMap.keySet().stream().findFirst().get();
      idealState.setPartitionState(segmentId, instanceId, targetMap.get(instanceId));
      LOGGER.debug("Adding " + instanceId + " to serve segment " + segmentId);
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
  private void waitForStable(String resourceName) throws InterruptedException {
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
}
