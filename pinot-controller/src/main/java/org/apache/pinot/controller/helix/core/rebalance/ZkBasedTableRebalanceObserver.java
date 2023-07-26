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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>ZkBasedTableRebalanceObserver</code> observes rebalance progress and tracks rebalance status,
 * stats in Zookeeper. This will be used to show the progress of rebalance to users via rebalanceStatus API.
 */
public class ZkBasedTableRebalanceObserver implements TableRebalanceObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkBasedTableRebalanceObserver.class);
  private final String _tableNameWithType;
  private final String _rebalanceJobId;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private TableRebalanceProgressStats _tableRebalanceProgressStats;
  // Keep track of number of updates. Useful during debugging.
  private int _numUpdatesToZk;

  public ZkBasedTableRebalanceObserver(String tableNameWithType, String rebalanceJobId,
      PinotHelixResourceManager pinotHelixResourceManager) {
    Preconditions.checkState(tableNameWithType != null, "Table name cannot be null");
    Preconditions.checkState(rebalanceJobId != null, "rebalanceId cannot be null");
    Preconditions.checkState(pinotHelixResourceManager != null, "PinotHelixManager cannot be null");
    _tableNameWithType = tableNameWithType;
    _rebalanceJobId = rebalanceJobId;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _tableRebalanceProgressStats = new TableRebalanceProgressStats();
    _numUpdatesToZk = 0;
  }

  @Override
  public void onTrigger(Trigger trigger, Map<String, Map<String, String>> currentState,
      Map<String, Map<String, String>> targetState) {
    switch (trigger) {
      case START_TRIGGER:
        updateOnStart(currentState, targetState);
        trackStatsInZk();
        break;
      // Write to Zk if there's change since previous stats computation
      case IDEAL_STATE_CHANGE_TRIGGER:
        TableRebalanceProgressStats.RebalanceStateStats latest =
            getDifferenceBetweenTableRebalanceStates(targetState, currentState);
        if (TableRebalanceProgressStats.statsDiffer(_tableRebalanceProgressStats.getCurrentToTargetConvergence(),
            latest)) {
          _tableRebalanceProgressStats.setCurrentToTargetConvergence(latest);
          trackStatsInZk();
        }
        break;
      case EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER:
        latest = getDifferenceBetweenTableRebalanceStates(targetState, currentState);
        if (TableRebalanceProgressStats.statsDiffer(
            _tableRebalanceProgressStats.getExternalViewToIdealStateConvergence(), latest)) {
          _tableRebalanceProgressStats.setExternalViewToIdealStateConvergence(latest);
          trackStatsInZk();
        }
        break;
      default:
        throw new IllegalArgumentException("Unimplemented trigger: " + trigger);
    }
  }

  private void updateOnStart(Map<String, Map<String, String>> currentState,
      Map<String, Map<String, String>> targetState) {
    Preconditions.checkState(_tableRebalanceProgressStats.getStatus() != RebalanceResult.Status.IN_PROGRESS.toString(),
        "Rebalance Observer onStart called multiple times");
    _tableRebalanceProgressStats.setStatus(RebalanceResult.Status.IN_PROGRESS.toString());
    _tableRebalanceProgressStats.setInitialToTargetStateConvergence(
        getDifferenceBetweenTableRebalanceStates(targetState, currentState));
    _tableRebalanceProgressStats.setStartTimeMs(System.currentTimeMillis());
  }

  @Override
  public void onSuccess(String msg) {
    Preconditions.checkState(_tableRebalanceProgressStats.getStatus() != RebalanceResult.Status.DONE.toString(),
        "Table Rebalance already completed");
    long timeToFinishInSeconds =
         (System.currentTimeMillis() - _tableRebalanceProgressStats.getStartTimeMs()) / 1000L;
    _tableRebalanceProgressStats.setCompletionStatusMsg(msg);
    _tableRebalanceProgressStats.setTimeToFinishInSeconds(timeToFinishInSeconds);
    _tableRebalanceProgressStats.setStatus(RebalanceResult.Status.DONE.toString());
    // Zero out the in_progress convergence stats
    TableRebalanceProgressStats.RebalanceStateStats stats = new TableRebalanceProgressStats.RebalanceStateStats();
    _tableRebalanceProgressStats.setExternalViewToIdealStateConvergence(stats);
    _tableRebalanceProgressStats.setCurrentToTargetConvergence(stats);
    trackStatsInZk();
  }

  @Override
  public void onError(String errorMsg) {
    long timeToFinishInSeconds =
        (long) (System.currentTimeMillis() - _tableRebalanceProgressStats.getStartTimeMs()) / 1000;
    _tableRebalanceProgressStats.setTimeToFinishInSeconds(timeToFinishInSeconds);
    _tableRebalanceProgressStats.setStatus(RebalanceResult.Status.FAILED.toString());
    _tableRebalanceProgressStats.setCompletionStatusMsg(errorMsg);
    trackStatsInZk();
  }

  public int getNumUpdatesToZk() {
    return _numUpdatesToZk;
  }

  private void trackStatsInZk() {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE, _tableNameWithType);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, _rebalanceJobId);
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(System.currentTimeMillis()));
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobType.TABLE_REBALANCE.name());
    try {
      jobMetadata.put(RebalanceConfigConstants.REBALANCE_PROGRESS_STATS,
          JsonUtils.objectToString(_tableRebalanceProgressStats));
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising rebalance stats to JSON for persisting to ZK {}", _rebalanceJobId, e);
    }
    _pinotHelixResourceManager.addControllerJobToZK(_rebalanceJobId, jobMetadata,
        ZKMetadataProvider.constructPropertyStorePathForControllerJob(ControllerJobType.TABLE_REBALANCE));
    _numUpdatesToZk++;
    LOGGER.debug("Number of updates to Zk: {} for rebalanceJob: {}  ", _numUpdatesToZk, _rebalanceJobId);
  }

  /**
   * Takes in targetState and sourceState and computes stats based on the comparison
   * between sourceState and targetState.This captures how far the source state is from
   * the target state. Example - if there are 4 segments and 16 replicas in the source state
   * not matching the target state, _segmentsToRebalance is 4 and _replicasToRebalance is 16.
   * @param targetState- The state that we want to get to
   * @param sourceState - A given state that needs to converge to targetState
   * @return RebalanceStats
   */
  public static TableRebalanceProgressStats.RebalanceStateStats getDifferenceBetweenTableRebalanceStates(
      Map<String, Map<String, String>> targetState, Map<String, Map<String, String>> sourceState) {

    TableRebalanceProgressStats.RebalanceStateStats rebalanceStats =
        new TableRebalanceProgressStats.RebalanceStateStats();

    for (Map.Entry<String, Map<String, String>> entry : targetState.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> sourceInstanceStateMap = sourceState.get(segmentName);
      if (sourceInstanceStateMap == null) {
        // Skip the missing segment
        rebalanceStats._segmentsMissing++;
        rebalanceStats._segmentsToRebalance++;
        continue;
      }
      Map<String, String> targetStateInstanceStateMap = entry.getValue();
      boolean hasSegmentConverged = true;
      for (Map.Entry<String, String> instanceStateEntry : targetStateInstanceStateMap.entrySet()) {
        // Ignore OFFLINE state in target state
        String targetStateInstanceState = instanceStateEntry.getValue();
        if (targetStateInstanceState.equals(CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE)) {
          continue;
        }
        // Check whether the instance state in source matches the target
        String instanceName = instanceStateEntry.getKey();
        String sourceInstanceState = sourceInstanceStateMap.get(instanceName);
        if (!targetStateInstanceState.equals(sourceInstanceState)) {
          rebalanceStats._replicasToRebalance++;
          hasSegmentConverged = false;
        }
      }
      if (!hasSegmentConverged) {
        rebalanceStats._segmentsToRebalance++;
      }
    }
    int totalSegments = targetState.size();
    rebalanceStats._percentSegmentsToRebalance =
        (totalSegments == 0) ? 0 : ((double) rebalanceStats._segmentsToRebalance / totalSegments) * 100.0;
    return rebalanceStats;
  }
}
