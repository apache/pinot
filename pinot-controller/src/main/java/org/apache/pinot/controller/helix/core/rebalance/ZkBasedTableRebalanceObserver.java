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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
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
  private final TableRebalanceProgressStats _tableRebalanceProgressStats;
  private final TableRebalanceContext _tableRebalanceContext;
  private long _lastUpdateTimeMs;
  // Keep track of number of updates. Useful during debugging.
  private int _numUpdatesToZk;
  private boolean _isStopped = false;
  private RebalanceResult.Status _stopStatus;

  private final ControllerMetrics _controllerMetrics;

  public ZkBasedTableRebalanceObserver(String tableNameWithType, String rebalanceJobId,
      TableRebalanceContext tableRebalanceContext, PinotHelixResourceManager pinotHelixResourceManager) {
    Preconditions.checkState(tableNameWithType != null, "Table name cannot be null");
    Preconditions.checkState(rebalanceJobId != null, "rebalanceId cannot be null");
    Preconditions.checkState(pinotHelixResourceManager != null, "PinotHelixManager cannot be null");
    _tableNameWithType = tableNameWithType;
    _rebalanceJobId = rebalanceJobId;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _tableRebalanceProgressStats = new TableRebalanceProgressStats();
    _tableRebalanceContext = tableRebalanceContext;
    _numUpdatesToZk = 0;
    _controllerMetrics = ControllerMetrics.get();
  }

  @Override
  public void onTrigger(Trigger trigger, Map<String, Map<String, String>> currentState,
      Map<String, Map<String, String>> targetState) {
    boolean updatedStatsInZk = false;
    _controllerMetrics.setValueOfTableGauge(_tableNameWithType, ControllerGauge.TABLE_REBALANCE_IN_PROGRESS, 1);
    switch (trigger) {
      case START_TRIGGER:
        updateOnStart(currentState, targetState);
        trackStatsInZk();
        updatedStatsInZk = true;
        break;
      // Write to Zk if there's change since previous stats computation
      case IDEAL_STATE_CHANGE_TRIGGER:
        TableRebalanceProgressStats.RebalanceStateStats latest =
            getDifferenceBetweenTableRebalanceStates(targetState, currentState);
        if (TableRebalanceProgressStats.statsDiffer(_tableRebalanceProgressStats.getCurrentToTargetConvergence(),
            latest)) {
          _tableRebalanceProgressStats.setCurrentToTargetConvergence(latest);
          trackStatsInZk();
          updatedStatsInZk = true;
        }
        break;
      case EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER:
        latest = getDifferenceBetweenTableRebalanceStates(targetState, currentState);
        if (TableRebalanceProgressStats.statsDiffer(
            _tableRebalanceProgressStats.getExternalViewToIdealStateConvergence(), latest)) {
          _tableRebalanceProgressStats.setExternalViewToIdealStateConvergence(latest);
          trackStatsInZk();
          updatedStatsInZk = true;
        }
        break;
      default:
        throw new IllegalArgumentException("Unimplemented trigger: " + trigger);
    }
    // The onTrigger method is mainly driven by the while loop of waiting for external view to converge to ideal
    // state. That while loop wait for at least externalViewCheckIntervalInMs. So the real interval to send out
    // heartbeat is the max(heartbeat_interval, externalViewCheckIntervalInMs);
    long heartbeatIntervalInMs = _tableRebalanceContext.getConfig().getHeartbeatIntervalInMs();
    if (!updatedStatsInZk && System.currentTimeMillis() - _lastUpdateTimeMs > heartbeatIntervalInMs) {
      LOGGER.debug("Update status of rebalance job: {} for table: {} after {}ms as heartbeat", _rebalanceJobId,
          _tableNameWithType, heartbeatIntervalInMs);
      trackStatsInZk();
    }
  }

  private void updateOnStart(Map<String, Map<String, String>> currentState,
      Map<String, Map<String, String>> targetState) {
    Preconditions.checkState(RebalanceResult.Status.IN_PROGRESS != _tableRebalanceProgressStats.getStatus(),
        "Rebalance Observer onStart called multiple times");
    _tableRebalanceProgressStats.setStatus(RebalanceResult.Status.IN_PROGRESS);
    _tableRebalanceProgressStats.setInitialToTargetStateConvergence(
        getDifferenceBetweenTableRebalanceStates(targetState, currentState));
    _tableRebalanceProgressStats.setStartTimeMs(System.currentTimeMillis());
  }

  @Override
  public void onSuccess(String msg) {
    Preconditions.checkState(RebalanceResult.Status.DONE != _tableRebalanceProgressStats.getStatus(),
        "Table Rebalance already completed");
    _controllerMetrics.setValueOfTableGauge(_tableNameWithType, ControllerGauge.TABLE_REBALANCE_IN_PROGRESS, 0);
    long timeToFinishInSeconds = (System.currentTimeMillis() - _tableRebalanceProgressStats.getStartTimeMs()) / 1000L;
    _tableRebalanceProgressStats.setCompletionStatusMsg(msg);
    _tableRebalanceProgressStats.setTimeToFinishInSeconds(timeToFinishInSeconds);
    _tableRebalanceProgressStats.setStatus(RebalanceResult.Status.DONE);
    // Zero out the in_progress convergence stats
    TableRebalanceProgressStats.RebalanceStateStats stats = new TableRebalanceProgressStats.RebalanceStateStats();
    _tableRebalanceProgressStats.setExternalViewToIdealStateConvergence(stats);
    _tableRebalanceProgressStats.setCurrentToTargetConvergence(stats);
    trackStatsInZk();
  }

  @Override
  public void onError(String errorMsg) {
    _controllerMetrics.setValueOfTableGauge(_tableNameWithType, ControllerGauge.TABLE_REBALANCE_IN_PROGRESS, 0);
    long timeToFinishInSeconds = (System.currentTimeMillis() - _tableRebalanceProgressStats.getStartTimeMs()) / 1000;
    _tableRebalanceProgressStats.setTimeToFinishInSeconds(timeToFinishInSeconds);
    _tableRebalanceProgressStats.setStatus(RebalanceResult.Status.FAILED);
    _tableRebalanceProgressStats.setCompletionStatusMsg(errorMsg);
    trackStatsInZk();
  }

  @Override
  public boolean isStopped() {
    return _isStopped;
  }

  @Override
  public RebalanceResult.Status getStopStatus() {
    return _stopStatus;
  }

  public int getNumUpdatesToZk() {
    return _numUpdatesToZk;
  }

  @VisibleForTesting
  TableRebalanceContext getTableRebalanceContext() {
    return _tableRebalanceContext;
  }

  private void trackStatsInZk() {
    Map<String, String> jobMetadata =
        createJobMetadata(_tableNameWithType, _rebalanceJobId, _tableRebalanceProgressStats, _tableRebalanceContext);
    _pinotHelixResourceManager.addControllerJobToZK(_rebalanceJobId, jobMetadata, ControllerJobType.TABLE_REBALANCE,
        prevJobMetadata -> {
          // In addition to updating job progress status, the observer also checks if the job status is IN_PROGRESS.
          // If not, then no need to update the job status, and we keep this status to end the job promptly.
          if (prevJobMetadata == null) {
            return true;
          }
          String prevStatsInStr = prevJobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS);
          TableRebalanceProgressStats prevStats;
          try {
            prevStats = JsonUtils.stringToObject(prevStatsInStr, TableRebalanceProgressStats.class);
          } catch (JsonProcessingException ignore) {
            return true;
          }
          if (prevStats == null || RebalanceResult.Status.IN_PROGRESS == prevStats.getStatus()) {
            return true;
          }
          _isStopped = true;
          _stopStatus = prevStats.getStatus();
          LOGGER.warn("Rebalance job: {} for table: {} has already stopped with status: {}", _rebalanceJobId,
              _tableNameWithType, _stopStatus);
          // No need to update job status if job has ended. This also keeps the last status from being overwritten.
          return false;
        });
    _numUpdatesToZk++;
    _lastUpdateTimeMs = System.currentTimeMillis();
    LOGGER.debug("Made {} ZK updates for rebalance job: {} of table: {}", _numUpdatesToZk, _rebalanceJobId,
        _tableNameWithType);
  }

  @VisibleForTesting
  static Map<String, String> createJobMetadata(String tableNameWithType, String jobId,
      TableRebalanceProgressStats tableRebalanceProgressStats, TableRebalanceContext tableRebalanceContext) {
    Map<String, String> jobMetadata = new HashMap<>();
    jobMetadata.put(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE, tableNameWithType);
    jobMetadata.put(CommonConstants.ControllerJob.JOB_ID, jobId);
    jobMetadata.put(CommonConstants.ControllerJob.SUBMISSION_TIME_MS, Long.toString(System.currentTimeMillis()));
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobType.TABLE_REBALANCE.name());
    try {
      jobMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS,
          JsonUtils.objectToString(tableRebalanceProgressStats));
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising stats for rebalance job: {} of table: {} to keep in ZK", jobId, tableNameWithType,
          e);
    }
    try {
      jobMetadata.put(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT,
          JsonUtils.objectToString(tableRebalanceContext));
    } catch (JsonProcessingException e) {
      LOGGER.error("Error serialising retry configs for rebalance job: {} of table: {} to keep in ZK", jobId,
          tableNameWithType, e);
    }
    return jobMetadata;
  }

  /**
   * Takes in targetState and sourceState and computes stats based on the comparison between sourceState and
   * targetState.This captures how far the source state is from the target state. Example - if there are 4 segments and
   * 16 replicas in the source state not matching the target state, _segmentsToRebalance is 4 and _replicasToRebalance
   * is 16.
   *
   * @param targetState - The state that we want to get to
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

