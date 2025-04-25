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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
  // These previous stats are used for rollback scenarios where the IdealState update fails dure to a version
  // change and the rebalance loop is retried.
  private TableRebalanceProgressStats.RebalanceProgressStats _previousStepStats;
  private TableRebalanceProgressStats.RebalanceProgressStats _previousOverallStats;
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
    _previousStepStats = new TableRebalanceProgressStats.RebalanceProgressStats();
    _previousOverallStats = new TableRebalanceProgressStats.RebalanceProgressStats();
    _numUpdatesToZk = 0;
    _controllerMetrics = ControllerMetrics.get();
  }

  @Override
  public void onTrigger(Trigger trigger, Map<String, Map<String, String>> currentState,
      Map<String, Map<String, String>> targetState, RebalanceContext rebalanceContext) {
    boolean updatedStatsInZk = false;
    _controllerMetrics.setValueOfTableGauge(_tableNameWithType, ControllerGauge.TABLE_REBALANCE_IN_PROGRESS, 1);
    TableRebalanceProgressStats.RebalanceStateStats latest;
    TableRebalanceProgressStats.RebalanceProgressStats latestProgress;
    switch (trigger) {
      case START_TRIGGER:
        updateOnStart(currentState, targetState, rebalanceContext);
        emitProgressMetric(_tableRebalanceProgressStats.getRebalanceProgressStatsOverall());
        trackStatsInZk();
        updatedStatsInZk = true;
        break;
      // Write to Zk if there's change since previous stats computation
      case IDEAL_STATE_CHANGE_TRIGGER:
        // Update the previous stats with the current values in case a rollback is needed due to IdealState version
        // change
        _previousOverallStats = new TableRebalanceProgressStats.RebalanceProgressStats(
            _tableRebalanceProgressStats.getRebalanceProgressStatsOverall());
        latest = getDifferenceBetweenTableRebalanceStates(targetState, currentState);
        latestProgress = calculateUpdatedProgressStats(targetState, currentState, rebalanceContext,
            Trigger.IDEAL_STATE_CHANGE_TRIGGER, _tableRebalanceProgressStats);
        if (TableRebalanceProgressStats.statsDiffer(_tableRebalanceProgressStats.getCurrentToTargetConvergence(),
            latest) || !_tableRebalanceProgressStats.getRebalanceProgressStatsOverall().equals(latestProgress)) {
          if (TableRebalanceProgressStats.statsDiffer(
              _tableRebalanceProgressStats.getExternalViewToIdealStateConvergence(), latest)) {
            _tableRebalanceProgressStats.setCurrentToTargetConvergence(latest);
          }
          if (!_tableRebalanceProgressStats.getRebalanceProgressStatsOverall().equals(latestProgress)) {
            _tableRebalanceProgressStats.setRebalanceProgressStatsOverall(latestProgress);
            emitProgressMetric(latestProgress);
          }
          trackStatsInZk();
          updatedStatsInZk = true;
        }
        break;
      case EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER:
        // Update the previous stats with the current values in case a rollback is needed due to IdealState version
        // change
        _previousStepStats = new TableRebalanceProgressStats.RebalanceProgressStats(
            _tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep());
        _previousOverallStats = new TableRebalanceProgressStats.RebalanceProgressStats(
            _tableRebalanceProgressStats.getRebalanceProgressStatsOverall());
        latest = getDifferenceBetweenTableRebalanceStates(targetState, currentState);
        latestProgress = calculateUpdatedProgressStats(targetState, currentState, rebalanceContext,
            Trigger.EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER, _tableRebalanceProgressStats);
        if (TableRebalanceProgressStats.statsDiffer(
            _tableRebalanceProgressStats.getExternalViewToIdealStateConvergence(), latest)
            || !_tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep().equals(latestProgress)) {
          if (TableRebalanceProgressStats.statsDiffer(
              _tableRebalanceProgressStats.getExternalViewToIdealStateConvergence(), latest)) {
            _tableRebalanceProgressStats.setExternalViewToIdealStateConvergence(latest);
          }
          if (!_tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep().equals(latestProgress)) {
            _tableRebalanceProgressStats.updateOverallAndStepStatsFromLatestStepStats(latestProgress);
          }
          emitProgressMetric(_tableRebalanceProgressStats.getRebalanceProgressStatsOverall());
          trackStatsInZk();
          updatedStatsInZk = true;
        }
        break;
      case NEXT_ASSINGMENT_CALCULATION_TRIGGER:
        // Update the previous stats with the current values in case a rollback is needed due to IdealState version
        // change
        _previousStepStats = new TableRebalanceProgressStats.RebalanceProgressStats(
            _tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep());
        latestProgress = calculateUpdatedProgressStats(targetState, currentState, rebalanceContext,
            Trigger.NEXT_ASSINGMENT_CALCULATION_TRIGGER, _tableRebalanceProgressStats);
        if (!_tableRebalanceProgressStats.getRebalanceProgressStatsCurrentStep().equals(latestProgress)) {
          _tableRebalanceProgressStats.setRebalanceProgressStatsCurrentStep(latestProgress);
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
      Map<String, Map<String, String>> targetState, RebalanceContext rebalanceContext) {
    Preconditions.checkState(RebalanceResult.Status.IN_PROGRESS != _tableRebalanceProgressStats.getStatus(),
        "Rebalance Observer onStart called multiple times");
    _tableRebalanceProgressStats.setStatus(RebalanceResult.Status.IN_PROGRESS);
    _tableRebalanceProgressStats.setInitialToTargetStateConvergence(
        getDifferenceBetweenTableRebalanceStates(targetState, currentState));
    _tableRebalanceProgressStats.setStartTimeMs(System.currentTimeMillis());
    _tableRebalanceProgressStats.setRebalanceProgressStatsOverall(calculateUpdatedProgressStats(targetState,
        currentState, rebalanceContext, Trigger.START_TRIGGER, _tableRebalanceProgressStats));
  }

  @Override
  public void onNoop(String msg) {
    _controllerMetrics.setValueOfTableGauge(_tableNameWithType, ControllerGauge.TABLE_REBALANCE_IN_PROGRESS, 0);
    long timeToFinishInSeconds = (System.currentTimeMillis() - _tableRebalanceProgressStats.getStartTimeMs()) / 1000L;
    _tableRebalanceProgressStats.setCompletionStatusMsg(msg);
    _tableRebalanceProgressStats.setTimeToFinishInSeconds(timeToFinishInSeconds);
    _tableRebalanceProgressStats.setStatus(RebalanceResult.Status.NO_OP);
    trackStatsInZk();
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
    TableRebalanceProgressStats.RebalanceProgressStats progressStats =
        new TableRebalanceProgressStats.RebalanceProgressStats();
    _tableRebalanceProgressStats.setRebalanceProgressStatsCurrentStep(progressStats);
    trackStatsInZk();
    emitProgressMetricDone();
  }

  @Override
  public void onError(String errorMsg) {
    _controllerMetrics.setValueOfTableGauge(_tableNameWithType, ControllerGauge.TABLE_REBALANCE_IN_PROGRESS, 0);
    long timeToFinishInSeconds = (System.currentTimeMillis() - _tableRebalanceProgressStats.getStartTimeMs()) / 1000L;
    _tableRebalanceProgressStats.setTimeToFinishInSeconds(timeToFinishInSeconds);
    _tableRebalanceProgressStats.setStatus(RebalanceResult.Status.FAILED);
    _tableRebalanceProgressStats.setCompletionStatusMsg(errorMsg);
    trackStatsInZk();
  }

  @Override
  public void onRollback() {
    _tableRebalanceProgressStats.setRebalanceProgressStatsCurrentStep(
        new TableRebalanceProgressStats.RebalanceProgressStats(_previousStepStats));
    _tableRebalanceProgressStats.setRebalanceProgressStatsOverall(
        new TableRebalanceProgressStats.RebalanceProgressStats(_previousOverallStats));
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

  /**
   * Emits the rebalance progress in percent to the metrics. Uses the percentage of remaining segments to be added as
   * the indicator of the overall progress.
   * Notice that for some jobs, the metrics may not be exactly accurate and would not be 100% when the job is done.
   * (e.g. when `lowDiskMode=false`, the job finishes without waiting for `totalRemainingSegmentsToBeDeleted` become
   * 0, or when `bestEffort=true` the job finishes without waiting for both `totalRemainingSegmentsToBeAdded`,
   * `totalRemainingSegmentsToBeDeleted`, and `totalRemainingSegmentsToConverge` become 0)
   * Therefore `emitProgressMetricDone()` should be called to emit the final progress as the time job exits.
   * @param overallProgress the latest overall progress
   */
  private void emitProgressMetric(TableRebalanceProgressStats.RebalanceProgressStats overallProgress) {
    // Round this up so the metric is 100 only when no segment remains
    long progressPercent = 100 - (long) Math.ceil(TableRebalanceProgressStats.calculatePercentageChange(
        overallProgress._totalSegmentsToBeAdded + overallProgress._totalSegmentsToBeDeleted,
        overallProgress._totalRemainingSegmentsToBeAdded + overallProgress._totalRemainingSegmentsToBeDeleted
            + overallProgress._totalRemainingSegmentsToConverge));
    // Using the original job ID to group rebalance retries together with the same label
    _controllerMetrics.setValueOfTableGauge(_tableNameWithType, ControllerGauge.TABLE_REBALANCE_JOB_PROGRESS_PERCENT,
        progressPercent < 0 ? 0 : progressPercent);
  }

  /**
   * Emits the rebalance progress as 100 (%) to the metrics. This is to ensure that the progress is at least aligned
   * when the job done to avoid confusion
   */
  private void emitProgressMetricDone() {
    _controllerMetrics.setValueOfTableGauge(_tableNameWithType, ControllerGauge.TABLE_REBALANCE_JOB_PROGRESS_PERCENT,
        100);
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
    jobMetadata.put(CommonConstants.ControllerJob.JOB_TYPE, ControllerJobType.TABLE_REBALANCE);
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

  @VisibleForTesting
  TableRebalanceProgressStats getTableRebalanceProgressStats() {
    return _tableRebalanceProgressStats;
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
  @VisibleForTesting
  static TableRebalanceProgressStats.RebalanceStateStats getDifferenceBetweenTableRebalanceStates(
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

  /**
   * Calculates the progress stats for the given step or for the overall based on the trigger type
   * @param targetAssignment target assignment (either updated IS or the target end IS depending on the step)
   * @param currentAssignment current assignment (either EV or the current IS depending on the step)
   * @param rebalanceContext rebalance context
   * @param trigger reason to trigger the stats update
   * @param rebalanceProgressStats current value of the rebalance progress stats, used to calculate the next
   * @return the calculated step or progress stats
   */
  @VisibleForTesting
  static TableRebalanceProgressStats.RebalanceProgressStats calculateUpdatedProgressStats(
      Map<String, Map<String, String>> targetAssignment, Map<String, Map<String, String>> currentAssignment,
      RebalanceContext rebalanceContext, Trigger trigger, TableRebalanceProgressStats rebalanceProgressStats) {
    Map<String, Set<String>> existingServersToSegmentMap = new HashMap<>();
    Map<String, Set<String>> newServersToSegmentMap = new HashMap<>();
    Map<String, Set<String>> targetInstanceToOfflineSegmentsMap = new HashMap<>();
    Set<String> newSegmentsNotExistingBefore = new HashSet<>();

    // Segments to monitor is the list of segments that are being moved as part of the table rebalance that the
    // table rebalance intends to track convergence for. This list usually includes segments from the last
    // assignment IS update and the current IS update. The EV-IS convergence check only tracks convergence for the
    // segments on this list, and if any additional segments are found that haven't converged they are ignored.
    // From the stats perspective, we also only care about tracking the convergence of actual segments that the
    // rebalance cares about, that is why we skip any segments that are not on this list.
    Set<String> segmentsToMonitor = rebalanceContext.getSegmentsToMonitor();
    int totalNewSegmentsNotMonitored = 0;
    int totalSegmentsTarget = 0;
    for (Map.Entry<String, Map<String, String>> entrySet : targetAssignment.entrySet()) {
      String segmentName = entrySet.getKey();
      if (!rebalanceContext.getUniqueSegments().contains(segmentName)) {
        newSegmentsNotExistingBefore.add(segmentName);
      }
      // Don't track segments that are not on the rebalance monitor list
      if (segmentsToMonitor != null && !segmentsToMonitor.contains(segmentName)) {
        if (newSegmentsNotExistingBefore.contains(segmentName)) {
          // Don't track newly added segments unless they're on the monitor list, remove them if they were added
          // before
          totalNewSegmentsNotMonitored++;
          newSegmentsNotExistingBefore.remove(segmentName);
        }
        continue;
      }
      for (Map.Entry<String, String> entry : entrySet.getValue().entrySet()) {
        String instanceName = entry.getKey();
        String instanceState = entry.getValue();
        if (instanceState.equals(CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE)) {
          // Skip tracking segments that are in OFFLINE state in the target assignment
          targetInstanceToOfflineSegmentsMap.computeIfAbsent(instanceName, k -> new HashSet<>()).add(segmentName);
          continue;
        }
        totalSegmentsTarget++;
        newServersToSegmentMap.computeIfAbsent(instanceName, k -> new HashSet<>()).add(segmentName);
      }
    }

    for (Map.Entry<String, Map<String, String>> entrySet : currentAssignment.entrySet()) {
      String segmentName = entrySet.getKey();
      // Don't track segments that are not on the rebalance monitor list
      if (segmentsToMonitor != null && !segmentsToMonitor.contains(segmentName)) {
        continue;
      }
      for (String instanceName : entrySet.getValue().keySet()) {
        if (targetInstanceToOfflineSegmentsMap.containsKey(instanceName)
            && targetInstanceToOfflineSegmentsMap.get(instanceName).contains(segmentName)) {
          // Skip tracking segments that are in OFFLINE state in the target assignment
          continue;
        }
        existingServersToSegmentMap.computeIfAbsent(instanceName, k -> new HashSet<>()).add(segmentName);
      }
    }

    int segmentsNotMoved = 0;
    int totalSegmentsToBeDeleted = 0;
    int segmentsUnchangedYetNotConverged = 0;
    for (Map.Entry<String, Set<String>> entry : newServersToSegmentMap.entrySet()) {
      String server = entry.getKey();
      Set<String> segmentSet = entry.getValue();

      Set<String> newSegmentSet = new HashSet<>(segmentSet);
      Set<String> existingSegmentSet = new HashSet<>();
      int segmentsUnchanged = 0;
      if (existingServersToSegmentMap.containsKey(server)) {
        Set<String> segmentSetForServer = existingServersToSegmentMap.get(server);
        existingSegmentSet.addAll(segmentSetForServer);
        Set<String> intersection = new HashSet<>(segmentSetForServer);
        intersection.retainAll(newSegmentSet);
        segmentsUnchanged = intersection.size();
        segmentsNotMoved += segmentsUnchanged;

        for (String segmentName : intersection) {
          String currentInstanceState = currentAssignment.get(segmentName).get(server);
          String targetInstanceState = targetAssignment.get(segmentName).get(server);
          if (!currentInstanceState.equals(targetInstanceState)) {
            segmentsUnchangedYetNotConverged++;
          }
        }
      }
      newSegmentSet.removeAll(existingSegmentSet);
      totalSegmentsToBeDeleted += existingSegmentSet.size() - segmentsUnchanged;
    }

    for (Map.Entry<String, Set<String>> entry : existingServersToSegmentMap.entrySet()) {
      if (!newServersToSegmentMap.containsKey(entry.getKey())) {
        totalSegmentsToBeDeleted += entry.getValue().size();
      }
    }

    int newSegsAddedInThisAssignment = 0;
    int newSegsDeletedInThisAssignment = 0;
    for (String segment : newSegmentsNotExistingBefore) {
      Set<String> currentSegmentAssign = currentAssignment.get(segment) != null
          ? currentAssignment.get(segment).keySet() : new HashSet<>();
      Set<String> targetSegmentAssign = targetAssignment.get(segment) != null
          ? targetAssignment.get(segment).keySet() : new HashSet<>();

      Set<String> segmentsAdded = new HashSet<>(targetSegmentAssign);
      segmentsAdded.removeAll(currentSegmentAssign);
      newSegsAddedInThisAssignment += segmentsAdded.size();

      Set<String> segmentsDeleted = new HashSet<>(currentSegmentAssign);
      segmentsDeleted.removeAll(targetSegmentAssign);
      newSegsDeletedInThisAssignment += segmentsDeleted.size();
    }

    int newNumberSegmentsTotal = totalSegmentsTarget;
    int totalSegmentsToBeAdded = newNumberSegmentsTotal - segmentsNotMoved;

    TableRebalanceProgressStats.RebalanceProgressStats existingProgressStats;
    long startTimeMs;
    TableRebalanceProgressStats.RebalanceProgressStats progressStats =
        new TableRebalanceProgressStats.RebalanceProgressStats();
    switch (trigger) {
      case START_TRIGGER:
      case NEXT_ASSINGMENT_CALCULATION_TRIGGER:
        // These are initialization steps for global / step progress stats
        progressStats._totalSegmentsToBeAdded = totalSegmentsToBeAdded;
        progressStats._totalSegmentsToBeDeleted = totalSegmentsToBeDeleted;
        progressStats._totalRemainingSegmentsToBeAdded = totalSegmentsToBeAdded;
        progressStats._totalRemainingSegmentsToBeDeleted = totalSegmentsToBeDeleted;
        progressStats._totalCarryOverSegmentsToBeAdded = 0;
        progressStats._totalCarryOverSegmentsToBeDeleted = 0;
        progressStats._totalRemainingSegmentsToConverge = segmentsUnchangedYetNotConverged;
        progressStats._totalUniqueNewUntrackedSegmentsDuringRebalance = totalNewSegmentsNotMonitored;
        progressStats._percentageRemainingSegmentsToBeAdded = totalSegmentsToBeAdded == 0 ? 0.0 : 100.0;
        progressStats._percentageRemainingSegmentsToBeDeleted = totalSegmentsToBeDeleted == 0 ? 0.0 : 100.0;
        progressStats._estimatedTimeToCompleteAddsInSeconds = totalSegmentsToBeAdded == 0 ? 0.0 : -1.0;
        progressStats._estimatedTimeToCompleteDeletesInSeconds = totalSegmentsToBeDeleted == 0 ? 0.0 : -1.0;
        progressStats._averageSegmentSizeInBytes = rebalanceContext.getEstimatedAverageSegmentSizeInBytes();
        progressStats._totalEstimatedDataToBeMovedInBytes =
            TableRebalanceProgressStats.calculateNewEstimatedDataToBeMovedInBytes(0,
                rebalanceContext.getEstimatedAverageSegmentSizeInBytes(), totalSegmentsToBeAdded);
        progressStats._startTimeMs = trigger == Trigger.NEXT_ASSINGMENT_CALCULATION_TRIGGER
            ? System.currentTimeMillis() : rebalanceProgressStats.getStartTimeMs();
        break;
      case IDEAL_STATE_CHANGE_TRIGGER:
        existingProgressStats = rebalanceProgressStats.getRebalanceProgressStatsOverall();
        progressStats._totalSegmentsToBeAdded =
            existingProgressStats._totalSegmentsToBeAdded + newSegsAddedInThisAssignment;
        progressStats._totalSegmentsToBeDeleted =
            existingProgressStats._totalSegmentsToBeDeleted + newSegsDeletedInThisAssignment;
        progressStats._totalRemainingSegmentsToBeAdded = totalSegmentsToBeAdded;
        progressStats._totalRemainingSegmentsToBeDeleted = totalSegmentsToBeDeleted;
        progressStats._totalCarryOverSegmentsToBeAdded = 0;
        progressStats._totalCarryOverSegmentsToBeDeleted = 0;
        progressStats._totalRemainingSegmentsToConverge = segmentsUnchangedYetNotConverged;
        // For IS update we don't re-calculate the new unique segments added as they were captured previously
        // (the segmentsToMonitor is passed in as null), copy over the existing stats
        progressStats._totalUniqueNewUntrackedSegmentsDuringRebalance =
            existingProgressStats._totalUniqueNewUntrackedSegmentsDuringRebalance;
        progressStats._percentageRemainingSegmentsToBeAdded = TableRebalanceProgressStats.calculatePercentageChange(
            progressStats._totalSegmentsToBeAdded, totalSegmentsToBeAdded);
        progressStats._percentageRemainingSegmentsToBeDeleted = TableRebalanceProgressStats.calculatePercentageChange(
            progressStats._totalSegmentsToBeDeleted, totalSegmentsToBeDeleted);
        // Calculate elapsed time based on start of global rebalance time
        startTimeMs = rebalanceProgressStats.getStartTimeMs();
        progressStats._estimatedTimeToCompleteAddsInSeconds =
            TableRebalanceProgressStats.calculateEstimatedTimeToCompleteChange(startTimeMs,
                progressStats._totalSegmentsToBeAdded, progressStats._totalRemainingSegmentsToBeAdded);
        progressStats._estimatedTimeToCompleteDeletesInSeconds =
            TableRebalanceProgressStats.calculateEstimatedTimeToCompleteChange(startTimeMs,
                progressStats._totalSegmentsToBeDeleted, progressStats._totalRemainingSegmentsToBeDeleted);
        progressStats._averageSegmentSizeInBytes = existingProgressStats._averageSegmentSizeInBytes;
        progressStats._totalEstimatedDataToBeMovedInBytes =
            TableRebalanceProgressStats.calculateNewEstimatedDataToBeMovedInBytes(
                existingProgressStats._totalEstimatedDataToBeMovedInBytes, progressStats._averageSegmentSizeInBytes,
                newSegsAddedInThisAssignment);
        progressStats._startTimeMs = startTimeMs;
        break;
      case EXTERNAL_VIEW_TO_IDEAL_STATE_CONVERGENCE_TRIGGER:
        existingProgressStats = rebalanceProgressStats.getRebalanceProgressStatsCurrentStep();
        progressStats._totalSegmentsToBeAdded =
            existingProgressStats._totalSegmentsToBeAdded + newSegsAddedInThisAssignment;
        progressStats._totalSegmentsToBeDeleted =
            existingProgressStats._totalSegmentsToBeDeleted + newSegsDeletedInThisAssignment;
        progressStats._totalRemainingSegmentsToBeAdded = totalSegmentsToBeAdded;
        progressStats._totalRemainingSegmentsToBeDeleted = totalSegmentsToBeDeleted;
        progressStats._totalCarryOverSegmentsToBeAdded = 0;
        progressStats._totalCarryOverSegmentsToBeDeleted = 0;
        // Divide up the total segments to be added / deleted into two buckets, one for the current expected number
        // of segments for the given step, and the carry over from the previous step that didn't complete then.
        // This can especially occur if bestEfforts=true
        // An example of carry-over:
        //     totalSegmentsToBeAdded = 15
        //     progressStats._totalSegmentsToBeAdded = 10
        // Based on the above the carry over and remaining segments will be split as:
        //     progressStats._totalCarryOverSegmentsToBeAdded = (15 - 10) = 5
        //     progressStats._totalRemainingSegmentsToBeAdded = 10
        // The totalCarryOverSegmentsToBeAdded are segment that were added in the last rebalance next assignment IS
        // update, but for which we didn't wait for convergence before updating the IS with the next assignment again
        if (progressStats._totalSegmentsToBeAdded < totalSegmentsToBeAdded) {
          progressStats._totalCarryOverSegmentsToBeAdded = totalSegmentsToBeAdded
              - progressStats._totalSegmentsToBeAdded;
          progressStats._totalRemainingSegmentsToBeAdded = progressStats._totalSegmentsToBeAdded;
        }
        if (progressStats._totalSegmentsToBeDeleted < totalSegmentsToBeDeleted) {
          progressStats._totalCarryOverSegmentsToBeDeleted = totalSegmentsToBeDeleted
              - progressStats._totalSegmentsToBeDeleted;
          progressStats._totalRemainingSegmentsToBeDeleted = progressStats._totalSegmentsToBeDeleted;
        }
        progressStats._totalRemainingSegmentsToConverge = segmentsUnchangedYetNotConverged;
        progressStats._totalUniqueNewUntrackedSegmentsDuringRebalance =
            existingProgressStats._totalUniqueNewUntrackedSegmentsDuringRebalance + totalNewSegmentsNotMonitored;
        // This percentage can be > 100% for EV-IS convergence since there could be some segments carried over from the
        // last step to this one that are yet to converge. This can especially occur if bestEfforts=true
        progressStats._percentageRemainingSegmentsToBeAdded = TableRebalanceProgressStats.calculatePercentageChange(
            progressStats._totalSegmentsToBeAdded, totalSegmentsToBeAdded);
        progressStats._percentageRemainingSegmentsToBeDeleted = TableRebalanceProgressStats.calculatePercentageChange(
            progressStats._totalSegmentsToBeDeleted, totalSegmentsToBeDeleted);
        // Calculate elapsed time based on start of the rebalance step start time
        startTimeMs = existingProgressStats._startTimeMs;
        progressStats._estimatedTimeToCompleteAddsInSeconds =
            TableRebalanceProgressStats.calculateEstimatedTimeToCompleteChange(startTimeMs,
                progressStats._totalSegmentsToBeAdded, totalSegmentsToBeAdded);
        progressStats._estimatedTimeToCompleteDeletesInSeconds =
            TableRebalanceProgressStats.calculateEstimatedTimeToCompleteChange(startTimeMs,
                progressStats._totalSegmentsToBeDeleted, totalSegmentsToBeDeleted);
        progressStats._averageSegmentSizeInBytes = existingProgressStats._averageSegmentSizeInBytes;
        progressStats._totalEstimatedDataToBeMovedInBytes =
            TableRebalanceProgressStats.calculateNewEstimatedDataToBeMovedInBytes(
                existingProgressStats._totalEstimatedDataToBeMovedInBytes, progressStats._averageSegmentSizeInBytes,
                newSegsAddedInThisAssignment);
        progressStats._startTimeMs = startTimeMs;
        break;
      default:
        LOGGER.error("Invalid progress stats trigger type found: {}, return default progress stats", trigger);
        break;
    }

    return progressStats;
  }
}
