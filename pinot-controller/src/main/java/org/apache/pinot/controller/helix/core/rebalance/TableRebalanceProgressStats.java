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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Objects;


/**
 * These are rebalance stats as to how the current state is, when compared to the target state.
 * Eg: If the current has 4 segments whose replicas (16) don't match the target state, _segmentsToRebalance
 * is 4 and _replicasToRebalance is 16.
 */
@JsonPropertyOrder({"status", "startTimeMs", "timeToFinishInSeconds", "completionStatusMsg",
    "rebalanceProgressStatsOverall", "rebalanceProgressStatsCurrentStep", "initialToTargetStateConvergence",
    "currentToTargetConvergence", "externalViewToIdealStateConvergence"})
public class TableRebalanceProgressStats {

  // Done/In_progress/Failed
  private RebalanceResult.Status _status;
  // When did Rebalance start
  private long _startTimeMs;
  // How long did rebalance take, this is only updated when rebalance completes
  private long _timeToFinishInSeconds;
  // Success/failure message
  private String _completionStatusMsg;
  @JsonProperty("initialToTargetStateConvergence")
  private RebalanceStateStats _initialToTargetStateConvergence;
  @JsonProperty("currentToTargetConvergence")
  private RebalanceStateStats _currentToTargetConvergence;
  @JsonProperty("externalViewToIdealStateConvergence")
  private RebalanceStateStats _externalViewToIdealStateConvergence;
  // This tracks the overall progress of segments to be added / deleted as part of rebalance
  @JsonProperty("rebalanceProgressStatsOverall")
  private RebalanceProgressStats _rebalanceProgressStatsOverall;
  // This tracks the segments to be added / deleted for a single rebalance step (each ideal-state update is 1 step)
  @JsonProperty("rebalanceProgressStatsCurrentStep")
  private RebalanceProgressStats _rebalanceProgressStatsCurrentStep;

  public TableRebalanceProgressStats() {
    _currentToTargetConvergence = new RebalanceStateStats();
    _externalViewToIdealStateConvergence = new RebalanceStateStats();
    _initialToTargetStateConvergence = new RebalanceStateStats();
    _rebalanceProgressStatsOverall = new RebalanceProgressStats();
    _rebalanceProgressStatsCurrentStep = new RebalanceProgressStats();
  }

  public void setStatus(RebalanceResult.Status status) {
    _status = status;
  }

  public void setInitialToTargetStateConvergence(RebalanceStateStats initialToTargetStateConvergence) {
    _initialToTargetStateConvergence = initialToTargetStateConvergence;
  }

  public void setStartTimeMs(long startTimeMs) {
    _startTimeMs = startTimeMs;
  }

  public void setTimeToFinishInSeconds(Long timeToFinishInSeconds) {
    _timeToFinishInSeconds = timeToFinishInSeconds;
  }

  public void setExternalViewToIdealStateConvergence(RebalanceStateStats externalViewToIdealStateConvergence) {
    _externalViewToIdealStateConvergence = externalViewToIdealStateConvergence;
  }

  public void setCurrentToTargetConvergence(RebalanceStateStats currentToTargetConvergence) {
    _currentToTargetConvergence = currentToTargetConvergence;
  }

  public void setRebalanceProgressStatsOverall(
      RebalanceProgressStats rebalanceProgressStatsOverall) {
    _rebalanceProgressStatsOverall = rebalanceProgressStatsOverall;
  }

  public void setRebalanceProgressStatsCurrentStep(
      RebalanceProgressStats rebalanceProgressStatsCurrentStep) {
    _rebalanceProgressStatsCurrentStep = rebalanceProgressStatsCurrentStep;
  }

  public void setCompletionStatusMsg(String completionStatusMsg) {
    _completionStatusMsg = completionStatusMsg;
  }

  public RebalanceResult.Status getStatus() {
    return _status;
  }

  public String getCompletionStatusMsg() {
    return _completionStatusMsg;
  }

  public RebalanceStateStats getInitialToTargetStateConvergence() {
    return _initialToTargetStateConvergence;
  }

  public long getStartTimeMs() {
    return _startTimeMs;
  }

  public long getTimeToFinishInSeconds() {
    return _timeToFinishInSeconds;
  }

  public RebalanceStateStats getExternalViewToIdealStateConvergence() {
    return _externalViewToIdealStateConvergence;
  }

  public RebalanceStateStats getCurrentToTargetConvergence() {
    return _currentToTargetConvergence;
  }

  public RebalanceProgressStats getRebalanceProgressStatsOverall() {
    return _rebalanceProgressStatsOverall;
  }

  public RebalanceProgressStats getRebalanceProgressStatsCurrentStep() {
    return _rebalanceProgressStatsCurrentStep;
  }

  /**
   * Updates the overall and step progress stats based on the latest calculated step's progress stats. This should
   * be called during the EV-IS convergence trigger to ensure the overall stats reflect the changes as they are made.
   * @param latestStepStats latest step level stats calculated in this iteration
   */
  public void updateOverallAndStepStatsFromLatestStepStats(
      TableRebalanceProgressStats.RebalanceProgressStats latestStepStats) {
    TableRebalanceProgressStats.RebalanceProgressStats lastStepStats = getRebalanceProgressStatsCurrentStep();
    TableRebalanceProgressStats.RebalanceProgressStats overallProgressStats = getRebalanceProgressStatsOverall();
    int numAdditionalSegmentsAdded =
        latestStepStats._totalSegmentsToBeAdded - lastStepStats._totalSegmentsToBeAdded;
    int numAdditionalSegmentsDeleted =
        latestStepStats._totalSegmentsToBeDeleted - lastStepStats._totalSegmentsToBeDeleted;
    int upperBoundOnSegmentsAdded =
        lastStepStats._totalRemainingSegmentsToBeAdded > lastStepStats._totalSegmentsToBeAdded
            ? lastStepStats._totalSegmentsToBeAdded : lastStepStats._totalRemainingSegmentsToBeAdded;
    int numSegmentAddsProcessedInLastStep = Math.abs(upperBoundOnSegmentsAdded
        - latestStepStats._totalRemainingSegmentsToBeAdded);
    int upperBoundOnSegmentsDeleted =
        lastStepStats._totalRemainingSegmentsToBeDeleted > lastStepStats._totalSegmentsToBeDeleted
            ? lastStepStats._totalSegmentsToBeDeleted : lastStepStats._totalRemainingSegmentsToBeDeleted;
    int numSegmentDeletesProcessedInLastStep = Math.abs(upperBoundOnSegmentsDeleted
        - latestStepStats._totalRemainingSegmentsToBeDeleted);
    int numberNewUntrackedSegmentsAdded = latestStepStats._totalUniqueNewUntrackedSegmentsDuringRebalance
        - lastStepStats._totalUniqueNewUntrackedSegmentsDuringRebalance;

    TableRebalanceProgressStats.RebalanceProgressStats newOverallProgressStats =
        new TableRebalanceProgressStats.RebalanceProgressStats();

    newOverallProgressStats._totalSegmentsToBeAdded = overallProgressStats._totalSegmentsToBeAdded
        + numAdditionalSegmentsAdded;
    newOverallProgressStats._totalSegmentsToBeDeleted = overallProgressStats._totalSegmentsToBeDeleted
        + numAdditionalSegmentsDeleted;
    if (latestStepStats._totalCarryOverSegmentsToBeAdded > 0) {
      newOverallProgressStats._totalRemainingSegmentsToBeAdded = overallProgressStats._totalRemainingSegmentsToBeAdded;
    } else {
      newOverallProgressStats._totalRemainingSegmentsToBeAdded = numAdditionalSegmentsAdded == 0
          ? overallProgressStats._totalRemainingSegmentsToBeAdded - numSegmentAddsProcessedInLastStep
          : overallProgressStats._totalRemainingSegmentsToBeAdded + numSegmentAddsProcessedInLastStep;
    }
    newOverallProgressStats._totalCarryOverSegmentsToBeAdded =
        latestStepStats._totalCarryOverSegmentsToBeAdded;
    if (latestStepStats._totalCarryOverSegmentsToBeDeleted > 0) {
      newOverallProgressStats._totalRemainingSegmentsToBeDeleted =
          overallProgressStats._totalRemainingSegmentsToBeDeleted;
    } else {
      newOverallProgressStats._totalRemainingSegmentsToBeDeleted = numAdditionalSegmentsDeleted == 0
          ? overallProgressStats._totalRemainingSegmentsToBeDeleted - numSegmentDeletesProcessedInLastStep
          : overallProgressStats._totalRemainingSegmentsToBeDeleted + numSegmentDeletesProcessedInLastStep;
    }
    newOverallProgressStats._totalCarryOverSegmentsToBeDeleted =
        latestStepStats._totalCarryOverSegmentsToBeDeleted;
    newOverallProgressStats._totalRemainingSegmentsToConverge = latestStepStats._totalRemainingSegmentsToConverge;
    newOverallProgressStats._totalUniqueNewUntrackedSegmentsDuringRebalance =
        overallProgressStats._totalUniqueNewUntrackedSegmentsDuringRebalance + numberNewUntrackedSegmentsAdded;
    newOverallProgressStats._percentageRemainingSegmentsToBeAdded =
        calculatePercentageChange(newOverallProgressStats._totalSegmentsToBeAdded,
            newOverallProgressStats._totalRemainingSegmentsToBeAdded
                + newOverallProgressStats._totalCarryOverSegmentsToBeAdded);
    newOverallProgressStats._percentageRemainingSegmentsToBeDeleted =
        calculatePercentageChange(newOverallProgressStats._totalSegmentsToBeDeleted,
            newOverallProgressStats._totalRemainingSegmentsToBeDeleted
                + newOverallProgressStats._totalCarryOverSegmentsToBeDeleted);
    // Calculate elapsed time based on start of rebalance (global)
    newOverallProgressStats._estimatedTimeToCompleteAddsInSeconds =
        calculateEstimatedTimeToCompleteChange(getStartTimeMs(),
            newOverallProgressStats._totalSegmentsToBeAdded, newOverallProgressStats._totalRemainingSegmentsToBeAdded);
    newOverallProgressStats._estimatedTimeToCompleteDeletesInSeconds =
        calculateEstimatedTimeToCompleteChange(getStartTimeMs(),
            newOverallProgressStats._totalSegmentsToBeDeleted,
            newOverallProgressStats._totalRemainingSegmentsToBeDeleted);
    newOverallProgressStats._averageSegmentSizeInBytes = overallProgressStats._averageSegmentSizeInBytes;
    newOverallProgressStats._totalEstimatedDataToBeMovedInBytes =
        overallProgressStats._totalEstimatedDataToBeMovedInBytes
            + (numAdditionalSegmentsAdded * overallProgressStats._averageSegmentSizeInBytes);
    newOverallProgressStats._startTimeMs = getStartTimeMs();

    setRebalanceProgressStatsOverall(newOverallProgressStats);
    setRebalanceProgressStatsCurrentStep(latestStepStats);
  }

  public static double calculatePercentageChange(int totalSegmentsToChange, int remainingSegmentsToChange) {
    return totalSegmentsToChange == 0
        ? 0.0 : (double) remainingSegmentsToChange / (double) totalSegmentsToChange * 100.0;
  }

  public static double calculateEstimatedTimeToCompleteChange(long startTime, int totalSegmentsToChange,
      int remainingSegmentsToChange) {
    double elapsedTimeInSeconds = (double) (System.currentTimeMillis() - startTime) / 1000.0;
    int segmentsAlreadyChanged = totalSegmentsToChange - remainingSegmentsToChange;
    return segmentsAlreadyChanged == 0 ? totalSegmentsToChange == 0 ? 0.0 : -1.0
        : (double) remainingSegmentsToChange / (double) segmentsAlreadyChanged * elapsedTimeInSeconds;
  }

  public static long calculateNewEstimatedDataToBeMovedInBytes(long existingDataToBeMovedInBytes,
      long averageSegmentSizeInBytes, int newSegmentsAdded) {
    return averageSegmentSizeInBytes < 0
        ? -1 : existingDataToBeMovedInBytes + ((long) newSegmentsAdded * averageSegmentSizeInBytes);
  }

  public static boolean statsDiffer(RebalanceStateStats base, RebalanceStateStats compare) {
    if (base._replicasToRebalance != compare._replicasToRebalance
        || base._segmentsToRebalance != compare._segmentsToRebalance
        || base._segmentsMissing != compare._segmentsMissing
        || base._percentSegmentsToRebalance != compare._percentSegmentsToRebalance) {
      return true;
    }
    return false;
  }

  // TODO: Clean this up once new stats are verified
  public static class RebalanceStateStats {
    public int _segmentsMissing;
    public int _segmentsToRebalance;
    public double _percentSegmentsToRebalance;
    public int _replicasToRebalance;

    RebalanceStateStats() {
      _segmentsMissing = 0;
      _segmentsToRebalance = 0;
      _replicasToRebalance = 0;
      _percentSegmentsToRebalance = 0.0;
    }
  }

  // These rebalance stats specifically track the total segments added / deleted across all replicas
  public static class RebalanceProgressStats {
    // Total segments - across all replicas
    @JsonProperty("totalSegmentsToBeAdded")
    public int _totalSegmentsToBeAdded;
    @JsonProperty("totalSegmentsToBeDeleted")
    public int _totalSegmentsToBeDeleted;
    // Total segments processed so far - across all replicas
    @JsonProperty("totalRemainingSegmentsToBeAdded")
    public int _totalRemainingSegmentsToBeAdded;
    @JsonProperty("totalRemainingSegmentsToBeDeleted")
    public int _totalRemainingSegmentsToBeDeleted;
    @JsonProperty("totalRemainingSegmentsToConverge")
    public int _totalRemainingSegmentsToConverge;
    // Carry over stats - for when previous step's convergence doesn't complete and next step starts (bestEffort=true)
    @JsonProperty("totalCarryOverSegmentsToBeAdded")
    public int _totalCarryOverSegmentsToBeAdded;
    @JsonProperty("totalCarryOverSegmentsToBeDeleted")
    public int _totalCarryOverSegmentsToBeDeleted;
    // Total new segments stats (not tracked by rebalance)
    @JsonProperty("totalUniqueNewUntrackedSegmentsDuringRebalance")
    public int _totalUniqueNewUntrackedSegmentsDuringRebalance;
    // Derived stats
    @JsonProperty("percentageRemainingSegmentsToBeAdded")
    public double _percentageRemainingSegmentsToBeAdded;
    @JsonProperty("percentageRemainingSegmentsToBeDeleted")
    public double _percentageRemainingSegmentsToBeDeleted;
    @JsonProperty("estimatedTimeToCompleteAddsInSeconds")
    public double _estimatedTimeToCompleteAddsInSeconds;
    @JsonProperty("estimatedTimeToCompleteDeletesInSeconds")
    public double _estimatedTimeToCompleteDeletesInSeconds;
    @JsonProperty("averageSegmentSizeInBytes")
    public long _averageSegmentSizeInBytes;
    @JsonProperty("totalEstimatedDataToBeMovedInBytes")
    public long _totalEstimatedDataToBeMovedInBytes;
    // Start time - mostly used for a given step, for overall the outer _startTimeMs is used which is rebalance start
    @JsonProperty("startTimeMs")
    public long _startTimeMs;

    RebalanceProgressStats() {
      _totalSegmentsToBeAdded = 0;
      _totalSegmentsToBeDeleted = 0;
      _totalRemainingSegmentsToBeAdded = 0;
      _totalRemainingSegmentsToBeDeleted = 0;
      _totalRemainingSegmentsToConverge = 0;
      _totalCarryOverSegmentsToBeAdded = 0;
      _totalCarryOverSegmentsToBeDeleted = 0;
      _totalUniqueNewUntrackedSegmentsDuringRebalance = 0;
      _percentageRemainingSegmentsToBeAdded = 0.0;
      _percentageRemainingSegmentsToBeDeleted = 0.0;
      _estimatedTimeToCompleteAddsInSeconds = 0;
      _estimatedTimeToCompleteDeletesInSeconds = 0;
      _averageSegmentSizeInBytes = 0;
      _totalEstimatedDataToBeMovedInBytes = 0;
      _startTimeMs = 0;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof RebalanceProgressStats)) {
        return false;
      }
      RebalanceProgressStats that = (RebalanceProgressStats) o;
      // Don't check for changes in the estimated time for completion as this will always be updated since it is
      // newly calculated for each iteration based on current time and start time
      return _totalSegmentsToBeAdded == that._totalSegmentsToBeAdded
          && _totalSegmentsToBeDeleted == that._totalSegmentsToBeDeleted
          && _totalRemainingSegmentsToBeAdded == that._totalRemainingSegmentsToBeAdded
          && _totalRemainingSegmentsToBeDeleted == that._totalRemainingSegmentsToBeDeleted
          && _totalRemainingSegmentsToConverge == that._totalRemainingSegmentsToConverge
          && _totalCarryOverSegmentsToBeAdded == that._totalCarryOverSegmentsToBeAdded
          && _totalCarryOverSegmentsToBeDeleted == that._totalCarryOverSegmentsToBeDeleted
          && _totalUniqueNewUntrackedSegmentsDuringRebalance == that._totalUniqueNewUntrackedSegmentsDuringRebalance
          && Double.compare(_percentageRemainingSegmentsToBeAdded, that._percentageRemainingSegmentsToBeAdded)
          == 0
          && Double.compare(_percentageRemainingSegmentsToBeDeleted, that._percentageRemainingSegmentsToBeDeleted)
          == 0 && _averageSegmentSizeInBytes == that._averageSegmentSizeInBytes
          && _totalEstimatedDataToBeMovedInBytes == that._totalEstimatedDataToBeMovedInBytes
          && _startTimeMs == that._startTimeMs;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_totalSegmentsToBeAdded, _totalSegmentsToBeDeleted, _totalRemainingSegmentsToBeAdded,
          _totalRemainingSegmentsToBeDeleted, _totalRemainingSegmentsToConverge, _totalCarryOverSegmentsToBeAdded,
          _totalCarryOverSegmentsToBeDeleted, _totalUniqueNewUntrackedSegmentsDuringRebalance,
          _percentageRemainingSegmentsToBeAdded, _percentageRemainingSegmentsToBeDeleted, _averageSegmentSizeInBytes,
          _totalEstimatedDataToBeMovedInBytes, _startTimeMs);
    }
  }
}
