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
 * These are rebalance progress stats to track how the rebalance is progressing over time
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
   * @param currentStepStats latest step level stats calculated in this iteration
   */
  public void updateOverallAndStepStatsFromLatestStepStats(
      TableRebalanceProgressStats.RebalanceProgressStats currentStepStats) {
    // Fetch the step level and overall stats that were calculated in the last convergence check. These will be used
    // to calculate the overall stats in the current convergence check
    TableRebalanceProgressStats.RebalanceProgressStats previousStepStats = getRebalanceProgressStatsCurrentStep();
    TableRebalanceProgressStats.RebalanceProgressStats previousOverallStats = getRebalanceProgressStatsOverall();

    // Calculate the new segments added / deleted since the last step to track overall change in segments. These are
    // only new segments tracked by table rebalance (segments that aren't tracked by rebalance convergence check are
    // ignored because the convergence check does not wait for them to complete)
    int numAdditionalSegmentsAdded =
        currentStepStats._totalSegmentsToBeAdded - previousStepStats._totalSegmentsToBeAdded;
    int numAdditionalSegmentsDeleted =
        currentStepStats._totalSegmentsToBeDeleted - previousStepStats._totalSegmentsToBeDeleted;

    // Number of adds / deletes processed in the last step
    int numSegmentAddsProcessedInLastStep = previousStepStats._totalRemainingSegmentsToBeAdded
        - currentStepStats._totalRemainingSegmentsToBeAdded;
    int numSegmentDeletesProcessedInLastStep = previousStepStats._totalRemainingSegmentsToBeDeleted
        - currentStepStats._totalRemainingSegmentsToBeDeleted;

    // Number of untracked segments that were added
    int numberNewUntrackedSegmentsAdded = currentStepStats._totalUniqueNewUntrackedSegmentsDuringRebalance
        - previousStepStats._totalUniqueNewUntrackedSegmentsDuringRebalance;

    TableRebalanceProgressStats.RebalanceProgressStats currentOverallStats =
        new TableRebalanceProgressStats.RebalanceProgressStats();

    // New number of total segment adds / deletes should include any new segments added from the previous stats
    currentOverallStats._totalSegmentsToBeAdded = previousOverallStats._totalSegmentsToBeAdded
        + numAdditionalSegmentsAdded;
    currentOverallStats._totalSegmentsToBeDeleted = previousOverallStats._totalSegmentsToBeDeleted
        + numAdditionalSegmentsDeleted;

    // If carry-over segments are present, keep the upper bound of remaining segments to be added / deleted at the same
    // level as the previous stats. This is so we track progress to handle the carry-over segments before we start
    // tracking progress against the segments that need to be handled as part of the current rebalance step.
    if (currentStepStats._totalCarryOverSegmentsToBeAdded > 0) {
      currentOverallStats._totalRemainingSegmentsToBeAdded = previousOverallStats._totalRemainingSegmentsToBeAdded;
    } else {
      currentOverallStats._totalRemainingSegmentsToBeAdded =
          previousOverallStats._totalRemainingSegmentsToBeAdded - numSegmentAddsProcessedInLastStep;
    }
    if (currentStepStats._totalCarryOverSegmentsToBeDeleted > 0) {
      currentOverallStats._totalRemainingSegmentsToBeDeleted = previousOverallStats._totalRemainingSegmentsToBeDeleted;
    } else {
      currentOverallStats._totalRemainingSegmentsToBeDeleted =
          previousOverallStats._totalRemainingSegmentsToBeDeleted - numSegmentDeletesProcessedInLastStep;
    }

    // Carry over segments stats from the previous step that didn't complete convergence
    currentOverallStats._totalCarryOverSegmentsToBeAdded = currentStepStats._totalCarryOverSegmentsToBeAdded;
    currentOverallStats._totalCarryOverSegmentsToBeDeleted = currentStepStats._totalCarryOverSegmentsToBeDeleted;

    // Segments that need to converge (i.e. the expected state differs from the current state)
    currentOverallStats._totalRemainingSegmentsToConverge = currentStepStats._totalRemainingSegmentsToConverge;

    // New segments that were added during the rebalance but that aren't actively tracked by table rebalance
    currentOverallStats._totalUniqueNewUntrackedSegmentsDuringRebalance =
        previousOverallStats._totalUniqueNewUntrackedSegmentsDuringRebalance + numberNewUntrackedSegmentsAdded;

    // Calculate the percentage segments that still need to complete processing. This is calculated based on the
    // total segments remaining and the carry-over segments from the last rebalance next assignment IS update
    currentOverallStats._percentageRemainingSegmentsToBeAdded =
        calculatePercentageChange(currentOverallStats._totalSegmentsToBeAdded,
            currentOverallStats._totalRemainingSegmentsToBeAdded
                + currentOverallStats._totalCarryOverSegmentsToBeAdded);
    currentOverallStats._percentageRemainingSegmentsToBeDeleted =
        calculatePercentageChange(currentOverallStats._totalSegmentsToBeDeleted,
            currentOverallStats._totalRemainingSegmentsToBeDeleted
                + currentOverallStats._totalCarryOverSegmentsToBeDeleted);

    // Calculate elapsed time based on start of rebalance (global)
    currentOverallStats._estimatedTimeToCompleteAddsInSeconds =
        calculateEstimatedTimeToCompleteChange(getStartTimeMs(),
            currentOverallStats._totalSegmentsToBeAdded, currentOverallStats._totalRemainingSegmentsToBeAdded
                + currentOverallStats._totalCarryOverSegmentsToBeAdded);
    currentOverallStats._estimatedTimeToCompleteDeletesInSeconds =
        calculateEstimatedTimeToCompleteChange(getStartTimeMs(),
            currentOverallStats._totalSegmentsToBeDeleted, currentOverallStats._totalRemainingSegmentsToBeDeleted
                + currentOverallStats._totalCarryOverSegmentsToBeDeleted);

    // Estimates bytes calculations
    currentOverallStats._averageSegmentSizeInBytes = previousOverallStats._averageSegmentSizeInBytes;
    currentOverallStats._totalEstimatedDataToBeMovedInBytes = previousOverallStats._totalEstimatedDataToBeMovedInBytes
        + (numAdditionalSegmentsAdded * previousOverallStats._averageSegmentSizeInBytes);

    currentOverallStats._startTimeMs = getStartTimeMs();

    // Update the progress stats with the current calculated stats
    setRebalanceProgressStatsOverall(currentOverallStats);
    setRebalanceProgressStatsCurrentStep(currentStepStats);
  }

  public static double calculatePercentageChange(int totalSegmentsToChange, int remainingSegmentsToChange) {
    return totalSegmentsToChange == 0
        ? 0.0 : (double) remainingSegmentsToChange / (double) totalSegmentsToChange * 100.0;
  }

  public static double calculateEstimatedTimeToCompleteChange(long startTime, int totalSegmentsToChange,
      int remainingSegmentsToChange) {
    double elapsedTimeInSeconds = (double) (System.currentTimeMillis() - startTime) / 1000.0;
    int segmentsAlreadyChanged = totalSegmentsToChange - remainingSegmentsToChange;
    // If carry over + remaining segments to change are > total segments to change then number of segments already
    // changed may be -ve, in which case we should just set the default value as we cannot measure elapsed time
    //return segmentsAlreadyChanged <= 0 ? totalSegmentsToChange == 0 ? 0.0 : -1.0
    return segmentsAlreadyChanged <= 0 ? totalSegmentsToChange == 0 ? 0.0 : -1.0
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
  /**
   * These are rebalance stats as to how the current state is, when compared to the target state.
   * Eg: If the current has 4 segments whose replicas (16) don't match the target state, _segmentsToRebalance
   * is 4 and _replicasToRebalance is 16.
   */
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

    RebalanceProgressStats(RebalanceProgressStats other) {
      _totalSegmentsToBeAdded = other._totalSegmentsToBeAdded;
      _totalSegmentsToBeDeleted = other._totalSegmentsToBeDeleted;
      _totalRemainingSegmentsToBeAdded = other._totalRemainingSegmentsToBeAdded;
      _totalRemainingSegmentsToBeDeleted = other._totalRemainingSegmentsToBeDeleted;
      _totalRemainingSegmentsToConverge = other._totalRemainingSegmentsToConverge;
      _totalCarryOverSegmentsToBeAdded = other._totalCarryOverSegmentsToBeAdded;
      _totalCarryOverSegmentsToBeDeleted = other._totalCarryOverSegmentsToBeDeleted;
      _totalUniqueNewUntrackedSegmentsDuringRebalance = other._totalUniqueNewUntrackedSegmentsDuringRebalance;
      _percentageRemainingSegmentsToBeAdded = other._percentageRemainingSegmentsToBeAdded;
      _percentageRemainingSegmentsToBeDeleted = other._percentageRemainingSegmentsToBeDeleted;
      _estimatedTimeToCompleteAddsInSeconds = other._estimatedTimeToCompleteAddsInSeconds;
      _estimatedTimeToCompleteDeletesInSeconds = other._estimatedTimeToCompleteDeletesInSeconds;
      _averageSegmentSizeInBytes = other._averageSegmentSizeInBytes;
      _totalEstimatedDataToBeMovedInBytes = other._totalEstimatedDataToBeMovedInBytes;
      _startTimeMs = other._startTimeMs;
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
