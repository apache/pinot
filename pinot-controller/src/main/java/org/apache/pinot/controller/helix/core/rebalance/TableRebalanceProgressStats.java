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


/**
 * These are rebalance stats as to how the current state is, when compared to the target state.
 * Eg: If the current has 4 segments whose replicas (16) don't match the target state, _segmentsToRebalance
 * is 4 and _replicasToRebalance is 16.
 */
@JsonPropertyOrder({"status", "startTimeMs", "timeToCompleteRebalanceInSeconds", "completionStatusMsg",
    "rebalanceProgressStatsOverall", "rebalanceProgressStatsCurrentStep", "initialToTargetStateConvergence",
    "currentToTargetConvergence", "externalViewToIdealStateConvergence"})
public class TableRebalanceProgressStats {

  // Done/In_progress/Failed
  private RebalanceResult.Status _status;
  // When did Rebalance start
  private long _startTimeMs;
  // How long did rebalance take
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

  public static boolean progressStatsDiffer(RebalanceProgressStats base, RebalanceProgressStats compare) {
    // Don't check for changes in the estimated time for completion as this will always be updated since it is
    // newly calculated for each iteration based on current time and start time
    return base._totalSegmentsToBeAdded != compare._totalSegmentsToBeAdded
        || base._totalSegmentsToBeDeleted != compare._totalSegmentsToBeDeleted
        || base._totalRemainingSegmentsToBeAdded != compare._totalRemainingSegmentsToBeAdded
        || base._totalRemainingSegmentsToBeDeleted != compare._totalRemainingSegmentsToBeDeleted
        || base._totalRemainingSegmentsToConverge != compare._totalRemainingSegmentsToConverge
        || base._totalCarryOverSegmentsToBeAdded != compare._totalCarryOverSegmentsToBeAdded
        || base._totalCarryOverSegmentsToBeDeleted != compare._totalCarryOverSegmentsToBeDeleted
        || base._totalUniqueNewUntrackedSegmentsDuringRebalance
        != compare._totalUniqueNewUntrackedSegmentsDuringRebalance
        || base._percentageTotalSegmentsAddsRemaining != compare._percentageTotalSegmentsAddsRemaining
        || base._percentageTotalSegmentDeletesRemaining != compare._percentageTotalSegmentDeletesRemaining
        || base._averageSegmentSizeInBytes != compare._averageSegmentSizeInBytes
        || base._totalEstimatedDataToBeMovedInBytes != compare._totalEstimatedDataToBeMovedInBytes
        || base._startTimeMs != compare._startTimeMs;
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
    @JsonProperty("percentageTotalSegmentsAddsRemaining")
    public double _percentageTotalSegmentsAddsRemaining;
    @JsonProperty("percentageTotalSegmentDeletesRemaining")
    public double _percentageTotalSegmentDeletesRemaining;
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
      _percentageTotalSegmentsAddsRemaining = 0.0;
      _percentageTotalSegmentDeletesRemaining = 0.0;
      _estimatedTimeToCompleteAddsInSeconds = 0;
      _estimatedTimeToCompleteDeletesInSeconds = 0;
      _averageSegmentSizeInBytes = 0;
      _totalEstimatedDataToBeMovedInBytes = 0;
      _startTimeMs = 0;
    }
  }
}
