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


/**
 * These are rebalance stats as to how the current state is, when compared to the target state.
 * Eg: If the current has 4 segments whose replicas (16) don't match the target state, _segmentsToRebalance
 * is 4 and _replicasToRebalance is 16.
 */
public class TableRebalanceProgressStats {
  public static class RebalanceStateStats {
    public int _segmentsMissingFromSource;
    public int _uniqueSegmentsToRebalance;
    public double _percentRemainingSegmentsToRebalance;
    public int _totalSegmentsToRebalance;

    RebalanceStateStats() {
      _segmentsMissingFromSource = 0;
      _uniqueSegmentsToRebalance = 0;
      _totalSegmentsToRebalance = 0;
      _percentRemainingSegmentsToRebalance = 0.0;
    }
  }

  // These rebalance stats specifically track the total segments added / deleted across all replicas
  public static class RebalanceProgressStats {
    public int _totalSegmentsToBeAdded; // across all replicas
    public int _totalSegmentsToBeDeleted; // across all replica
    // Total - # processed so far
    public int _totalRemainingSegmentsToBeAdded; // across all replicas
    public int _totalRemainingSegmentsToBeDeleted; // across all replicas
    // Derived
    public double _percentageTotalSegmentsAddsRemaining;
    public double _percentageTotalSegmentDeletesRemaining;
    public long _estimatedTimeToCompleteAddsInSeconds;
    public long _estimatedTimeToCompleteDeletesInSeconds;
    public long _averageSegmentSizeInBytes;
    public long _totalEstimatedDataToBeMovedInBytes;

    RebalanceProgressStats() {
      _totalSegmentsToBeAdded = 0;
      _totalSegmentsToBeDeleted = 0;
      _totalRemainingSegmentsToBeAdded = 0;
      _totalRemainingSegmentsToBeDeleted = 0;
      _percentageTotalSegmentsAddsRemaining = 0.0;
      _percentageTotalSegmentDeletesRemaining = 0.0;
      _estimatedTimeToCompleteAddsInSeconds = 0;
      _estimatedTimeToCompleteDeletesInSeconds = 0;
      _averageSegmentSizeInBytes = 0;
      _totalEstimatedDataToBeMovedInBytes = 0;
    }
  }

  // Done/In_progress/Failed
  private RebalanceResult.Status _status;
  // When did Rebalance start
  private long _startTimeMs;
  // How long did rebalance take
  private long _timeToCompleteRebalanceInSeconds;
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

  public void setTimeToCompleteRebalanceInSeconds(Long timeToCompleteRebalanceInSeconds) {
    _timeToCompleteRebalanceInSeconds = timeToCompleteRebalanceInSeconds;
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

  public long getTimeToCompleteRebalanceInSeconds() {
    return _timeToCompleteRebalanceInSeconds;
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
    return base._totalSegmentsToBeAdded != compare._totalSegmentsToBeAdded
        || base._totalSegmentsToBeDeleted != compare._totalSegmentsToBeDeleted
        || base._totalRemainingSegmentsToBeAdded != compare._totalRemainingSegmentsToBeAdded
        || base._totalRemainingSegmentsToBeDeleted != compare._totalRemainingSegmentsToBeDeleted
        || base._percentageTotalSegmentsAddsRemaining != compare._percentageTotalSegmentsAddsRemaining
        || base._percentageTotalSegmentDeletesRemaining != compare._percentageTotalSegmentDeletesRemaining
        || base._estimatedTimeToCompleteAddsInSeconds != compare._estimatedTimeToCompleteAddsInSeconds
        || base._estimatedTimeToCompleteDeletesInSeconds != compare._estimatedTimeToCompleteDeletesInSeconds
        || base._averageSegmentSizeInBytes != compare._averageSegmentSizeInBytes
        || base._totalEstimatedDataToBeMovedInBytes != compare._totalEstimatedDataToBeMovedInBytes;
  }

  public static boolean statsDiffer(RebalanceStateStats base, RebalanceStateStats compare) {
    if (base._totalSegmentsToRebalance != compare._totalSegmentsToRebalance
        || base._uniqueSegmentsToRebalance != compare._uniqueSegmentsToRebalance
        || base._segmentsMissingFromSource != compare._segmentsMissingFromSource
        || base._percentRemainingSegmentsToRebalance != compare._percentRemainingSegmentsToRebalance) {
      return true;
    }
    return false;
  }
}
