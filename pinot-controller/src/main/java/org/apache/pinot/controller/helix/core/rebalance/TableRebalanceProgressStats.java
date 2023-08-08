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

  // Done/In_progress/Failed
  private String _status;
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

  public TableRebalanceProgressStats() {
    _currentToTargetConvergence = new RebalanceStateStats();
    _externalViewToIdealStateConvergence = new RebalanceStateStats();
    _initialToTargetStateConvergence = new RebalanceStateStats();
  }

  public void setStatus(String status) {
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

  public void setCompletionStatusMsg(String completionStatusMsg) {
    _completionStatusMsg = completionStatusMsg;
  }

  public String getStatus() {
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

  public static boolean statsDiffer(RebalanceStateStats base, RebalanceStateStats compare) {
    if (base._replicasToRebalance != compare._replicasToRebalance
        || base._segmentsToRebalance != compare._segmentsToRebalance
        || base._segmentsMissing != compare._segmentsMissing
        || base._percentSegmentsToRebalance != compare._percentSegmentsToRebalance) {
      return true;
    }
    return false;
  }
}
