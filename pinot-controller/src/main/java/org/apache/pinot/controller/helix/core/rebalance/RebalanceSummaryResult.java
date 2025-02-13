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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Holds the summary data of the rebalance result
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RebalanceSummaryResult {

  public static class ServerSegmentChangeInfo {
    public final int _totalNewSegments;
    public final int _totalExistingSegments;
    public final int _segmentsAdded;
    public final int _segmentsDeleted;
    public final int _segmentsUnchanged;

    @JsonCreator
    public ServerSegmentChangeInfo(@JsonProperty("totalNewSegments") int totalNewSegments,
        @JsonProperty("totalExistingSegments") int totalExistingSegments,
        @JsonProperty("segmentsAdded") int segmentsAdded, @JsonProperty("segmentsDeleted") int segmentsDeleted,
        @JsonProperty("segmentsUnchanged") int segmentsUnchanged) {
      _totalNewSegments = totalNewSegments;
      _totalExistingSegments = totalExistingSegments;
      _segmentsAdded = segmentsAdded;
      _segmentsDeleted = segmentsDeleted;
      _segmentsUnchanged = segmentsUnchanged;
    }
  }

  public static class RebalanceChangeInfo {
    public final int _existingValue;
    public final int _newValue;

    @JsonCreator
    public RebalanceChangeInfo(@JsonProperty("existingValue") int existingValue,
        @JsonProperty("newValue") int newValue) {
      _existingValue = existingValue;
      _newValue = newValue;
    }
  }

  // TODO: Add stats about total data size to be moved and estimated calculations on how long the data move can take
  //       during rebalance. Estimations are fine based on total table size / total number of segments
  private final int _totalSegmentsToBeMoved;
  private final int _numServersGettingNewSegments;
  private final long _estimatedAverageSegmentSizeInBytes;
  private final long _totalEstimatedDataToBeMovedInBytes;
  private final double _totalEstimatedTimeToMoveDataInSecs;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final RebalanceChangeInfo _numServers;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final RebalanceChangeInfo _replicationFactor;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final RebalanceChangeInfo _numUniqueSegments;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final RebalanceChangeInfo _numTotalSegments;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final Map<String, ServerSegmentChangeInfo> _serverSegmentChangeInfo;

  @JsonCreator
  public RebalanceSummaryResult(
      @JsonProperty("totalSegmentsToBeMoved") int totalSegmentsToBeMoved,
      @JsonProperty("numServersGettingNewSegments") int numServersGettingNewSegments,
      @JsonProperty("estimatedAverageSegmentSizeInBytes") long estimatedAverageSegmentSizeInBytes,
      @JsonProperty("totalEstimatedDataToBeMovedInBytes") long totalEstimatedDataToBeMovedInBytes,
      @JsonProperty("totalEstimatedTimeToMoveDataInSecs") double totalEstimatedTimeToMoveDataInSecs,
      @JsonProperty("numServers") @Nullable RebalanceChangeInfo numServers,
      @JsonProperty("replicationFactor") @Nullable RebalanceChangeInfo replicationFactor,
      @JsonProperty("numUniqueSegments") @Nullable RebalanceChangeInfo numUniqueSegments,
      @JsonProperty("numTotalSegments") @Nullable RebalanceChangeInfo numTotalSegments,
      @JsonProperty("serverSegmentChangeInfo") @Nullable Map<String, ServerSegmentChangeInfo> serverSegmentChangeInfo) {
    _totalSegmentsToBeMoved = totalSegmentsToBeMoved;
    _numServersGettingNewSegments = numServersGettingNewSegments;
    _estimatedAverageSegmentSizeInBytes = estimatedAverageSegmentSizeInBytes;
    _totalEstimatedDataToBeMovedInBytes = totalEstimatedDataToBeMovedInBytes;
    _totalEstimatedTimeToMoveDataInSecs = totalEstimatedTimeToMoveDataInSecs;
    _numServers = numServers;
    _replicationFactor = replicationFactor;
    _numUniqueSegments = numUniqueSegments;
    _numTotalSegments = numTotalSegments;
    _serverSegmentChangeInfo = serverSegmentChangeInfo;
  }

  @JsonProperty
  public int getTotalSegmentsToBeMoved() {
    return _totalSegmentsToBeMoved;
  }

  @JsonProperty
  public int getNumServersGettingNewSegments() {
    return _numServersGettingNewSegments;
  }

  @JsonProperty
  public long getEstimatedAverageSegmentSizeInBytes() {
    return _estimatedAverageSegmentSizeInBytes;
  }

  @JsonProperty
  public long getTotalEstimatedDataToBeMovedInBytes() {
    return _totalEstimatedDataToBeMovedInBytes;
  }

  @JsonProperty
  public double getTotalEstimatedTimeToMoveDataInSecs() {
    return _totalEstimatedTimeToMoveDataInSecs;
  }

  @JsonProperty
  public RebalanceChangeInfo getNumServers() {
    return _numServers;
  }

  @JsonProperty
  public RebalanceChangeInfo getReplicationFactor() {
    return _replicationFactor;
  }

  @JsonProperty
  public RebalanceChangeInfo getNumUniqueSegments() {
    return _numUniqueSegments;
  }

  @JsonProperty
  public RebalanceChangeInfo getNumTotalSegments() {
    return _numTotalSegments;
  }

  @JsonProperty
  public Map<String, ServerSegmentChangeInfo> getServerSegmentChangeInfo() {
    return _serverSegmentChangeInfo;
  }
}
