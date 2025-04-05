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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Holds the summary data of the rebalance result
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RebalanceSummaryResult {
  private final ServerInfo _serverInfo;
  private final SegmentInfo _segmentInfo;
  private final List<TagInfo> _tagsInfo;

  /**
   * Constructor for RebalanceSummaryResult
   * @param serverInfo server related summary information
   * @param segmentInfo segment related summary information
   */
  @JsonCreator
  public RebalanceSummaryResult(@JsonProperty("serverInfo") @Nullable ServerInfo serverInfo,
      @JsonProperty("segmentInfo") @Nullable SegmentInfo segmentInfo,
      @JsonProperty("tagsInfo") @Nullable List<TagInfo> tagsInfo) {
    _serverInfo = serverInfo;
    _segmentInfo = segmentInfo;
    _tagsInfo = tagsInfo;
  }

  @JsonProperty
  public ServerInfo getServerInfo() {
    return _serverInfo;
  }

  @JsonProperty
  public SegmentInfo getSegmentInfo() {
    return _segmentInfo;
  }

  @JsonProperty
  public List<TagInfo> getTagsInfo() {
    return _tagsInfo;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class ServerSegmentChangeInfo {
    private final ServerStatus _serverStatus;
    private final int _totalSegmentsAfterRebalance;
    private final int _totalSegmentsBeforeRebalance;
    private final int _segmentsAdded;
    private final int _segmentsDeleted;
    private final int _segmentsUnchanged;
    private final List<String> _tagList;

    /**
     * Constructor for ServerSegmentChangeInfo
     * @param serverStatus server status, whether it was added, removed, or unchanged as part of this rebalance
     * @param totalSegmentsAfterRebalance expected total segments on this server after rebalance
     * @param totalSegmentsBeforeRebalance current number of segments on this server before rebalance
     * @param segmentsAdded number of segments expected to be added as part of this rebalance
     * @param segmentsDeleted number of segments expected to be deleted as part of this rebalance
     * @param segmentsUnchanged number of segments that aren't moving from this server as part of this rebalance
     * @param tagList server tag list
     */
    @JsonCreator
    public ServerSegmentChangeInfo(@JsonProperty("serverStatus") ServerStatus serverStatus,
        @JsonProperty("totalSegmentsAfterRebalance") int totalSegmentsAfterRebalance,
        @JsonProperty("totalSegmentsBeforeRebalance") int totalSegmentsBeforeRebalance,
        @JsonProperty("segmentsAdded") int segmentsAdded, @JsonProperty("segmentsDeleted") int segmentsDeleted,
        @JsonProperty("segmentsUnchanged") int segmentsUnchanged,
        @JsonProperty("tagList") @Nullable List<String> tagList) {
      _serverStatus = serverStatus;
      _totalSegmentsAfterRebalance = totalSegmentsAfterRebalance;
      _totalSegmentsBeforeRebalance = totalSegmentsBeforeRebalance;
      _segmentsAdded = segmentsAdded;
      _segmentsDeleted = segmentsDeleted;
      _segmentsUnchanged = segmentsUnchanged;
      _tagList = tagList;
    }

    @JsonProperty
    public ServerStatus getServerStatus() {
      return _serverStatus;
    }

    @JsonProperty
    public int getTotalSegmentsAfterRebalance() {
      return _totalSegmentsAfterRebalance;
    }

    @JsonProperty
    public int getTotalSegmentsBeforeRebalance() {
      return _totalSegmentsBeforeRebalance;
    }

    @JsonProperty
    public int getSegmentsAdded() {
      return _segmentsAdded;
    }

    @JsonProperty
    public int getSegmentsDeleted() {
      return _segmentsDeleted;
    }

    @JsonProperty
    public int getSegmentsUnchanged() {
      return _segmentsUnchanged;
    }

    @JsonProperty
    public List<String> getTagList() {
      return _tagList;
    }
  }

  public static class RebalanceChangeInfo {
    private final int _valueBeforeRebalance;
    private final int _expectedValueAfterRebalance;

    /**
     * Constructor for RebalanceChangeInfo
     * @param valueBeforeRebalance current value before rebalance
     * @param expectedValueAfterRebalance expected value after rebalance
     */
    @JsonCreator
    public RebalanceChangeInfo(@JsonProperty("valueBeforeRebalance") int valueBeforeRebalance,
        @JsonProperty("expectedValueAfterRebalance") int expectedValueAfterRebalance) {
      _valueBeforeRebalance = valueBeforeRebalance;
      _expectedValueAfterRebalance = expectedValueAfterRebalance;
    }

    @JsonProperty
    public int getValueBeforeRebalance() {
      return _valueBeforeRebalance;
    }

    @JsonProperty
    public int getExpectedValueAfterRebalance() {
      return _expectedValueAfterRebalance;
    }
  }

  public static class TagInfo {
    public static final String TAG_FOR_OUTDATED_SERVERS = "OUTDATED_SERVERS";
    private final String _tagName;
    private int _numSegmentsUnchanged;
    private int _numSegmentsToDownload;
    private int _numServerParticipants;

    @JsonCreator
    public TagInfo(
        @JsonProperty("tagName") String tagName,
        @JsonProperty("numSegmentsToDownload") int numSegmentsToDownload,
        @JsonProperty("numSegmentsUnchanged") int numSegmentsUnchanged,
        @JsonProperty("numServerParticipants") int numServerParticipants
    ) {
      _tagName = tagName;
      _numSegmentsUnchanged = numSegmentsUnchanged;
      _numSegmentsToDownload = numSegmentsToDownload;
      _numServerParticipants = numServerParticipants;
    }

    public TagInfo(String tagName) {
      this(tagName, 0, 0, 0);
    }

    @JsonProperty
    public String getTagName() {
      return _tagName;
    }

    @JsonProperty
    public int getNumSegmentsUnchanged() {
      return _numSegmentsUnchanged;
    }

    @JsonProperty
    public int getNumSegmentsToDownload() {
      return _numSegmentsToDownload;
    }

    @JsonProperty
    public int getNumServerParticipants() {
      return _numServerParticipants;
    }

    public void increaseNumSegmentsUnchanged(int numSegments) {
      _numSegmentsUnchanged += numSegments;
    }

    public void increaseNumSegmentsToDownload(int numSegments) {
      _numSegmentsToDownload += numSegments;
    }

    public void increaseNumServerParticipants(int numServers) {
      _numServerParticipants += numServers;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class ServerInfo {
    private final int _numServersGettingNewSegments;
    private final RebalanceChangeInfo _numServers;
    private final Set<String> _serversAdded;
    private final Set<String> _serversRemoved;
    private final Set<String> _serversUnchanged;
    private final Set<String> _serversGettingNewSegments;
    private final Map<String, ServerSegmentChangeInfo> _serverSegmentChangeInfo;

    /**
     * Constructor for ServerInfo
     * @param numServersGettingNewSegments total number of servers receiving new segments as part of this rebalance
     * @param numServers number of servers before and after this rebalance
     * @param serversAdded set of servers getting added as part of this rebalance
     * @param serversRemoved set of servers getting removed as part of this rebalance
     * @param serversUnchanged set of servers existing both before and as part of this rebalance
     * @param serversGettingNewSegments set of servers getting segments added
     * @param serverSegmentChangeInfo per server statistics for this rebalance
     */
    @JsonCreator
    public ServerInfo(@JsonProperty("numServersGettingNewSegments") int numServersGettingNewSegments,
        @JsonProperty("numServers") @Nullable RebalanceChangeInfo numServers,
        @JsonProperty("serversAdded") @Nullable Set<String> serversAdded,
        @JsonProperty("serversRemoved") @Nullable Set<String> serversRemoved,
        @JsonProperty("serversUnchanged") @Nullable Set<String> serversUnchanged,
        @JsonProperty("serversGettingNewSegments") @Nullable Set<String> serversGettingNewSegments,
        @JsonProperty("serverSegmentChangeInfo")
        @Nullable Map<String, ServerSegmentChangeInfo> serverSegmentChangeInfo) {
      _numServersGettingNewSegments = numServersGettingNewSegments;
      _numServers = numServers;
      _serversAdded = serversAdded;
      _serversRemoved = serversRemoved;
      _serversUnchanged = serversUnchanged;
      _serversGettingNewSegments = serversGettingNewSegments;
      _serverSegmentChangeInfo = serverSegmentChangeInfo;
    }

    @JsonProperty
    public int getNumServersGettingNewSegments() {
      return _numServersGettingNewSegments;
    }

    @JsonProperty
    public RebalanceChangeInfo getNumServers() {
      return _numServers;
    }

    @JsonProperty
    public Set<String> getServersAdded() {
      return _serversAdded;
    }

    @JsonProperty
    public Set<String> getServersRemoved() {
      return _serversRemoved;
    }

    @JsonProperty
    public Set<String> getServersUnchanged() {
      return _serversUnchanged;
    }

    @JsonProperty
    public Set<String> getServersGettingNewSegments() {
      return _serversGettingNewSegments;
    }

    @JsonProperty
    public Map<String, ServerSegmentChangeInfo> getServerSegmentChangeInfo() {
      return _serverSegmentChangeInfo;
    }
  }

  public static class ConsumingSegmentToBeMovedSummary {
    private final int _numConsumingSegmentsToBeMoved;
    private final int _numServersGettingConsumingSegmentsAdded;
    private final Map<String, Integer> _consumingSegmentsToBeMovedWithMostOffsetsToCatchUp;
    private final Map<String, Integer> _consumingSegmentsToBeMovedWithOldestAgeInMinutes;
    private final Map<String, ConsumingSegmentSummaryPerServer> _serverConsumingSegmentSummary;

    /**
     * Constructor for ConsumingSegmentToBeMovedSummary
     * @param numConsumingSegmentsToBeMoved total number of consuming segments to be moved as part of this rebalance
     * @param numServersGettingConsumingSegmentsAdded maximum bytes of consuming segments to be moved to catch up
     * @param consumingSegmentsToBeMovedWithMostOffsetsToCatchUp top consuming segments to be moved to catch up.
     *                                                           Map from segment name to its number of offsets to
     *                                                           catch up on the new server. This is essentially the
     *                                                           difference between the latest offset of the stream
     *                                                           and the segment's start offset of the stream. The
     *                                                           map is set to null if the number of offsets to catch
     *                                                           up could not be determined for at least one
     *                                                           consuming segment
     * @param consumingSegmentsToBeMovedWithOldestAgeInMinutes oldest consuming segments to be moved to catch up. Map
     *                                                         from segment name to its age in minutes. The map is
     *                                                         set to null if ZK metadata is not available or the
     *                                                         creation time is not found for at least one consuming
     *                                                         segment.
     *                                                         The age of a segment is determined by its creation
     *                                                         time from ZK metadata. Segment age is an approximation
     *                                                         to data age for a consuming segment. It may not reflect
     *                                                         the actual oldest age of data in the consuming segment.
     *                                                         For reasons, a segment could consume events which date
     *                                                         before the segment created. We collect segment age
     *                                                         here as there is no obvious way to get the age of the
     *                                                         oldest data in the stream for a specific consuming
     *                                                         segment
     * @param serverConsumingSegmentSummary ConsumingSegmentSummaryPerServer per server
     */
    @JsonCreator
    public ConsumingSegmentToBeMovedSummary(
        @JsonProperty("numConsumingSegmentsToBeMoved") int numConsumingSegmentsToBeMoved,
        @JsonProperty("numServersGettingConsumingSegmentsAdded") int numServersGettingConsumingSegmentsAdded,
        @JsonProperty("consumingSegmentsToBeMovedWithMostOffsetsToCatchUp") @Nullable
        Map<String, Integer> consumingSegmentsToBeMovedWithMostOffsetsToCatchUp,
        @JsonProperty("consumingSegmentsToBeMovedWithOldestAgeInMinutes") @Nullable
        Map<String, Integer> consumingSegmentsToBeMovedWithOldestAgeInMinutes,
        @JsonProperty("serverConsumingSegmentSummary") @Nullable
        Map<String, ConsumingSegmentSummaryPerServer> serverConsumingSegmentSummary) {
      _numConsumingSegmentsToBeMoved = numConsumingSegmentsToBeMoved;
      _numServersGettingConsumingSegmentsAdded = numServersGettingConsumingSegmentsAdded;
      _consumingSegmentsToBeMovedWithMostOffsetsToCatchUp = consumingSegmentsToBeMovedWithMostOffsetsToCatchUp;
      _consumingSegmentsToBeMovedWithOldestAgeInMinutes = consumingSegmentsToBeMovedWithOldestAgeInMinutes;
      _serverConsumingSegmentSummary = serverConsumingSegmentSummary;
    }

    @JsonProperty
    public int getNumConsumingSegmentsToBeMoved() {
      return _numConsumingSegmentsToBeMoved;
    }

    @JsonProperty
    public int getNumServersGettingConsumingSegmentsAdded() {
      return _numServersGettingConsumingSegmentsAdded;
    }

    @JsonProperty
    public Map<String, Integer> getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp() {
      return _consumingSegmentsToBeMovedWithMostOffsetsToCatchUp;
    }

    @JsonProperty
    public Map<String, Integer> getConsumingSegmentsToBeMovedWithOldestAgeInMinutes() {
      return _consumingSegmentsToBeMovedWithOldestAgeInMinutes;
    }

    @JsonProperty
    public Map<String, ConsumingSegmentSummaryPerServer> getServerConsumingSegmentSummary() {
      return _serverConsumingSegmentSummary;
    }

    public static class ConsumingSegmentSummaryPerServer {
      private final int _numConsumingSegmentsToBeAdded;
      private final int _totalOffsetsToCatchUpAcrossAllConsumingSegments;

      /**
       * Constructor for ConsumingSegmentSummaryPerServer
       * @param numConsumingSegmentsToBeAdded number of consuming segments to be added to this server
       * @param totalOffsetsToCatchUpAcrossAllConsumingSegments total number of offsets to catch up across all consuming
       *                                                         segments. The number of offsets to catch up for a
       *                                                         consuming segment is essentially the difference
       *                                                         between the latest offset of the stream and the
       *                                                         segment's start offset of the stream. Set to -1 if
       *                                                         the offsets to catch up could not be determined for
       *                                                         at least one consuming segment
       */
      @JsonCreator
      public ConsumingSegmentSummaryPerServer(
          @JsonProperty("numConsumingSegmentsToBeAdded") int numConsumingSegmentsToBeAdded,
          @JsonProperty("totalOffsetsToCatchUpAcrossAllConsumingSegments")
          int totalOffsetsToCatchUpAcrossAllConsumingSegments) {
        _numConsumingSegmentsToBeAdded = numConsumingSegmentsToBeAdded;
        _totalOffsetsToCatchUpAcrossAllConsumingSegments = totalOffsetsToCatchUpAcrossAllConsumingSegments;
      }

      @JsonProperty
      public int getNumConsumingSegmentsToBeAdded() {
        return _numConsumingSegmentsToBeAdded;
      }

      @JsonProperty
      public int getTotalOffsetsToCatchUpAcrossAllConsumingSegments() {
        return _totalOffsetsToCatchUpAcrossAllConsumingSegments;
      }
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class SegmentInfo {
    // TODO: Add a metric to estimate the total time it will take to rebalance
    private final int _totalSegmentsToBeMoved;
    private final int _maxSegmentsAddedToASingleServer;
    private final long _estimatedAverageSegmentSizeInBytes;
    private final long _totalEstimatedDataToBeMovedInBytes;
    private final RebalanceChangeInfo _replicationFactor;
    private final RebalanceChangeInfo _numSegmentsInSingleReplica;
    private final RebalanceChangeInfo _numSegmentsAcrossAllReplicas;
    private final ConsumingSegmentToBeMovedSummary _consumingSegmentToBeMovedSummary;

    /**
     * Constructor for SegmentInfo
     * @param totalSegmentsToBeMoved total number of segments to be moved as part of this rebalance
     * @param maxSegmentsAddedToASingleServer maximum segments added to a single server as part of this rebalance
     * @param estimatedAverageSegmentSizeInBytes estimated average size of segments in bytes
     * @param totalEstimatedDataToBeMovedInBytes total estimated amount of data to be moved as part of this rebalance
     * @param replicationFactor replication factor before and after this rebalance
     * @param numSegmentsInSingleReplica number of segments in single replica before and after this rebalance
     * @param numSegmentsAcrossAllReplicas total number of segments across all replicas before and after this rebalance
     * @param consumingSegmentToBeMovedSummary consuming segment summary. Set to null if the table is an offline table
     */
    @JsonCreator
    public SegmentInfo(@JsonProperty("totalSegmentsToBeMoved") int totalSegmentsToBeMoved,
        @JsonProperty("maxSegmentsAddedToASingleServer") int maxSegmentsAddedToASingleServer,
        @JsonProperty("estimatedAverageSegmentSizeInBytes") long estimatedAverageSegmentSizeInBytes,
        @JsonProperty("totalEstimatedDataToBeMovedInBytes") long totalEstimatedDataToBeMovedInBytes,
        @JsonProperty("replicationFactor") @Nullable RebalanceChangeInfo replicationFactor,
        @JsonProperty("numSegmentsInSingleReplica") @Nullable RebalanceChangeInfo numSegmentsInSingleReplica,
        @JsonProperty("numSegmentsAcrossAllReplicas") @Nullable RebalanceChangeInfo numSegmentsAcrossAllReplicas,
        @JsonProperty("consumingSegmentToBeMovedSummary") @Nullable
        ConsumingSegmentToBeMovedSummary consumingSegmentToBeMovedSummary) {
      _totalSegmentsToBeMoved = totalSegmentsToBeMoved;
      _maxSegmentsAddedToASingleServer = maxSegmentsAddedToASingleServer;
      _estimatedAverageSegmentSizeInBytes = estimatedAverageSegmentSizeInBytes;
      _totalEstimatedDataToBeMovedInBytes = totalEstimatedDataToBeMovedInBytes;
      _replicationFactor = replicationFactor;
      _numSegmentsInSingleReplica = numSegmentsInSingleReplica;
      _numSegmentsAcrossAllReplicas = numSegmentsAcrossAllReplicas;
      _consumingSegmentToBeMovedSummary = consumingSegmentToBeMovedSummary;
    }

    @JsonProperty
    public int getTotalSegmentsToBeMoved() {
      return _totalSegmentsToBeMoved;
    }

    @JsonProperty
    public int getMaxSegmentsAddedToASingleServer() {
      return _maxSegmentsAddedToASingleServer;
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
    public RebalanceChangeInfo getReplicationFactor() {
      return _replicationFactor;
    }

    @JsonProperty
    public RebalanceChangeInfo getNumSegmentsInSingleReplica() {
      return _numSegmentsInSingleReplica;
    }

    @JsonProperty
    public RebalanceChangeInfo getNumSegmentsAcrossAllReplicas() {
      return _numSegmentsAcrossAllReplicas;
    }

    @JsonProperty
    public ConsumingSegmentToBeMovedSummary getConsumingSegmentToBeMovedSummary() {
      return _consumingSegmentToBeMovedSummary;
    }
  }

  public enum ServerStatus {
    // ADDED if the server is newly added as part of rebalance;
    // REMOVED if the server is removed as part of rebalance;
    // UNCHANGED if the server status is unchanged as part of rebalance;
    ADDED, REMOVED, UNCHANGED
  }
}
