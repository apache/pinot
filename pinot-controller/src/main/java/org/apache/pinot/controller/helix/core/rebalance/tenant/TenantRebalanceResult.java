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
package org.apache.pinot.controller.helix.core.rebalance.tenant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.controller.helix.core.rebalance.RebalancePreCheckerResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSummaryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "jobId", "totalTables", "statusSummary", "aggregatedPreChecksResult",
    "aggregatedRebalanceSummary", "rebalanceTableResults"
})
public class TenantRebalanceResult {
  private static final Logger LOGGER = LoggerFactory.getLogger(TenantRebalanceResult.class);
  private String _jobId;
  private Map<String, RebalanceResult> _rebalanceTableResults;

  @JsonCreator
  public TenantRebalanceResult(
      @JsonProperty("jobId") String jobId,
      @JsonProperty("rebalanceTableResults") Map<String, RebalanceResult> rebalanceTableResults,
      @JsonProperty("totalTables") int totalTables,
      @JsonProperty("statusSummary") Map<RebalanceResult.Status, Integer> statusSummary,
      @JsonProperty("aggregatedPreChecksResult") Map<String, AggregatedPrecheckResult> aggregatedPreChecksResult,
      @JsonProperty("aggregatedRebalanceSummary") RebalanceSummaryResult aggregatedRebalanceSummary) {
    _jobId = jobId;
    _rebalanceTableResults = rebalanceTableResults;
    _totalTables = totalTables;
    _statusSummary = statusSummary;
    _aggregatedPreChecksResult = aggregatedPreChecksResult;
    _aggregatedRebalanceSummary = aggregatedRebalanceSummary;
  }

  // Aggregated view fields
  private final int _totalTables;
  private Map<RebalanceResult.Status, Integer> _statusSummary;

  // Aggregated details from RebalanceResult fields
  private Map<String, AggregatedPrecheckResult> _aggregatedPreChecksResult;
  private RebalanceSummaryResult _aggregatedRebalanceSummary;

  public TenantRebalanceResult(String jobId, Map<String, RebalanceResult> rebalanceTableResults, boolean verbose) {
    _jobId = jobId;
    _totalTables = rebalanceTableResults.size();

    // Compute aggregated information
    computeStatusSummary(rebalanceTableResults);
    computeAggregatedPreChecks(rebalanceTableResults);
    computeAggregatedRebalanceSummary(rebalanceTableResults);

    if (verbose) {
      _rebalanceTableResults = rebalanceTableResults;
    } else {
      _rebalanceTableResults = new HashMap<>();
      rebalanceTableResults.forEach((table, result) -> {
        _rebalanceTableResults.put(table, new RebalanceResult(result.getJobId(), result.getStatus(),
            result.getDescription(), null, null, null, null, null));
      });
    }
  }

  private void computeStatusSummary(Map<String, RebalanceResult> rebalanceTableResults) {
    _statusSummary = new HashMap<>();
    for (RebalanceResult result : rebalanceTableResults.values()) {
      RebalanceResult.Status status = result.getStatus();
      _statusSummary.put(status, _statusSummary.getOrDefault(status, 0) + 1);
    }
  }

  private void computeAggregatedPreChecks(Map<String, RebalanceResult> rebalanceTableResults) {
    _aggregatedPreChecksResult = new HashMap<>();

    // Organize pre-check results by pre-check name
    Map<String, Map<String, String>> passedTablesByCheck = new HashMap<>();
    Map<String, Map<String, String>> warnedTablesByCheck = new HashMap<>();
    Map<String, Map<String, String>> erroredTablesByCheck = new HashMap<>();

    // Process each table's pre-check results
    for (Map.Entry<String, RebalanceResult> entry : rebalanceTableResults.entrySet()) {
      String tableName = entry.getKey();
      RebalanceResult result = entry.getValue();

      if (result.getPreChecksResult() != null) {
        for (Map.Entry<String, RebalancePreCheckerResult> checkEntry : result.getPreChecksResult().entrySet()) {
          String checkName = checkEntry.getKey();
          RebalancePreCheckerResult checkResult = checkEntry.getValue();

          // Initialize maps for this check if not present
          passedTablesByCheck.computeIfAbsent(checkName, k -> new HashMap<>());
          warnedTablesByCheck.computeIfAbsent(checkName, k -> new HashMap<>());
          erroredTablesByCheck.computeIfAbsent(checkName, k -> new HashMap<>());

          // Categorize table based on this specific check's status
          String message = checkResult.getMessage() != null ? checkResult.getMessage() : "";
          switch (checkResult.getPreCheckStatus()) {
            case PASS:
              passedTablesByCheck.get(checkName).put(tableName, message);
              break;
            case WARN:
              warnedTablesByCheck.get(checkName).put(tableName, message);
              break;
            case ERROR:
              erroredTablesByCheck.get(checkName).put(tableName, message);
              break;
            default:
              LOGGER.warn("Unknown pre-check status '{}' for table '{}', check '{}'. Ignoring.",
                  checkResult.getPreCheckStatus(), tableName, checkName);
              break; // Ignore unknown statuses
          }
        }
      }
    }

    // Create AggregatedPrecheckResult for each pre-check type
    for (String checkName : passedTablesByCheck.keySet()) {
      Map<String, String> passedTables = passedTablesByCheck.get(checkName);
      Map<String, String> warnedTables = warnedTablesByCheck.get(checkName);
      Map<String, String> erroredTables = erroredTablesByCheck.get(checkName);

      _aggregatedPreChecksResult.put(checkName, new AggregatedPrecheckResult(
          passedTables.size(),
          warnedTables.size(),
          erroredTables.size(),
          passedTables,
          warnedTables,
          erroredTables
      ));
    }
  }

  private void computeAggregatedRebalanceSummary(Map<String, RebalanceResult> rebalanceTableResults) {
    List<RebalanceSummaryResult> summaryResults = new ArrayList<>();

    for (RebalanceResult result : rebalanceTableResults.values()) {
      if (result.getRebalanceSummaryResult() != null) {
        summaryResults.add(result.getRebalanceSummaryResult());
      }
    }

    // Step 1: Aggregate server change info across all tables first
    Map<String, AggregatedServerSegmentChangeInfo> serverAggregates = aggregateServerSegmentChangeInfo(summaryResults);

    // Step 2: Use aggregated server data to create both ServerInfo and SegmentInfo
    AggregatedServerInfo aggregatedServerInfo = new AggregatedServerInfo(serverAggregates);
    AggregatedSegmentInfo aggregatedSegmentInfo = new AggregatedSegmentInfo(summaryResults, serverAggregates);
    List<RebalanceSummaryResult.TagInfo> aggregatedTagsInfo =
        createAggregatedTagsInfo(summaryResults, serverAggregates);
    _aggregatedRebalanceSummary = new RebalanceSummaryResult(
        aggregatedServerInfo,
        aggregatedSegmentInfo,
        aggregatedTagsInfo
    );
  }

  @JsonProperty
  public String getJobId() {
    return _jobId;
  }

  @JsonProperty
  public int getTotalTables() {
    return _totalTables;
  }

  @JsonProperty
  public Map<RebalanceResult.Status, Integer> getStatusSummary() {
    return _statusSummary;
  }

  @JsonProperty
  public Map<String, AggregatedPrecheckResult> getAggregatedPreChecksResult() {
    return _aggregatedPreChecksResult;
  }

  @JsonProperty
  public RebalanceSummaryResult getAggregatedRebalanceSummary() {
    return _aggregatedRebalanceSummary;
  }

  @JsonProperty
  public Map<String, RebalanceResult> getRebalanceTableResults() {
    return _rebalanceTableResults;
  }

  public void setJobId(String jobId) {
    _jobId = jobId;
  }

  public void setRebalanceTableResults(Map<String, RebalanceResult> rebalanceTableResults) {
    _rebalanceTableResults = rebalanceTableResults;
  }

  /**
   * Aggregated pre-check result that provides table-level pre-check status counts and message mappings
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class AggregatedPrecheckResult {
    private final int _tablesPassedCount;
    private final int _tablesWarnedCount;
    private final int _tablesErroredCount;
    private final Map<String, String> _passedTables;
    private final Map<String, String> _warnedTables;
    private final Map<String, String> _erroredTables;

    public AggregatedPrecheckResult(int tablesPassedCount, int tablesWarnedCount, int tablesErroredCount,
        Map<String, String> passedTables, Map<String, String> warnedTables,
        Map<String, String> erroredTables) {
      _tablesPassedCount = tablesPassedCount;
      _tablesWarnedCount = tablesWarnedCount;
      _tablesErroredCount = tablesErroredCount;
      _passedTables = passedTables;
      _warnedTables = warnedTables;
      _erroredTables = erroredTables;
    }

    @JsonProperty
    public int getTablesPassedCount() {
      return _tablesPassedCount;
    }

    @JsonProperty
    public int getTablesWarnedCount() {
      return _tablesWarnedCount;
    }

    @JsonProperty
    public int getTablesErroredCount() {
      return _tablesErroredCount;
    }

    @JsonProperty
    public Map<String, String> getPassedTables() {
      return _passedTables;
    }

    @JsonProperty
    public Map<String, String> getWarnedTables() {
      return _warnedTables;
    }

    @JsonProperty
    public Map<String, String> getErroredTables() {
      return _erroredTables;
    }
  }

  /**
   * Step 1: Aggregate ServerSegmentChangeInfo across all tables for each server
   */
  private static Map<String, AggregatedServerSegmentChangeInfo> aggregateServerSegmentChangeInfo(
      List<RebalanceSummaryResult> summaryResults) {
    Map<String, AggregatedServerSegmentChangeInfo> serverAggregates = new HashMap<>();

    for (RebalanceSummaryResult summary : summaryResults) {
      if (summary.getServerInfo() != null && summary.getServerInfo().getServerSegmentChangeInfo() != null) {
        for (Map.Entry<String, RebalanceSummaryResult.ServerSegmentChangeInfo> entry : summary.getServerInfo()
            .getServerSegmentChangeInfo()
            .entrySet()) {
          String serverName = entry.getKey();
          RebalanceSummaryResult.ServerSegmentChangeInfo changeInfo = entry.getValue();

          serverAggregates.computeIfAbsent(serverName, k -> new AggregatedServerSegmentChangeInfo())
              .merge(changeInfo);
        }
      }
    }

    return serverAggregates;
  }

  /**
   * Helper class to aggregate ServerSegmentChangeInfo across multiple tables
   */
  private static class AggregatedServerSegmentChangeInfo extends RebalanceSummaryResult.ServerSegmentChangeInfo {

    AggregatedServerSegmentChangeInfo() {
      super(RebalanceSummaryResult.ServerStatus.UNCHANGED, 0, 0, 0, 0, 0, new ArrayList<>());
    }

    void merge(RebalanceSummaryResult.ServerSegmentChangeInfo changeInfo) {
      _totalSegmentsAfterRebalance += changeInfo.getTotalSegmentsAfterRebalance();
      _totalSegmentsBeforeRebalance += changeInfo.getTotalSegmentsBeforeRebalance();
      _segmentsAdded += changeInfo.getSegmentsAdded();
      _segmentsDeleted += changeInfo.getSegmentsDeleted();
      _segmentsUnchanged += changeInfo.getSegmentsUnchanged();

      // Use tag list from any of the change infos (should be consistent)
      if (changeInfo.getTagList() != null) {
        _tagList = new ArrayList<>(Sets.union(new HashSet<>(_tagList), new HashSet<>(changeInfo.getTagList())));
      }
      if (_totalSegmentsAfterRebalance == 0) {
        _serverStatus = RebalanceSummaryResult.ServerStatus.REMOVED;
      } else if (_totalSegmentsBeforeRebalance == 0) {
        _serverStatus = RebalanceSummaryResult.ServerStatus.ADDED;
      } else {
        _serverStatus = RebalanceSummaryResult.ServerStatus.UNCHANGED;
      }
    }
  }

  /**
   * Aggregated ServerInfo that extends RebalanceSummaryResult.ServerInfo
   */
  private static class AggregatedServerInfo extends RebalanceSummaryResult.ServerInfo {
    AggregatedServerInfo(Map<String, AggregatedServerSegmentChangeInfo> serverAggregates) {
      super(0, null, null, null, null, null, null);

      if (serverAggregates.isEmpty()) {
        return;
      }

      Set<String> serversAdded = new HashSet<>();
      Set<String> serversRemoved = new HashSet<>();
      Set<String> serversUnchanged = new HashSet<>();
      Set<String> serversGettingNewSegments = new HashSet<>();
      Map<String, RebalanceSummaryResult.ServerSegmentChangeInfo> finalServerSegmentChangeInfo = new HashMap<>();

      int numServersGettingNewSegments = 0;
      int totalServersBefore = 0;
      int totalServersAfter = 0;

      for (Map.Entry<String, AggregatedServerSegmentChangeInfo> entry : serverAggregates.entrySet()) {
        String serverName = entry.getKey();
        AggregatedServerSegmentChangeInfo aggregate = entry.getValue();

        // Determine server status based on aggregated segment counts
        if (aggregate.getServerStatus() == RebalanceSummaryResult.ServerStatus.REMOVED) {
          serversRemoved.add(serverName);
        } else if (aggregate.getServerStatus() == RebalanceSummaryResult.ServerStatus.ADDED) {
          serversAdded.add(serverName);
        } else if (aggregate.getServerStatus() == RebalanceSummaryResult.ServerStatus.UNCHANGED) {
          serversUnchanged.add(serverName);
        }

        // Track servers getting new segments
        if (aggregate.getSegmentsAdded() > 0) {
          serversGettingNewSegments.add(serverName);
          numServersGettingNewSegments++;
        }

        // Create final ServerSegmentChangeInfo with determined status
        finalServerSegmentChangeInfo.put(serverName, aggregate);

        // Count servers for before/after totals
        if (aggregate.getTotalSegmentsBeforeRebalance() > 0) {
          totalServersBefore++;
        }
        if (aggregate.getTotalSegmentsAfterRebalance() > 0) {
          totalServersAfter++;
        }
      }

      RebalanceSummaryResult.RebalanceChangeInfo
          aggregatedNumServers = new RebalanceSummaryResult.RebalanceChangeInfo(totalServersBefore, totalServersAfter);

      _numServersGettingNewSegments = numServersGettingNewSegments;
      _numServers = aggregatedNumServers;
      _serversAdded = serversAdded;
      _serversRemoved = serversRemoved;
      _serversUnchanged = serversUnchanged;
      _serversGettingNewSegments = serversGettingNewSegments;
      _serverSegmentChangeInfo = finalServerSegmentChangeInfo;
    }
  }

  /**
   * Aggregated SegmentInfo that extends RebalanceSummaryResult.SegmentInfo
   */
  private static class AggregatedSegmentInfo extends RebalanceSummaryResult.SegmentInfo {
    AggregatedSegmentInfo(List<RebalanceSummaryResult> summaryResults,
        Map<String, AggregatedServerSegmentChangeInfo> serverAggregates) {
      super(0, 0, 0, 0, 0, null, null, null, null);

      if (summaryResults.isEmpty()) {
        return;
      }

      int totalSegmentsToBeMoved = 0;
      int totalSegmentsToBeDeleted = 0;
      long totalEstimatedDataToBeMovedInBytes = 0;

      int beforeRebalanceSegmentsInSingleReplica = 0;
      int afterRebalanceSegmentsInSingleReplica = 0;
      int beforeRebalanceSegmentsAcrossAllReplicas = 0;
      int afterRebalanceSegmentsAcrossAllReplicas = 0;

      // Track consuming segment summaries for realtime tables
      List<RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary> consumingSummaries = new ArrayList<>();
      int validSummariesCount = 0;

      // Aggregate data from individual table summaries
      for (RebalanceSummaryResult summary : summaryResults) {
        if (summary.getSegmentInfo() != null) {
          RebalanceSummaryResult.SegmentInfo segmentInfo = summary.getSegmentInfo();
          validSummariesCount++;

          totalSegmentsToBeMoved += segmentInfo.getTotalSegmentsToBeMoved();
          totalSegmentsToBeDeleted += segmentInfo.getTotalSegmentsToBeDeleted();
          if (totalEstimatedDataToBeMovedInBytes >= 0) {
            totalEstimatedDataToBeMovedInBytes = segmentInfo.getTotalEstimatedDataToBeMovedInBytes() < 0 ? -1
                : totalEstimatedDataToBeMovedInBytes + segmentInfo.getTotalEstimatedDataToBeMovedInBytes();
          }

          if (segmentInfo.getNumSegmentsInSingleReplica() != null) {
            beforeRebalanceSegmentsInSingleReplica +=
                segmentInfo.getNumSegmentsInSingleReplica().getValueBeforeRebalance();
            afterRebalanceSegmentsInSingleReplica +=
                segmentInfo.getNumSegmentsInSingleReplica().getExpectedValueAfterRebalance();
          }

          if (segmentInfo.getNumSegmentsAcrossAllReplicas() != null) {
            beforeRebalanceSegmentsAcrossAllReplicas +=
                segmentInfo.getNumSegmentsAcrossAllReplicas().getValueBeforeRebalance();
            afterRebalanceSegmentsAcrossAllReplicas +=
                segmentInfo.getNumSegmentsAcrossAllReplicas().getExpectedValueAfterRebalance();
          }

          if (segmentInfo.getConsumingSegmentToBeMovedSummary() != null) {
            consumingSummaries.add(segmentInfo.getConsumingSegmentToBeMovedSummary());
          }
        }
      }

      if (validSummariesCount == 0) {
        return;
      }

      int maxSegmentsAddedToASingleServer = 0;
      for (AggregatedServerSegmentChangeInfo aggregate : serverAggregates.values()) {
        maxSegmentsAddedToASingleServer = Math.max(maxSegmentsAddedToASingleServer, aggregate.getSegmentsAdded());
      }

      // Calculate average segment size
      long estimatedAverageSegmentSizeInBytes =
          totalEstimatedDataToBeMovedInBytes >= 0 && totalSegmentsToBeMoved > 0 ? totalEstimatedDataToBeMovedInBytes
              / totalSegmentsToBeMoved : 0;

      // Create aggregated RebalanceChangeInfo objects
      RebalanceSummaryResult.RebalanceChangeInfo aggregatedNumSegmentsInSingleReplica =
          new RebalanceSummaryResult.RebalanceChangeInfo(
              beforeRebalanceSegmentsInSingleReplica, afterRebalanceSegmentsInSingleReplica);
      RebalanceSummaryResult.RebalanceChangeInfo aggregatedNumSegmentsAcrossAllReplicas =
          new RebalanceSummaryResult.RebalanceChangeInfo(
              beforeRebalanceSegmentsAcrossAllReplicas, afterRebalanceSegmentsAcrossAllReplicas);

      // Aggregate consuming segment summaries
      RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary aggregatedConsumingSummary =
          aggregateConsumingSegmentSummary(consumingSummaries);

      // Set the computed values
      _totalSegmentsToBeMoved = totalSegmentsToBeMoved;
      _totalSegmentsToBeDeleted = totalSegmentsToBeDeleted;
      _maxSegmentsAddedToASingleServer = maxSegmentsAddedToASingleServer;
      _estimatedAverageSegmentSizeInBytes = estimatedAverageSegmentSizeInBytes;
      _totalEstimatedDataToBeMovedInBytes = totalEstimatedDataToBeMovedInBytes;
      _replicationFactor = null; // This is irrelevant for tenant rebalance
      _numSegmentsInSingleReplica = aggregatedNumSegmentsInSingleReplica;
      _numSegmentsAcrossAllReplicas = aggregatedNumSegmentsAcrossAllReplicas;
      _consumingSegmentToBeMovedSummary = aggregatedConsumingSummary;
    }
  }

  /**
   * Aggregates consuming segment summaries for realtime tables
   */
  private static RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary aggregateConsumingSegmentSummary(
      List<RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary> summaries) {
    if (summaries.isEmpty()) {
      return null;
    }

    int totalNumConsumingSegmentsToBeMoved = 0;

    // Create maps to store all segments by offset and age
    Map<String, Integer> consumingSegmentsWithMostOffsetsPerTable = new HashMap<>();
    Map<String, Integer> consumingSegmentsWithOldestAgePerTable = new HashMap<>();

    // Aggregate ConsumingSegmentSummaryPerServer by server name across all tables
    Map<String, AggregatedConsumingSegmentSummaryPerServer> serverAggregates = new HashMap<>();

    for (RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary summary : summaries) {
      totalNumConsumingSegmentsToBeMoved += summary.getNumConsumingSegmentsToBeMoved();

      // Add one segment with offsets for each table
      if (summary.getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp() != null
          && !summary.getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp().isEmpty()) {
        Map.Entry<String, Integer> consumingSegmentWithMostOffsetsToCatchUp =
            summary.getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp().entrySet().iterator().next();
        consumingSegmentsWithMostOffsetsPerTable.put(consumingSegmentWithMostOffsetsToCatchUp.getKey(),
            consumingSegmentWithMostOffsetsToCatchUp.getValue());
      }

      // Add all segments with ages
      if (summary.getConsumingSegmentsToBeMovedWithOldestAgeInMinutes() != null
          && !summary.getConsumingSegmentsToBeMovedWithOldestAgeInMinutes().isEmpty()) {
        Map.Entry<String, Integer> consumingSegmentWithOldestAge =
            summary.getConsumingSegmentsToBeMovedWithOldestAgeInMinutes().entrySet().iterator().next();
        consumingSegmentsWithOldestAgePerTable.put(consumingSegmentWithOldestAge.getKey(),
            consumingSegmentWithOldestAge.getValue());
      }

      // Aggregate server consuming segment summaries by server name
      if (summary.getServerConsumingSegmentSummary() != null) {
        for (Map.Entry<String,
            RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary.ConsumingSegmentSummaryPerServer> entry
            : summary.getServerConsumingSegmentSummary()
            .entrySet()) {
          String serverName = entry.getKey();
          RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary.ConsumingSegmentSummaryPerServer serverSummary =
              entry.getValue();

          serverAggregates.computeIfAbsent(serverName, k -> new AggregatedConsumingSegmentSummaryPerServer())
              .merge(serverSummary);
        }
      }
    }

    // Sort consuming segments (top one from each table) by offsets and age
    Map<String, Integer> sortedConsumingSegmentsWithMostOffsetsPerTable = new LinkedHashMap<>();
    consumingSegmentsWithMostOffsetsPerTable.entrySet().stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
        .forEach(entry -> sortedConsumingSegmentsWithMostOffsetsPerTable.put(entry.getKey(), entry.getValue()));

    Map<String, Integer> sortedConsumingSegmentsWithOldestAgePerTable = new LinkedHashMap<>();
    consumingSegmentsWithOldestAgePerTable.entrySet().stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
        .forEach(entry -> sortedConsumingSegmentsWithOldestAgePerTable.put(entry.getKey(), entry.getValue()));

    // Convert aggregated server data to final ConsumingSegmentSummaryPerServer map
    Map<String, RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary.ConsumingSegmentSummaryPerServer>
        finalServerConsumingSummary = new HashMap<>(serverAggregates);

    int totalNumServersGettingConsumingSegmentsAdded = finalServerConsumingSummary.size();

    return new RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary(
        totalNumConsumingSegmentsToBeMoved,
        totalNumServersGettingConsumingSegmentsAdded,
        sortedConsumingSegmentsWithMostOffsetsPerTable.isEmpty() ? null
            : sortedConsumingSegmentsWithMostOffsetsPerTable,
        sortedConsumingSegmentsWithOldestAgePerTable.isEmpty() ? null : sortedConsumingSegmentsWithOldestAgePerTable,
        finalServerConsumingSummary.isEmpty() ? null : finalServerConsumingSummary
    );
  }

  /**
   * Helper class to aggregate ConsumingSegmentSummaryPerServer across multiple tables
   */
  private static class AggregatedConsumingSegmentSummaryPerServer
      extends RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary.ConsumingSegmentSummaryPerServer {
    AggregatedConsumingSegmentSummaryPerServer() {
      super(0, 0);
    }

    void merge(
        RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary.ConsumingSegmentSummaryPerServer serverSummary) {
      _numConsumingSegmentsToBeAdded += serverSummary.getNumConsumingSegmentsToBeAdded();

      // Handle offset aggregation - if any table has invalid offset data (-1), mark the aggregate as invalid
      if (serverSummary.getTotalOffsetsToCatchUpAcrossAllConsumingSegments() == -1) {
        _totalOffsetsToCatchUpAcrossAllConsumingSegments = -1;
      } else if (_totalOffsetsToCatchUpAcrossAllConsumingSegments != -1) {
        // Only add to total if we haven't encountered invalid data yet
        _totalOffsetsToCatchUpAcrossAllConsumingSegments +=
            serverSummary.getTotalOffsetsToCatchUpAcrossAllConsumingSegments();
      }
    }
  }

  /**
   * Aggregates TagInfo across multiple RebalanceSummaryResults
   */
  private static List<RebalanceSummaryResult.TagInfo> createAggregatedTagsInfo(
      List<RebalanceSummaryResult> summaryResults,
      Map<String, AggregatedServerSegmentChangeInfo> serverAggregates) {
    Map<String, AggregatedTagInfo> tagAggregates = new HashMap<>();

    // First, aggregate numeric fields from table-level TagInfo
    for (RebalanceSummaryResult summary : summaryResults) {
      if (summary.getTagsInfo() != null) {
        for (RebalanceSummaryResult.TagInfo tagInfo : summary.getTagsInfo()) {
          String tagName = tagInfo.getTagName();
          tagAggregates.computeIfAbsent(tagName, k -> new AggregatedTagInfo(tagName))
              .merge(tagInfo);
        }
      }
    }

    // Second, calculate totalNumServerParticipants from server aggregates
    for (Map.Entry<String, AggregatedServerSegmentChangeInfo> entry : serverAggregates.entrySet()) {
      AggregatedServerSegmentChangeInfo aggregate = entry.getValue();

      // Only count servers with status UNCHANGED or ADDED (not REMOVED)
      boolean isActiveServer = aggregate.getTotalSegmentsAfterRebalance() > 0;
      if (isActiveServer && aggregate.getTagList() != null) {
        for (String tag : aggregate.getTagList()) {
          if (tagAggregates.containsKey(tag)) {
            tagAggregates.get(tag).increaseNumServerParticipants(1);
          }
        }
      }
    }

    if (tagAggregates.isEmpty()) {
      return null;
    }

    return new ArrayList<>(tagAggregates.values());
  }

  /**
   * Helper class to aggregate TagInfo across multiple tables
   */
  private static class AggregatedTagInfo extends RebalanceSummaryResult.TagInfo {

    AggregatedTagInfo(String tagName) {
      super(tagName);
    }

    void merge(RebalanceSummaryResult.TagInfo tagInfo) {
      increaseNumSegmentsUnchanged(tagInfo.getNumSegmentsUnchanged());
      increaseNumSegmentsToDownload(tagInfo.getNumSegmentsToDownload());
      // Note: Do NOT add numServerParticipants here - it will be derived from the aggregated ServerSegmentChangeInfo
    }
  }
}
