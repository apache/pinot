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
package org.apache.pinot.broker.routing.instanceselector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.adaptiveserverselector.ServerSelectionContext;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.HashUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Instance selector for strict replica-group routing strategy.
 *
 * <pre>
 * The strict replica-group routing strategy always routes the query to the instances within the same replica-group.
 * (Note that the replica-group information is derived from the ideal state of the table, where the instances are sorted
 * alphabetically in the instance state map, so the replica-groups in the instance selector might not match the
 * replica-groups in the instance partitions). The goal of this algorithm is to ensure that segments from the same
 * partition are never served from multiple different instances. The instances in a replica-group should have all the
 * online segments (segments with ONLINE/CONSUMING instances in the ideal state and selected by the pre-selector)
 * available (ONLINE/CONSUMING in the external view) in order to serve queries. If any segment is unavailable in the
 * replica-group, we mark the whole replica-group down and not serve queries with this replica-group.
 *
 * The selection algorithm is the same as {@link ReplicaGroupInstanceSelector}, and will always evenly distribute the
 * traffic to all replica-groups that have all online segments available.
 *
 * The algorithm relies on the mirror segment assignment from replica-group segment assignment strategy. With mirror
 * segment assignment, any server in one replica-group will always have a corresponding server in other replica-groups
 * that have the same segments assigned. For example, if S1 is a server in replica-group 1, and it has mirror server
 * S2 in replica-group 2 and S3 in replica-group 3. All segments assigned to S1 will also be assigned to S2 and S3. In
 * stable scenario (external view matches ideal state), all segments assigned to S1 will have the same enabled instances
 * of [S1, S2, S3] sorted (in alphabetical order). If we always pick the same index of enabled instances for all
 * segments, only one of S1, S2, S3 will be picked, and all the segments are processed by the same server. In
 * transitioning/error scenario (external view does not match ideal state), if a segment is down on S1, we mark all
 * segments with the same assignment ([S1, S2, S3]) down on S1 to ensure that we always route the segments to the same
 * replica-group.
 *
 * When adaptive server selection is enabled, this selector uses replica-group-level adaptive routing: it picks the best
 * replica group for the entire query (using worst-case server rank within each group) and routes all segments to
 * that group. This preserves the same-replica-group guarantee while benefiting from adaptive routing intelligence.
 *
 * Note that new segments won't be used to exclude instances from serving when the segment is unavailable.
 * </pre>
 */
public class StrictReplicaGroupInstanceSelector extends ReplicaGroupInstanceSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(StrictReplicaGroupInstanceSelector.class);

  @Override
  void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, Long> newSegmentCreationTimeMap) {
    super.updateSegmentMapsForUpsertTable(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
  }

  @Override
  public InstanceMapping select(List<String> segments, int requestId,
      SegmentStates segmentStates, Map<String, String> queryOptions) {

    if (_adaptiveServerSelector == null || _priorityPoolInstanceSelector == null) {
      ServerSelectionContext ctx = new ServerSelectionContext(queryOptions, _config);
      return selectServers(segments, requestId, segmentStates, null, ctx);
    }

    // Build a map: idealStateReplicaId (replica group) -> distinct servers in that group that are candidates for this
    // query. Keyed by instance name to deduplicate (a server hosting multiple segments appears once per group).
    // Also track how many query segments each replica group can serve (segment coverage) in a single pass.
    // The guard defends against multiple candidates per replica group per segment (e.g., during rebalancing).
    Map<Integer, Map<String, SegmentInstanceCandidate>> replicaGroupToQueryServers = new LinkedHashMap<>();
    Map<Integer, Integer> replicaGroupSegmentCount = new HashMap<>();
    int totalSegmentsWithCandidates = 0;

    Set<Integer> seenReplicaIds = new HashSet<>(); // allocate once and clear per segment to avoid O(n) allocations
    for (String segment : segments) {
      List<SegmentInstanceCandidate> candidates = segmentStates.getCandidates(segment);
      if (candidates == null) {
        continue;
      }

      seenReplicaIds.clear();

      totalSegmentsWithCandidates++;
      for (SegmentInstanceCandidate candidate : candidates) {
        int replicaId = candidate.getIdealStateReplicaId();
        replicaGroupToQueryServers
            .computeIfAbsent(replicaId, k -> new LinkedHashMap<>())
            .putIfAbsent(candidate.getInstance(), candidate);
        if (seenReplicaIds.add(replicaId)) {
          replicaGroupSegmentCount.merge(replicaId, 1, Integer::sum);
        }
      }
    }

    if (replicaGroupToQueryServers.isEmpty()) {
      return new InstanceMapping(Map.of(), Map.of());
    }

    // Collect all distinct query-relevant candidates from replicaGroupToQueryServers, which already
    // deduplicates by instance name. This avoids a second traversal of segments.
    List<SegmentInstanceCandidate> allQueryCandidates = replicaGroupToQueryServers.values().stream()
        .flatMap(m -> m.values().stream())
        .collect(Collectors.toList());
    ServerSelectionContext ctx = new ServerSelectionContext(queryOptions, _config);
    int bestReplicaGroupId = chooseBestReplicaGroup(
        replicaGroupToQueryServers, replicaGroupSegmentCount, totalSegmentsWithCandidates,
        allQueryCandidates, ctx, requestId);
    return selectServersForReplicaGroup(segments, bestReplicaGroupId, segmentStates, queryOptions);
  }

  /**
   * Scores and ranks replica groups using adaptive server selection stats.
   *
   * Each replica group is scored by the worst (maximum) rank among its query-relevant servers.
   * Since scatter-gather query latency is bounded by the slowest server, we pick the group
   * whose bottleneck server is the best (lowest worst-case rank).
   *
   * Groups that can serve ALL query segments (full coverage) are preferred over groups that
   * would drop segments. Among full-coverage groups, the one with the best worst-case rank wins.
   * If no group has full coverage, the best-ranked group is chosen regardless of coverage.
   */
  private int chooseBestReplicaGroup(
      Map<Integer, Map<String, SegmentInstanceCandidate>> replicaGroupToQueryServers,
      Map<Integer, Integer> replicaGroupSegmentCount,
      int totalSegmentsWithCandidates,
      List<SegmentInstanceCandidate> allQueryCandidates,
      ServerSelectionContext ctx,
      int requestId) {

    List<String> rankedServers = _priorityPoolInstanceSelector.rank(ctx, allQueryCandidates);

    // If no stats are available yet (empty ranking), fall back to pool based round-robin.
    if (rankedServers.isEmpty()) {
      List<Integer> groupIds = new ArrayList<>(replicaGroupToQueryServers.keySet());
      return groupIds.get(Math.abs(requestId) % groupIds.size());
    }

    Map<String, Integer> serverRankMap = new HashMap<>(HashUtil.getHashMapCapacity(rankedServers.size()));
    for (int i = 0; i < rankedServers.size(); i++) {
      serverRankMap.put(rankedServers.get(i), i);
    }

    // Pick the group with full coverage first, then best worst-case rank.
    // getOrDefault guards against AdaptiveServerSelector implementations that may not return every
    // submitted server — unranked servers get rank -1 (best), matching HybridSelector's convention.
    // As of 8 July 2026, this fallback is unreachable, but new implementations could require it.
    return replicaGroupToQueryServers.entrySet().stream()
        .min(Comparator.<Map.Entry<Integer, Map<String, SegmentInstanceCandidate>>>comparingInt(
                e -> hasFullCoverage(e.getKey(), replicaGroupSegmentCount, totalSegmentsWithCandidates) ? 0 : 1)
            .thenComparingInt(e -> e.getValue().values().stream()
                .mapToInt(c -> serverRankMap.getOrDefault(c.getInstance(), -1))
                .max().orElse(Integer.MAX_VALUE)))
        .map(Map.Entry::getKey)
        .orElse(-1);
  }

  private boolean hasFullCoverage(int pool, Map<Integer, Integer> replicaGroupSegmentCount,
      int totalSegmentsWithCandidates) {
    return replicaGroupSegmentCount.getOrDefault(pool, 0) >= totalSegmentsWithCandidates;
  }

  /**
   * Routes all segments to the chosen replica group. For each segment, picks the candidate whose
   * idealStateReplicaId matches the selected replica group ID. If no candidate matches (transitional state during
   * rebalancing), the segment is reported as unavailable.
   */
  private InstanceMapping selectServersForReplicaGroup(
      List<String> segments, int replicaGroupId, SegmentStates segmentStates, Map<String, String> queryOptions) {

    Map<String, String> segmentToInstance = new HashMap<>(HashUtil.getHashMapCapacity(segments.size()));
    Map<String, String> optionalSegmentToInstance = new HashMap<>();
    List<String> unavailableSegments = new ArrayList<>();
    Map<Integer, Integer> poolToSegmentCount = new HashMap<>();

    for (String segment : segments) {
      List<SegmentInstanceCandidate> candidates = segmentStates.getCandidates(segment);
      if (candidates == null) {
        continue;
      }

      SegmentInstanceCandidate selected = candidates.stream()
          .filter(c -> c.getIdealStateReplicaId() == replicaGroupId)
          .findFirst()
          .orElse(null);

      // Skip segments with no candidate in the chosen replica group to preserve the strict same-replica-group
      // invariant. We prefer to use groups that cover all queried segments, so this warning only occurs when we've
      // selected a partial group.
      if (selected == null) {
        LOGGER.debug("No candidate found in replica group {} for segment {}; reporting as unavailable",
            replicaGroupId, segment);
        unavailableSegments.add(segment);
        continue;
      }

      if (selected.isOnline()) {
        segmentToInstance.put(segment, selected.getInstance());
      } else {
        optionalSegmentToInstance.put(segment, selected.getInstance());
      }
      poolToSegmentCount.merge(selected.getPool(), 1, Integer::sum);
    }

    // Emit per-pool POOL_SEG_QUERIES metric for observability, matching the parent's pattern.
    for (Map.Entry<Integer, Integer> entry : poolToSegmentCount.entrySet()) {
      _brokerMetrics.addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, entry.getValue(),
          BrokerMetrics.getTagForPreferredPool(queryOptions), String.valueOf(entry.getKey()));
    }

    return new InstanceMapping(segmentToInstance, optionalSegmentToInstance, unavailableSegments);
  }
}
