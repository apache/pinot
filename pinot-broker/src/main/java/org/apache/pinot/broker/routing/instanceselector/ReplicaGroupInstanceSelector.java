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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.broker.routing.adaptiveserverselector.ServerSelectionContext;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;


/// Instance selector for replica-group routing strategy.
///
/// The selection algorithm will always evenly distribute the traffic to all replicas of each segment, and will select
/// the same index of the enabled instances for all segments with the same number of replicas. The algorithm is very
/// light-weight and will do best effort to select the least servers for the request.
///
/// The algorithm relies on the mirror segment assignment from replica-group segment assignment strategy. With mirror
/// segment assignment, any server in one replica-group will always have a corresponding server in other replica-groups
/// that have the same segments assigned. For an example, if S1 is a server in replica-group 1, and it has mirror server
/// S2 in replica-group 2 and S3 in replica-group 3. All segments assigned to S1 will also be assigned to S2 and S3. In
/// stable scenario (external view matches ideal state), all segments assigned to S1 will have the same enabled
/// instances of \[S1, S2, S3\] sorted (in alphabetical order). If we pick the same index of enabled instances for all
/// segments for a request, only one of S1, S2, S3 will be picked, so it is guaranteed that we pick the least server
/// instances for the request (there is no guarantee on choosing servers from the same replica-group though). In
/// transitioning/error scenario (external view does not match ideal state), there is no guarantee on picking the least
/// server instances, but the traffic is guaranteed to be evenly distributed to all available instances to avoid
/// overwhelming hotspot servers.
///
/// If the query option NUM_REPLICA_GROUPS_TO_QUERY is provided, the servers to be picked will be from different
/// replica groups such that segments are evenly distributed amongst the provided value of NUM_REPLICA_GROUPS_TO_QUERY.
/// Thus in case of \[S1, S2, S3\] if NUM_REPLICA_GROUPS_TO_QUERY = 2, the ReplicaGroup S1 and ReplicaGroup S2 will be
/// selected such that half the segments will come from S1 and other half from S2. If NUM_REPLICA_GROUPS_TO_QUERY value
/// is much greater than available servers, then ReplicaGroupInstanceSelector will behave similar to
/// BalancedInstanceSelector.
///
/// If AdaptiveServerSelection is enabled, a single snapshot of the server ranking is fetched. This ranking is
/// referenced to pick the best available server for each segment. The algorithm ends up picking the minimum number of
/// servers required to process a query because it references a single snapshot of the server rankings. Currently,
/// NUM_REPLICA_GROUPS_TO_QUERY is not supported if AdaptiveServerSelection is enabled.
public class ReplicaGroupInstanceSelector extends BaseInstanceSelector {
  private static final int UNRANKED = Integer.MAX_VALUE;

  @Override
  public SelectionResult select(Collection<String> segments, int requestId, SegmentStates segmentStates,
      Map<String, String> queryOptions) {
    return selectWithContext(segments, requestId, segmentStates, new ServerSelectionContext(queryOptions, _config));
  }

  /// Selects with the adaptive server selection when it is enabled, unless the query uses fixed replica routing.
  protected SelectionResult selectWithContext(Collection<String> segments, int requestId, SegmentStates segmentStates,
      ServerSelectionContext ctx) {
    // Fixed replica routing configured on the broker or table disables adaptive routing, but it can still be set per
    // query, in which case it is preferred over adaptive routing
    if (_priorityPoolInstanceSelector != null && !ctx.isUseFixedReplica()) {
      SelectionResult selectionResult = selectAdaptive(segments, requestId, segmentStates, ctx);
      if (selectionResult != null) {
        return selectionResult;
      }
    }
    return selectServers(segments, requestId, segmentStates, ctx);
  }

  /// Selects without the adaptive server selection.
  protected SelectionResult selectServers(Collection<String> segments, int requestId, SegmentStates segmentStates,
      ServerSelectionContext ctx) {
    Integer numReplicaGroupsToQuery = QueryOptionsUtils.getNumReplicaGroupsToQuery(ctx.getQueryOptions());
    // With fixed replica routing, the candidate index is derived from the raw table name instead of the request id
    int baseIndex = ctx.isUseFixedReplica() ? _tableNameHashForFixedReplicaRouting : requestId;
    SelectionCollector collector = numReplicaGroupsToQuery != null && numReplicaGroupsToQuery > 1
        ? selectRotatingCandidateIndex(segments, segmentStates, baseIndex, numReplicaGroupsToQuery)
        : selectSameCandidateIndex(segments, segmentStates, baseIndex);
    collector.emitPoolSegQueries(_brokerMetrics, ctx.getQueryOptions());
    return collector.build();
  }

  /// Spreads the segments over the given number of replica groups, by advancing the candidate index for each segment.
  private SelectionCollector selectRotatingCandidateIndex(Collection<String> segments, SegmentStates segmentStates,
      int baseIndex, int numReplicaGroupsToQuery) {
    SelectionCollector collector = new SelectionCollector(segmentStates, segments.size(), true);
    int numReplicaGroups = numReplicaGroupsToQuery;
    int replicaOffset = 0;
    for (String segment : segments) {
      GroupRouting routing = collector.resolveRouting(segment);
      if (routing == null) {
        continue;
      }
      int numCandidates = routing.getNumCandidates();
      collector.add(routing, Math.floorMod(baseIndex + replicaOffset, numCandidates), segment);
      if (numReplicaGroups > numCandidates) {
        numReplicaGroups = numCandidates;
      }
      replicaOffset = (replicaOffset + 1) % numReplicaGroups;
    }
    return collector;
  }

  /// Selects the best ranked candidate of each segment, from one snapshot of the server ranking, or returns `null`
  /// when the ranking is empty.
  @Nullable
  private SelectionResult selectAdaptive(Collection<String> segments, int requestId, SegmentStates segmentStates,
      ServerSelectionContext ctx) {
    // Only rank the instances serving the queried segments. Keep the group of each segment to resolve it only once.
    int numSegments = segments.size();
    int[] groupIds = new int[numSegments];
    boolean[] queriedBySlot = new boolean[segmentStates.getNumSlots()];
    IntArrayList queriedSlotList = new IntArrayList();
    List<SegmentInstanceCandidate> candidatesToRank = new ArrayList<>();
    int index = 0;
    for (String segment : segments) {
      int groupId = segmentStates.getGroupId(segment);
      groupIds[index++] = groupId;
      if (groupId != SegmentStates.UNKNOWN_SEGMENT) {
        GroupRouting routing = segmentStates.getGroupRouting(groupId);
        int numCandidates = routing.getNumCandidates();
        for (int i = 0; i < numCandidates; i++) {
          int slot = routing.getSlot(i);
          if (!queriedBySlot[slot]) {
            queriedBySlot[slot] = true;
            queriedSlotList.add(slot);
            candidatesToRank.add(routing.getCandidates().get(i));
          }
        }
      }
    }

    // Fetch the ranking before selecting for the segments. This is important to make sure that we pick the least
    // amount of instances for a query by referring to a single snapshot of the rankings.
    List<String> rankedInstances = _priorityPoolInstanceSelector.rank(ctx, candidatesToRank);
    if (rankedInstances.isEmpty()) {
      return null;
    }
    // Only the ranks of the queried slots are read
    int[] rankBySlot = new int[segmentStates.getNumSlots()];
    int numQueriedSlots = queriedSlotList.size();
    for (int i = 0; i < numQueriedSlots; i++) {
      rankBySlot[queriedSlotList.getInt(i)] = UNRANKED;
    }
    int numRankedInstances = rankedInstances.size();
    for (int rank = 0; rank < numRankedInstances; rank++) {
      int slot = segmentStates.getSlot(rankedInstances.get(rank));
      if (slot != SegmentGroups.NO_SLOT) {
        rankBySlot[slot] = rank;
      }
    }

    Integer numReplicaGroupsToQuery = QueryOptionsUtils.getNumReplicaGroupsToQuery(ctx.getQueryOptions());
    int numReplicaGroups = numReplicaGroupsToQuery != null ? numReplicaGroupsToQuery : 1;
    int replicaOffset = 0;
    SelectionCollector collector = new SelectionCollector(segmentStates, numSegments, false);
    index = 0;
    for (String segment : segments) {
      GroupRouting routing = collector.resolveRouting(segment, groupIds[index++]);
      if (routing == null) {
        continue;
      }
      int numCandidates = routing.getNumCandidates();
      // Use the instance with the best rank if all candidates are ranked, else use the round-robin selected instance.
      // As of 8 July 2026, this fallback is unreachable, but new implementations could require it.
      int candidateIndex = -1;
      int bestRank = UNRANKED;
      for (int i = 0; i < numCandidates; i++) {
        int rank = rankBySlot[routing.getSlot(i)];
        if (rank == UNRANKED) {
          candidateIndex = Math.floorMod(requestId + replicaOffset, numCandidates);
          break;
        }
        if (rank < bestRank) {
          bestRank = rank;
          candidateIndex = i;
        }
      }
      collector.add(routing, candidateIndex, segment);
      if (numReplicaGroups > numCandidates) {
        numReplicaGroups = numCandidates;
      }
      replicaOffset = (replicaOffset + 1) % numReplicaGroups;
    }
    collector.emitPoolSegQueries(_brokerMetrics, ctx.getQueryOptions());
    return collector.build();
  }

  /// {@inheritDoc}
  ///
  /// Upsert and dedup tables need all the segments of a partition to be routed to the same instance.
  @Override
  boolean isStrictReplicaGroupRouting() {
    return _tableConfig.isUpsertEnabled() || _tableConfig.isDedupEnabled();
  }
}
