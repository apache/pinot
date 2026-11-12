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
package org.apache.pinot.broker.routing.adaptiveserverselector;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.broker.routing.instanceselector.SegmentInstanceCandidate;


/// A server selector that implements priority-based server selection based on pools.
/// This selector works in conjunction with an [AdaptiveServerSelector] to provide
/// a two-level selection strategy:
///
/// 1. First level: Select servers based on pool priorities
/// 2. Second level: Use adaptive selection within the chosen pool
///
/// The selector maintains the following invariants:
///
/// - Servers from preferred pools are always selected before non-preferred pools
/// - Within each pools, servers are selected using adaptive selection criteria
/// - When no preferred pools are available, falls back to non-preferred pools
public class PriorityPoolInstanceSelector {

  /// The underlying adaptive server selector used for selection within pools
  private final AdaptiveServerSelector _adaptiveServerSelector;

  /// Sentinel value used to pool all non-preferred servers together
  private static final int SENTINEL_POOL_OF_NON_PREFERRED_SERVERS = Integer.MAX_VALUE;

  /// Creates a new priority pool instance selector with the given adaptive server selector.
  ///
  /// @param adaptiveServerSelector the adaptive server selector to use for selection within pools
  /// @throws IllegalArgumentException if adaptiveServerSelector is null
  public PriorityPoolInstanceSelector(AdaptiveServerSelector adaptiveServerSelector) {
    assert adaptiveServerSelector != null;
    _adaptiveServerSelector = adaptiveServerSelector;
  }

  /// Returns a [QuerySelector] that selects server instances for the segments of one query, with the ordered
  /// preferred pools of the query resolved once.
  ///
  /// @param ctx the server selection context containing ordered preferred pools
  public QuerySelector forQuery(ServerSelectionContext ctx) {
    return new QuerySelector(ctx.getOrderedPreferredPools());
  }

  /// Invoke adaptiveServerSelector to get the original ranking the servers (min first). Reorder the servers based on
  /// the pool preference. The head of the OrderedPreferredPools list is the most preferred pool.
  /// The servers in the same pool are ranked by the original ranking.
  ///
  /// Example:
  ///
  /// ```
  /// Given:
  ///   - Server candidates:
  ///     - server1 (pool 1, score 80)
  ///     - server2 (pool 2, score 70)
  ///     - server3 (pool 1, score 90)
  ///     - server4 (pool 3, score 60)
  ///   - Ordered preferred pools: [2, 1]
  ///
  /// Original ranking by score would be: [server4, server2, server1, server3]
  /// Final ranking after pool preference: [server2, server1, server3, server4]
  /// Because:
  ///   1. pool 2 servers come first (server2)
  ///   2. pool 1 servers come next, maintaining their relative order (server1, server3)
  ///   3. Remaining servers come last (server4)
  /// ```
  ///
  /// @param ctx the server selection context containing ordered preferred pools
  /// @param serverCandidates the server candidates to be ranked
  /// @return the ranked servers, ordered by pool preference and then by original ranking within each pool
  public List<String> rank(ServerSelectionContext ctx, List<SegmentInstanceCandidate> serverCandidates) {
    if (serverCandidates == null || serverCandidates.isEmpty()) {
      return List.of();
    }

    // TODO: return the pos of the selected server in the input array rather than the server instance id.
    List<Pair<String, Double>> serverRankListWithScores = _adaptiveServerSelector.fetchServerRankingsWithScores(
        serverCandidates.stream()
        .map(SegmentInstanceCandidate::getInstance)
        .collect(Collectors.toList()));
    List<Integer> pools = new ArrayList<>(ctx.getOrderedPreferredPools());
    if (pools.isEmpty()) {
      return serverRankListWithScores.stream().map(Pair::getLeft).collect(Collectors.toList());
    }
    Map<String, SegmentInstanceCandidate> idToCandidate = serverCandidates.stream()
        .map(candidate -> new ImmutablePair<>(candidate.getInstance(), candidate))
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    // Create a set of preferred pools for efficient lookup
    Set<Integer> preferredPools = new HashSet<>(pools);
    Map<Integer, List<String>> poolToRankedServers = new HashMap<>();
    for (Pair<String, Double> entry : serverRankListWithScores) {
      int pool = idToCandidate.get(entry.getLeft()).getPool();
      // If the pool is not in the preferred pools list, assign it the sentinel pool
      pool = preferredPools.contains(pool) ? pool : SENTINEL_POOL_OF_NON_PREFERRED_SERVERS;
      poolToRankedServers.computeIfAbsent(pool, k -> new ArrayList<>()).add(entry.getLeft());
    }

    // Add the sentinel pool to the end of the pools list to ensure its pool members are included in the tail
    pools.add(SENTINEL_POOL_OF_NON_PREFERRED_SERVERS);

    // Build the final ranked list by processing pools in order
    List<String> rankedServers = new ArrayList<>();
    for (int pool : pools) {
      List<String> instancesInPool = poolToRankedServers.get(pool);
      if (instancesInPool != null) {
        rankedServers.addAll(instancesInPool);
      }
    }
    return rankedServers;
  }

  /// Selects server instances for the segments of one query based on pool preferences. The selection process follows
  /// these steps:
  /// 1. Assigns each candidate the priority of its pool: its position in the ordered preferred pools. All the servers
  ///    not in a preferred pool share the priority of pool `Integer.MAX_VALUE`, as [PriorityPoolInstanceSelector#rank]
  ///    groups them: the lowest priority, or the position of `Integer.MAX_VALUE` when it is itself a preferred pool
  /// 2. Keeps the candidates with the highest priority
  /// 3. Uses adaptiveServerSelector to choose one of them
  ///
  /// Example 1 - Preferred pool has servers:
  ///
  /// ```
  /// Candidates:
  ///   - server1 (pool 1)
  ///   - server2 (pool 2)
  ///   - server3 (pool 1)
  /// Preferred pools: [2, 1]
  /// Result: server2 is selected (from pool 2, highest priority)
  /// ```
  ///
  /// Example 2 - Fallback to second preferred pool:
  ///
  /// ```
  /// Candidates:
  ///   - server1 (pool 1)
  ///   - server3 (pool 1)
  ///   - server4 (pool 3)
  /// Preferred pools: [2, 1]
  /// Result: adaptiveServerSelector chooses between server1 and server3 (from pool 1)
  /// ```
  ///
  /// Example 3 - Fallback to non-preferred pool:
  ///
  /// ```
  /// Candidates:
  ///   - server4 (pool 3)
  ///   - server5 (pool 3)
  /// Preferred pools: [2, 1]
  /// Result: adaptiveServerSelector chooses between server4 and server5 (from pool 3)
  /// ```
  ///
  /// Not thread safe. One instance per query.
  public final class QuerySelector {
    // Pool to priority (lower is preferred), or null when the query has no preferred pool
    @Nullable
    private final Int2IntOpenHashMap _priorityByPool;
    // Reused for every selection
    private final List<String> _candidateInstances = new ArrayList<>();

    private QuerySelector(List<Integer> orderedPreferredPools) {
      if (orderedPreferredPools.isEmpty()) {
        _priorityByPool = null;
      } else {
        int numPreferredPools = orderedPreferredPools.size();
        _priorityByPool = new Int2IntOpenHashMap(numPreferredPools + 1);
        for (int priority = 0; priority < numPreferredPools; priority++) {
          _priorityByPool.putIfAbsent((int) orderedPreferredPools.get(priority), priority);
        }
        // The servers not in a preferred pool share the priority of the sentinel pool: its position when it is itself
        // a preferred pool, and the lowest priority otherwise
        _priorityByPool.putIfAbsent(SENTINEL_POOL_OF_NON_PREFERRED_SERVERS, numPreferredPools);
        _priorityByPool.defaultReturnValue(_priorityByPool.get(SENTINEL_POOL_OF_NON_PREFERRED_SERVERS));
      }
    }

    /// Returns the index of the selected server instance in the given non-empty candidates.
    public int select(List<SegmentInstanceCandidate> candidates) {
      int numCandidates = candidates.size();
      _candidateInstances.clear();
      if (_priorityByPool == null) {
        for (int i = 0; i < numCandidates; i++) {
          _candidateInstances.add(candidates.get(i).getInstance());
        }
      } else {
        int highestPriority = Integer.MAX_VALUE;
        for (int i = 0; i < numCandidates; i++) {
          highestPriority = Math.min(highestPriority, _priorityByPool.get(candidates.get(i).getPool()));
        }
        for (int i = 0; i < numCandidates; i++) {
          SegmentInstanceCandidate candidate = candidates.get(i);
          if (_priorityByPool.get(candidate.getPool()) == highestPriority) {
            _candidateInstances.add(candidate.getInstance());
          }
        }
      }
      String selectedInstance = _adaptiveServerSelector.select(_candidateInstances);
      for (int i = 0; i < numCandidates; i++) {
        if (candidates.get(i).getInstance().equals(selectedInstance)) {
          return i;
        }
      }
      throw new IllegalStateException(
          "Selected instance: " + selectedInstance + " is not one of the candidates: " + _candidateInstances);
    }
  }
}
