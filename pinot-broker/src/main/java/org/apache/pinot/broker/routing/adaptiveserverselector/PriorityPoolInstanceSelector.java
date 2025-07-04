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

import java.util.ArrayList;
import java.util.Collections;
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


/**
 * A server selector that implements priority-based server selection based on pools.
 * This selector works in conjunction with an {@link AdaptiveServerSelector} to provide
 * a two-level selection strategy:
 * <ol>
 *   <li>First level: Select servers based on pool priorities</li>
 *   <li>Second level: Use adaptive selection within the chosen pool</li>
 * </ol>
 *
 * <p>The selector maintains the following invariants:</p>
 * <ul>
 *   <li>Servers from preferred pools are always selected before non-preferred pools</li>
 *   <li>Within each pools, servers are selected using adaptive selection criteria</li>
 *   <li>When no preferred pools are available, falls back to non-preferred pools</li>
 * </ul>
 */
public class PriorityPoolInstanceSelector {

  /** The underlying adaptive server selector used for selection within pools */
  private final AdaptiveServerSelector _adaptiveServerSelector;

  /** Sentinel value used to pool all non-preferred servers together */
  private static final int SENTINEL_POOL_OF_NON_PREFERRED_SERVERS = Integer.MAX_VALUE;

  /**
   * Creates a new priority pool instance selector with the given adaptive server selector.
   *
   * @param adaptiveServerSelector the adaptive server selector to use for selection within pools
   * @throws IllegalArgumentException if adaptiveServerSelector is null
   */
  public PriorityPoolInstanceSelector(AdaptiveServerSelector adaptiveServerSelector) {
    assert adaptiveServerSelector != null;
    _adaptiveServerSelector = adaptiveServerSelector;
  }

  /**
   * Selects a server instance from the given candidates based on pool preferences.
   * The selection process follows these steps:
   * <ol>
   *   <li>pools all candidates by their pool</li>
   *   <li>Iterates through the ordered preferred pools in priority order</li>
   *   <li>For the first pool that has available servers, uses adaptiveServerSelector to choose one</li>
   *   <li>If no preferred pools have servers, falls back to selecting from remaining servers</li>
   * </ol>
   *
   * <p>Example 1 - Preferred pool has servers:</p>
   * <pre>
   *   Candidates:
   *     - server1 (pool 1)
   *     - server2 (pool 2)
   *     - server3 (pool 1)
   *   Preferred pools: [2, 1]
   *   Result: server2 is selected (from pool 2, highest priority)
   * </pre>
   *
   * <p>Example 2 - Fallback to second preferred pool:</p>
   * <pre>
   *   Candidates:
   *     - server1 (pool 1)
   *     - server3 (pool 1)
   *     - server4 (pool 3)
   *   Preferred pools: [2, 1]
   *   Result: adaptiveServerSelector chooses between server1 and server3 (from pool 1)
   * </pre>
   *
   * <p>Example 3 - Fallback to non-preferred pool:</p>
   * <pre>
   *   Candidates:
   *     - server4 (pool 3)
   *     - server5 (pool 3)
   *   Preferred pools: [2, 1]
   *   Result: adaptiveServerSelector chooses between server4 and server5 (from pool 3)
   * </pre>
   *
   * @param ctx the server selection context containing ordered preferred pools
   * @param candidates the list of server candidates to choose from
   * @return the selected server instance as a SegmentInstanceCandidate, or null if no candidates are available
   */
  @Nullable
  public SegmentInstanceCandidate select(ServerSelectionContext ctx,
      @Nullable List<SegmentInstanceCandidate> candidates) {
    assert _adaptiveServerSelector != null;
    if (candidates == null || candidates.isEmpty()) {
      return null;
    }
    // intentional copy to avoid modifying the original list; we will add Integer.MAX_VALUE
    // as a sentinel value to the end of the list to ensure non-preferred servers are processed last
    List<Integer> pools = new ArrayList<>(ctx.getOrderedPreferredPools());
    if (pools.isEmpty()) {
      return choose(candidates);
    }
    Set<Integer> poolSet = new HashSet<>(pools);
    Map<Integer, List<SegmentInstanceCandidate>> poolToServerPos = new HashMap<>();
    // Group servers by their pools. For servers not in preferred pools,
    // use Integer.MAX_VALUE as a sentinel value to ensure they are processed last.
    // This allows us to:
    // 1. Process preferred pools in their specified order
    // 2. Handle all non-preferred servers as a single pool with lowest priority
    // 3. Avoid complex conditional logic for handling non-preferred servers
    for (SegmentInstanceCandidate candidate : candidates) {
      int pool = candidate.getPool();
      pool = poolSet.contains(pool) ? pool : SENTINEL_POOL_OF_NON_PREFERRED_SERVERS;
      poolToServerPos.computeIfAbsent(pool, k -> new ArrayList<>()).add(candidate);
    }
    // Add Integer.MAX_VALUE to the end of preferred pools to ensure non-preferred servers
    // are processed after all preferred pools
    pools.add(SENTINEL_POOL_OF_NON_PREFERRED_SERVERS);
    for (int pool : pools) {
      List<SegmentInstanceCandidate> instancesInPool = poolToServerPos.get(pool);
      if (instancesInPool != null) {
        return choose(instancesInPool);
      }
    }
    assert false;
    return null;
  }

  /**
   * Invoke adaptiveServerSelector to get the original ranking the servers (min first). Reorder the servers based on
   * the pool preference. The head of the OrderedPreferredPools list is the most preferred pool.
   * The servers in the same pool are ranked by the original ranking.
   *
   * <p>Example:</p>
   * <pre>
   * Given:
   *   - Server candidates:
   *     - server1 (pool 1, score 80)
   *     - server2 (pool 2, score 70)
   *     - server3 (pool 1, score 90)
   *     - server4 (pool 3, score 60)
   *   - Ordered preferred pools: [2, 1]
   *
   * Original ranking by score would be: [server4, server2, server1, server3]
   * Final ranking after pool preference: [server2, server1, server3, server4]
   * Because:
   *   1. pool 2 servers come first (server2)
   *   2. pool 1 servers come next, maintaining their relative order (server1, server3)
   *   3. Remaining servers come last (server4)
   * </pre>
   *
   * @param ctx the server selection context containing ordered preferred pools
   * @param serverCandidates the server candidates to be ranked
   * @return the ranked servers, ordered by pool preference and then by original ranking within each pool
   */
  public List<String> rank(ServerSelectionContext ctx, List<SegmentInstanceCandidate> serverCandidates) {
    if (serverCandidates == null || serverCandidates.isEmpty()) {
      return Collections.emptyList();
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

  private SegmentInstanceCandidate choose(List<SegmentInstanceCandidate> candidates) {
    // TODO: Optimize this by passing the list of candidates to the adaptiveServerSelector
    List<String> candidateInstances = new ArrayList<>(candidates.size());
    for (SegmentInstanceCandidate candidate : candidates) {
      candidateInstances.add(candidate.getInstance());
    }
    String selectedInstance = _adaptiveServerSelector.select(candidateInstances);
    return candidates.get(candidateInstances.indexOf(selectedInstance));
  }
}
