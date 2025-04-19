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


public class PriorityGroupInstanceSelector {

  private final AdaptiveServerSelector _adaptiveServerSelector;

  private static final int SENTINEL_GROUP_OF_NON_PREFERRED_SERVERS = Integer.MAX_VALUE;

  public PriorityGroupInstanceSelector(AdaptiveServerSelector adaptiveServerSelector) {
    assert adaptiveServerSelector != null;
    _adaptiveServerSelector = adaptiveServerSelector;
  }

  /**
   * Selects a server instance from the given candidates based on replica group preferences.
   * The selection process follows these steps:
   * <ol>
   *   <li>Groups all candidates by their replica group</li>
   *   <li>Iterates through the ordered preferred groups in priority order</li>
   *   <li>For the first group that has available servers, uses adaptiveServerSelector to choose one</li>
   *   <li>If no preferred groups have servers, falls back to selecting from remaining servers</li>
   * </ol>
   *
   * <p>Example 1 - Preferred group has servers:</p>
   * <pre>
   *   Candidates:
   *     - server1 (replica group 1)
   *     - server2 (replica group 2)
   *     - server3 (replica group 1)
   *   Preferred groups: [2, 1]
   *   Result: server2 is selected (from group 2, highest priority)
   * </pre>
   *
   * <p>Example 2 - Fallback to second preferred group:</p>
   * <pre>
   *   Candidates:
   *     - server1 (replica group 1)
   *     - server3 (replica group 1)
   *     - server4 (replica group 3)
   *   Preferred groups: [2, 1]
   *   Result: adaptiveServerSelector chooses between server1 and server3 (from group 1)
   * </pre>
   *
   * <p>Example 3 - Fallback to non-preferred group:</p>
   * <pre>
   *   Candidates:
   *     - server4 (replica group 3)
   *     - server5 (replica group 3)
   *   Preferred groups: [2, 1]
   *   Result: adaptiveServerSelector chooses between server4 and server5 (from group 3)
   * </pre>
   *
   * @param ctx the server selection context containing ordered preferred groups
   * @param candidates the list of server candidates to choose from
   * @return the selected server instance as a SegmentInstanceCandidate, or null if no candidates are available
   */
  public SegmentInstanceCandidate select(ServerSelectionContext ctx,
      @Nullable List<SegmentInstanceCandidate> candidates) {
    assert _adaptiveServerSelector != null;
    if (candidates == null || candidates.isEmpty()) {
      return null;
    }
    // intentional copy to avoid modifying the original list; we will add Integer.MAX_VALUE
    // as a sentinel value to the end of the list to ensure non-preferred servers are processed last
    List<Integer> groups = new ArrayList<>(ctx.getOrderedPreferredGroups());
    if (groups.isEmpty()) {
      return choose(candidates);
    }
    Set<Integer> groupSet = new HashSet<>(groups);
    Map<Integer, List<Integer>> groupToServerPos = new HashMap<>();
    // Group servers by their replica groups. For servers not in preferred groups,
    // use Integer.MAX_VALUE as a sentinel value to ensure they are processed last.
    // This allows us to:
    // 1. Process preferred groups in their specified order
    // 2. Handle all non-preferred servers as a single group with lowest priority
    // 3. Avoid complex conditional logic for handling non-preferred servers
    for (int i = 0; i < candidates.size(); i++) {
      int group = candidates.get(i).getReplicaGroup();
      group = groupSet.contains(group) ? group : SENTINEL_GROUP_OF_NON_PREFERRED_SERVERS;
      groupToServerPos.computeIfAbsent(group, k -> new ArrayList<>()).add(i);
    }
    // Add Integer.MAX_VALUE to the end of preferred groups to ensure non-preferred servers
    // are processed after all preferred groups
    groups.add(SENTINEL_GROUP_OF_NON_PREFERRED_SERVERS);
    for (int group : groups) {
      List<Integer> instancesInGroup = groupToServerPos.get(group);
      if (instancesInGroup != null) {
        return choose(instancesInGroup.stream().map(candidates::get).collect(Collectors.toList()));
      }
    }
    assert false;
    return null;
  }

  /**
   * Invoke adaptiveServerSelector to get the original ranking the servers (min first). Reorder the servers based on
   * the replica group preference. The head of the OrderedPreferredGroups list is the most preferred group.
   * The servers in the same group are ranked by the original ranking.
   *
   * <p>Example:</p>
   * <pre>
   * Given:
   *   - Server candidates:
   *     - server1 (group 1, score 80)
   *     - server2 (group 2, score 70)
   *     - server3 (group 1, score 90)
   *     - server4 (group 3, score 60)
   *   - Ordered preferred groups: [2, 1]
   *
   * Original ranking by score would be: [server4, server2, server1, server3]
   * Final ranking after group preference: [server2, server1, server3, server4]
   * Because:
   *   1. Group 2 servers come first (server2)
   *   2. Group 1 servers come next, maintaining their relative order (server1, server3)
   *   3. Remaining servers come last (server4)
   * </pre>
   *
   * @param ctx the server selection context containing ordered preferred groups
   * @param serverCandidates the server candidates to be ranked
   * @return the ranked servers, ordered by group preference and then by original ranking within each group
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
    List<Integer> groups = new ArrayList<>(ctx.getOrderedPreferredGroups());
    if (groups.isEmpty()) {
      return serverRankListWithScores.stream().map(Pair::getLeft).collect(Collectors.toList());
    }
    Map<String, SegmentInstanceCandidate> idToCandidate = serverCandidates.stream()
        .map(candidate -> new ImmutablePair<>(candidate.getInstance(), candidate))
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    // Create a set of preferred groups for efficient lookup
    Set<Integer> preferredGroups = new HashSet<>(groups);
    Map<Integer, List<String>> groupToRankedServers = new HashMap<>();
    for (Pair<String, Double> entry : serverRankListWithScores) {
      int group = idToCandidate.get(entry.getLeft()).getReplicaGroup();
      // If the group is not in the preferred groups list, assign it the sentinel group
      group = preferredGroups.contains(group) ? group : SENTINEL_GROUP_OF_NON_PREFERRED_SERVERS;
      groupToRankedServers.computeIfAbsent(group, k -> new ArrayList<>()).add(entry.getLeft());
    }

    // Add the sentinel group to the end of the groups list to ensure its group members are included in the tail
    groups.add(SENTINEL_GROUP_OF_NON_PREFERRED_SERVERS);

    // Build the final ranked list by processing groups in order
    List<String> rankedServers = new ArrayList<>();
    for (int group : groups) {
      List<String> instancesInGroup = groupToRankedServers.get(group);
      if (instancesInGroup != null) {
        rankedServers.addAll(instancesInGroup);
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
