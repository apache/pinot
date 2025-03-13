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

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.broker.routing.instanceselector.SegmentInstanceCandidate;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;


/**
 * The {@code AdaptiveServerSelector} intelligently selects the best available server for a segment during query
 * processing. The decision is made based on stats recorded for each server during query processing.
 */
public interface AdaptiveServerSelector {

  /**
   * Picks the best server to route a query from the list of candidate servers.
   *
   * @param serverCandidates Candidate servers from which the best server should be chosen.
   * @return server identifier
   */
  String select(List<String> serverCandidates);

  /**
   * Returns the ranking of servers ordered from best to worst along with the absolute scores based on which the
   * servers are ranked. Based on the implementation of the interface, the score could refer to different things. For
   * NumInFlightReqSelector, score is the number of inflight requests. For LatencySelector, score is the EMA latency.
   * For HybridSelector, score is the hybridScore which is computed by combining latency and # inflight requests.
   *
   * @return List of servers along with their values ranked from best to worst.
   */
  List<Pair<String, Double>> fetchAllServerRankingsWithScores();

  /**
   * Same as above but fetches ranking only for the list of serverCandidates provided in the parameter. If a server
   * doesn't have an entry, it's ranked better than other serverCandidates and a value of -1.0 is returned. With the
   * above "fetchAllServerRankingsWithScores" API, ranking for all the servers are fetched. This can become
   * problematic if a broker is routing to multiple  server tenants but a query needs to touch only a single server
   * tenant. This API helps fetch ranking only for a subset of servers.
   * @return List of servers along with their values ranked from best to worst.
   */
  List<Pair<String, Double>> fetchServerRankingsWithScores(List<String> serverCandidates);

  Optional<SegmentInstanceCandidate> choose(List<SegmentInstanceCandidate> serverCandidates, Map<String, String> queryOptions);

  List<String> rank(List<SegmentInstanceCandidate> serverCandidates, Map<String, String> queryOptions);

  static SegmentInstanceCandidate selectSevers(List<SegmentInstanceCandidate> servers, Function<String, Double> scoreFunc) {
    double minScore = Double.MAX_VALUE;
    SegmentInstanceCandidate selectedServer = null;
    // TODO: If two or more servers have the same score, break the tie intelligently.
    for (SegmentInstanceCandidate server : servers) {
      Double score = scoreFunc.apply(server.getInstance());
      // No stats for this server. That means this server hasn't received any queries yet.
      if (score == null) {
        int randIdx = ThreadLocalRandom.current().nextInt(servers.size());
        return servers.get(randIdx);
      }
      if (score < minScore) {
        minScore = score;
        selectedServer = server;
      }
    }
    return selectedServer;
  }

  static Optional<SegmentInstanceCandidate> select(List<SegmentInstanceCandidate> candidates, Function<String, Double> scoreFunc, Map<String, String> queryOptions) {
    if (candidates == null || candidates.isEmpty()) {
      return Optional.empty();
    }
    List<Integer> groups = QueryOptionsUtils.getOrderedPreferredReplicas(queryOptions);
    if (groups.isEmpty()) {
        return Optional.of(selectSevers(candidates,scoreFunc));
    }
    Set<Integer> groupSet = new HashSet<>(groups);
    Map<Integer, List<Integer>> groupToRankedServers = new HashMap<>();
    for (int i = 0; i < candidates.size(); i++) {
      int group = candidates.get(i).getReplicaGroup();
      group = groupSet.contains(group) ? group : Integer.MAX_VALUE;
      groupToRankedServers.computeIfAbsent(group, k -> new ArrayList<>()).add(i);
    }
    groups.add(Integer.MAX_VALUE);
    for(int group : groups) {
      List<Integer> instancesInGroup = groupToRankedServers.get(group);
      if(instancesInGroup != null) {
        return Optional.of(selectSevers(
                instancesInGroup.stream().map(candidates::get).collect(Collectors.toList()),
                scoreFunc));
      }
    }
    assert false;
    return Optional.empty();
  }

  static List<String> rankServers(List<SegmentInstanceCandidate> serverCandidates, Function<String, Double> scoreFunc, Map<String, String> queryOptions) {
    if (serverCandidates == null || serverCandidates.isEmpty()) {
          return Collections.emptyList();
    }
    List<Pair<Integer, Double>> serverRankListWithScores = new ArrayList<>();
    int idx = 0;
    for (SegmentInstanceCandidate candidate: serverCandidates) {
      Double score = Optional.ofNullable(scoreFunc.apply(candidate.getInstance())).orElse(-1.0);
      serverRankListWithScores.add(new ImmutablePair<>(idx, score));
      idx++;
    }
    // Let's shuffle the list before sorting. This helps with randomly choosing different servers if there is a tie.
    Collections.shuffle(serverRankListWithScores);
    serverRankListWithScores.sort(Comparator.comparing(Pair::getRight));

    List<Integer> groups = QueryOptionsUtils.getOrderedPreferredReplicas(queryOptions);
    List<String> rankedServers = new ArrayList<>();
    if (groups.isEmpty()) {
      for (Pair<Integer, Double> entry : serverRankListWithScores) {
        rankedServers.add(serverCandidates.get(entry.getLeft()).getInstance());
      }
      return rankedServers;
    }
    Set<Integer> groupSet = new HashSet<>(groups);
    Map<Integer, List<Integer>> groupToRankedServers = new HashMap<>();
    for (int i = 0; i < serverRankListWithScores.size(); i++) {
      int pos = serverRankListWithScores.get(i).getLeft();
      SegmentInstanceCandidate candidate = serverCandidates.get(pos);
      int group = candidate.getReplicaGroup();
      group = groupSet.contains(group) ? group : Integer.MAX_VALUE;
      groupToRankedServers.computeIfAbsent(group, k -> new ArrayList<>()).add(pos);
    }
    groups.add(Integer.MAX_VALUE);
    for(int group : groups) {
      List<Integer> instancesInGroup = groupToRankedServers.get(group);
      if(instancesInGroup != null) {
        rankedServers.addAll(instancesInGroup.stream().map(pos ->
                serverCandidates.get(pos).getInstance()).collect(Collectors.toList()));
      }
    }
    return rankedServers;
  }
}
