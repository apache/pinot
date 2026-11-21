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
package org.apache.pinot.controller.helix.core.assignment.segment;

import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


/// Balances consumption leaders over constrained replica candidates during an explicit table rebalance.
/// Augmenting paths can exchange intermediate leaders on ties, enabling a transfer from a more loaded Server to a
/// reachable Server with at least two fewer leaders. Intermediate loads do not change; each transfer strictly
/// decreases the sum of squared loads, so the finite assignment space cannot cycle. Already balanced assignments
/// are retained instead of introducing tie-driven leader churn.
///
/// The helper is thread-safe: all mutable state is local to an invocation. Callers provide stable snapshots with
/// non-null keys and values, and each movable partition must already have an eligible leader. Partitions absent
/// from the eligibility map are fixed but still count toward Server load. Neither input is modified.
final class ReplicateModeLeaderBalancer {
  private ReplicateModeLeaderBalancer() {
  }

  static Map<Integer, String> balance(Map<Integer, String> currentLeaders,
      Map<Integer, List<String>> eligibleServers) {
    Map<Integer, String> leaders = new TreeMap<>(currentLeaders);
    Map<String, Integer> counts = new HashMap<>();
    leaders.values().forEach(server -> counts.merge(server, 1, Integer::sum));
    Map<String, Set<Integer>> ownedPartitions = new HashMap<>();
    Map<Integer, List<String>> candidates = new TreeMap<>();
    Set<String> allServers = new TreeSet<>(counts.keySet());
    eligibleServers.forEach((partition, eligible) -> {
      String leader = leaders.get(partition);
      Preconditions.checkState(leader != null && eligible.contains(leader),
          "Partition: %s must start with an eligible consumption leader", partition);
      candidates.put(partition, List.copyOf(new TreeSet<>(eligible)));
      allServers.addAll(eligible);
      ownedPartitions.computeIfAbsent(leader, ignored -> new TreeSet<>()).add(partition);
    });
    List<String> serverOrder = new ArrayList<>(allServers);
    while (true) {
      serverOrder.sort(Comparator.comparingInt((String server) -> counts.getOrDefault(server, 0)).reversed()
          .thenComparing(Comparator.naturalOrder()));
      boolean improved = false;
      for (String source : serverOrder) {
        if (counts.getOrDefault(source, 0) < 2) {
          break;
        }
        List<Move> path = findPath(source, counts, ownedPartitions, candidates);
        if (path.isEmpty()) {
          continue;
        }
        for (Move move : path) {
          leaders.put(move.partition(), move.to());
          ownedPartitions.get(move.from()).remove(move.partition());
          ownedPartitions.computeIfAbsent(move.to(), ignored -> new TreeSet<>()).add(move.partition());
        }
        counts.merge(source, -1, Integer::sum);
        counts.merge(path.getFirst().to(), 1, Integer::sum);
        improved = true;
        break;
      }
      if (!improved) {
        return leaders;
      }
    }
  }

  /// Each breadth-first search visits each reachable Server and its owned movable partitions once. Edges are
  /// partition eligibility entries, avoiding an exponential search over combinations of partition assignments.
  private static List<Move> findPath(String source, Map<String, Integer> counts,
      Map<String, Set<Integer>> ownedPartitions, Map<Integer, List<String>> candidates) {
    Queue<String> pending = new ArrayDeque<>();
    Set<String> visited = new HashSet<>();
    Map<String, Move> previous = new HashMap<>();
    pending.add(source);
    visited.add(source);
    int sourceLoad = counts.get(source);
    while (!pending.isEmpty()) {
      String owner = pending.remove();
      for (int partition : ownedPartitions.getOrDefault(owner, Set.of())) {
        for (String candidate : candidates.get(partition)) {
          if (!visited.add(candidate)) {
            continue;
          }
          previous.put(candidate, new Move(partition, owner, candidate));
          if (sourceLoad - counts.getOrDefault(candidate, 0) >= 2) {
            List<Move> path = new ArrayList<>();
            String cursor = candidate;
            while (!cursor.equals(source)) {
              Move move = previous.get(cursor);
              path.add(move);
              cursor = move.from();
            }
            return path;
          }
          pending.add(candidate);
        }
      }
    }
    return List.of();
  }

  private record Move(int partition, String from, String to) {
  }
}
