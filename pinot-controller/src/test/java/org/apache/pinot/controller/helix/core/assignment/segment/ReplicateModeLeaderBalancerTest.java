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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Checks constrained leader balancing against exhaustive feasible assignments, including fixed Server loads.
public class ReplicateModeLeaderBalancerTest {
  @DataProvider
  public Object[][] candidateGraphs() {
    return new Object[][]{
        {Map.of(0, List.of("A", "B"), 1, List.of("C", "D"), 2, List.of("E", "A"),
            3, List.of("B", "C"), 4, List.of("D", "E")), Map.of()},
        {Map.of(0, List.of("A"), 1, List.of("A", "B"), 2, List.of("B", "C"),
            3, List.of("A", "C"), 4, List.of("C")), Map.of(100, "A", 101, "A", 102, "D")},
        {Map.of(0, List.of("A", "B"), 1, List.of("A", "B"),
            2, List.of("C", "D"), 3, List.of("C", "D")), Map.of(100, "A", 101, "A")},
        {Map.of(0, List.of("A", "B"), 1, List.of("B", "C"), 2, List.of("C", "D"),
            3, List.of("D"), 4, List.of("A")), Map.of(100, "D", 101, "D")},
        {Map.of(0, List.of("A", "B", "C"), 1, List.of("A", "B", "C"),
            2, List.of("A", "B", "C"), 3, List.of("A", "B", "C")), Map.of(100, "A")},
        {Map.of(0, List.of("A", "B")), Map.of(100, "A", 101, "A")},
        {Map.of(), Map.of()}
    };
  }

  @Test(dataProvider = "candidateGraphs")
  public void testBalanceMatchesExhaustiveOptimum(Map<Integer, List<String>> candidates,
      Map<Integer, String> fixedLeaders) {
    List<Map<Integer, String>> feasible = new ArrayList<>();
    enumerate(candidates.keySet().stream().sorted().toList(), candidates, 0,
        new TreeMap<>(fixedLeaders), feasible);
    int optimum = feasible.stream().mapToInt(ReplicateModeLeaderBalancerTest::squaredLoad).min().orElseThrow();
    for (Map<Integer, String> original : feasible) {
      Map<Integer, String> before = Map.copyOf(original);

      Map<Integer, String> balanced = ReplicateModeLeaderBalancer.balance(original, candidates);

      assertEquals(squaredLoad(balanced), optimum, "Non-optimal balance from " + original);
      assertEquals(balanced.size(), original.size());
      fixedLeaders.forEach((partition, leader) -> assertEquals(balanced.get(partition), leader));
      candidates.forEach((partition, eligible) -> assertTrue(eligible.contains(balanced.get(partition))));
      assertEquals(original, before, "Balancing must not mutate its input snapshot");
      if (squaredLoad(original) == optimum) {
        assertEquals(balanced, original, "An optimal assignment must not churn its leaders");
      }
      assertEquals(ReplicateModeLeaderBalancer.balance(balanced, candidates), balanced,
          "Repeated planning must retain the balanced assignment");
    }
  }

  private static void enumerate(List<Integer> partitions, Map<Integer, List<String>> candidates, int index,
      Map<Integer, String> current, List<Map<Integer, String>> assignments) {
    if (index == partitions.size()) {
      assignments.add(Map.copyOf(current));
      return;
    }
    int partition = partitions.get(index);
    for (String server : candidates.get(partition)) {
      current.put(partition, server);
      enumerate(partitions, candidates, index + 1, current, assignments);
    }
    current.remove(partition);
  }

  private static int squaredLoad(Map<Integer, String> leaders) {
    Map<String, Integer> counts = new HashMap<>();
    leaders.values().forEach(server -> counts.merge(server, 1, Integer::sum));
    return counts.values().stream().mapToInt(count -> count * count).sum();
  }
}
