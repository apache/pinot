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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class SegmentGroupsBuilderHashStrategyTest {

  @Test
  public void testEqualSetsWithDifferentIterationOrders() {
    Set<String> first = new LinkedHashSet<>(List.of("server1", "server2", "server3"));
    Set<String> reordered = new LinkedHashSet<>(List.of("server3", "server1", "server2"));

    assertEquals(first, reordered);
    assertEquals(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.hashCode(first),
        SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.hashCode(reordered));

    Map<Set<String>, String> map =
        new Object2ObjectOpenCustomHashMap<>(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY);

    map.put(first, "unavailable");
    assertEquals(map.get(reordered), "unavailable");
  }

  @Test
  public void testReducedCollisionsForDifferentInstanceSets() {
    Set<String> first = Set.of("a", "d");
    Set<String> second = Set.of("b", "c");

    assertEquals(first.hashCode(), second.hashCode());
    assertNotEquals(first, second);

    assertNotEquals(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.hashCode(first),
        SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.hashCode(second));

    Map<Set<String>, String> map =
        new Object2ObjectOpenCustomHashMap<>(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY);

    map.put(first, "first");
    map.put(second, "second");

    assertEquals(map.size(), 2);
    assertEquals(map.get(first), "first");
    assertEquals(map.get(second), "second");
  }

  @Test
  public void testStrategyNullAndSetEqualityContract() {
    Set<String> first = Set.of("server1", "server2");
    Set<String> reordered = new LinkedHashSet<>(List.of("server2", "server1"));

    assertEquals(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.hashCode(null), 0);
    assertTrue(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.equals(null, null));
    assertFalse(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.equals(first, null));
    assertFalse(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.equals(null, first));
    assertTrue(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.equals(first, reordered));
    assertFalse(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.equals(first, Set.of("server1", "server3")));
  }

  @Test
  public void testHashDistributionForNumericServerNames() {
    List<String> servers = new ArrayList<>(200);
    for (int i = 0; i < 200; i++) {
      servers.add("Server_pinot-server-" + i + ".pinot-server-headless.ns.svc.cluster.local_8098");
    }

    Set<Integer> originalHashes = new HashSet<>();
    Set<Integer> strategyHashes = new HashSet<>();
    int groups = 0;

    outer:
    for (int i = 0; i < servers.size() - 2; i++) {
      for (int j = i + 1; j < servers.size() - 1; j++) {
        for (int k = j + 1; k < servers.size(); k++) {
          Set<String> instances = Set.of(servers.get(i), servers.get(j), servers.get(k));
          originalHashes.add(instances.hashCode());
          strategyHashes.add(SegmentGroupsBuilder.INSTANCE_SET_STRATEGY.hashCode(instances));
          if (++groups == 100_000) {
            break outer;
          }
        }
      }
    }

    assertEquals(groups, 100_000);
    assertTrue(originalHashes.size() < 5_000, "Unexpected legacy collision distribution");
    assertTrue(strategyHashes.size() >= 99_000, "Insufficient hash diversity: " + strategyHashes.size());
  }
}
