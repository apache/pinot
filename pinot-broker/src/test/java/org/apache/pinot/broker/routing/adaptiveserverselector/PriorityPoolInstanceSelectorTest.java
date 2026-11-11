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

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.broker.routing.instanceselector.SegmentInstanceCandidate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class PriorityPoolInstanceSelectorTest {

  private AdaptiveServerSelector _adaptiveServerSelector;
  private PriorityPoolInstanceSelector _selector;
  private ServerSelectionContext _context;

  @BeforeMethod
  public void setUp() {
    _adaptiveServerSelector = mock(AdaptiveServerSelector.class);
    _selector = new PriorityPoolInstanceSelector(_adaptiveServerSelector);
    _context = mock(ServerSelectionContext.class);
  }

  @Test
  public void testSelectWithNoPreferredGroups() {
    // Setup
    List<SegmentInstanceCandidate> candidates = List.of(
        createCandidate("server1", 1),
        createCandidate("server2", 2),
        createCandidate("server3", 3)
    );
    when(_context.getOrderedPreferredPools()).thenReturn(List.of());
    when(_adaptiveServerSelector.select(any())).thenReturn("server2");

    // Execute
    int index = _selector.forQuery(_context).select(candidates);

    // Verify
    verify(_adaptiveServerSelector).select(List.of("server1", "server2", "server3"));
    assertEquals(index, 1);
  }

  @Test
  public void testSelectWithPreferredGroupHasServers() {
    // Setup - Example 1 from Javadoc
    List<SegmentInstanceCandidate> candidates = List.of(
        createCandidate("server1", 1),
        createCandidate("server2", 2),
        createCandidate("server3", 1)
    );
    when(_context.getOrderedPreferredPools()).thenReturn(List.of(2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server2");

    // Execute
    int index = _selector.forQuery(_context).select(candidates);

    // Verify
    verify(_adaptiveServerSelector).select(List.of("server2"));
    assertEquals(index, 1);
  }

  @Test
  public void testSelectWithFallbackToSecondPreferredGroup() {
    // Setup - Example 2 from Javadoc
    List<SegmentInstanceCandidate> candidates = List.of(
        createCandidate("server1", 1),
        createCandidate("server3", 1),
        createCandidate("server4", 3)
    );
    when(_context.getOrderedPreferredPools()).thenReturn(List.of(2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server1");

    // Execute
    int index = _selector.forQuery(_context).select(candidates);

    // Verify
    verify(_adaptiveServerSelector).select(List.of("server1", "server3"));
    assertEquals(index, 0);
  }

  @Test
  public void testSelectWithFallbackToNonPreferredGroup() {
    // Setup - Example 3 from Javadoc
    List<SegmentInstanceCandidate> candidates = List.of(
        createCandidate("server4", -1),
        createCandidate("server5", -1)
    );
    when(_context.getOrderedPreferredPools()).thenReturn(List.of(2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server4");

    // Execute
    int index = _selector.forQuery(_context).select(candidates);

    // Verify
    verify(_adaptiveServerSelector).select(List.of("server4", "server5"));
    assertEquals(index, 0);
  }

  @Test
  public void testSelectWithRepeatedPreferredPool() {
    // Setup - Pool 1 is listed twice and keeps the priority of its first position
    List<SegmentInstanceCandidate> candidates = List.of(
        createCandidate("server1", 1),
        createCandidate("server2", 2),
        createCandidate("server3", 1)
    );
    when(_context.getOrderedPreferredPools()).thenReturn(List.of(1, 2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server3");

    // Execute
    int index = _selector.forQuery(_context).select(candidates);

    // Verify
    verify(_adaptiveServerSelector).select(List.of("server1", "server3"));
    assertEquals(index, 2);
  }

  @Test
  public void testSelectWithMaxValuePreferredPool() {
    // Setup - The servers not in a preferred pool share the priority of preferred pool `Integer.MAX_VALUE`
    List<SegmentInstanceCandidate> candidates = List.of(
        createCandidate("server1", 1),
        createCandidate("server2", Integer.MAX_VALUE),
        createCandidate("server3", 3)
    );
    when(_context.getOrderedPreferredPools()).thenReturn(List.of(Integer.MAX_VALUE, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server3");

    // Execute
    int index = _selector.forQuery(_context).select(candidates);

    // Verify
    verify(_adaptiveServerSelector).select(List.of("server2", "server3"));
    assertEquals(index, 2);
  }

  @Test
  public void testSelectWithNoCandidateSelected() {
    // Setup
    List<SegmentInstanceCandidate> candidates = List.of(
        createCandidate("server1", 1),
        createCandidate("server2", 2)
    );
    when(_context.getOrderedPreferredPools()).thenReturn(List.of());
    when(_adaptiveServerSelector.select(any())).thenReturn(null);

    // Execute and verify
    assertThrows(IllegalStateException.class, () -> _selector.forQuery(_context).select(candidates));
  }

  @Test
  public void testRankWithEmptyCandidates() {
    List<String> result = _selector.rank(_context, List.of());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testRankWithNoPreferredGroups() {
    // Setup
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server2", 2),
        createCandidate("server1", 1),
        createCandidate("server3", 1),
        createCandidate("server4", -1)
    );
    when(_context.getOrderedPreferredPools()).thenReturn(List.of());
    when(_adaptiveServerSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        Pair.of("server1", 0.1),
        Pair.of("server2", 0.2),
        Pair.of("server4", 0.3),
        Pair.of("server3", 0.4)
    ));

    // Execute
    List<String> result = _selector.rank(_context, candidates);

    // Verify
    assertEquals(result, Arrays.asList("server1", "server2", "server4", "server3"));
  }

  @Test
  public void testRankWithPreferredGroups() {
    // Setup
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server2", 2),
        createCandidate("server1", 1),
        createCandidate("server3", 1),
        createCandidate("server4", -1)
    );
    when(_context.getOrderedPreferredPools()).thenReturn(Arrays.asList(2, 1));
    when(_adaptiveServerSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        Pair.of("server1", 0.1),
        Pair.of("server2", 0.2),
        Pair.of("server4", 0.3),
        Pair.of("server3", 0.4)
    ));

    // Execute
    List<String> result = _selector.rank(_context, candidates);

    // Verify
    // Group 2 servers should come first, followed by group 1 servers, then others
    assertEquals(result, Arrays.asList("server2", "server1", "server3", "server4"));
  }

  private SegmentInstanceCandidate createCandidate(String instance, int replicaGroup) {
    SegmentInstanceCandidate candidate = mock(SegmentInstanceCandidate.class);
    when(candidate.getInstance()).thenReturn(instance);
    when(candidate.getPool()).thenReturn(replicaGroup);
    return candidate;
  }
}
