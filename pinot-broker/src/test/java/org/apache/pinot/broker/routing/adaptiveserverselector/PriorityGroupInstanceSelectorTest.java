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
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.broker.routing.instanceselector.SegmentInstanceCandidate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class PriorityGroupInstanceSelectorTest {

  private AdaptiveServerSelector _adaptiveServerSelector;
  private PriorityGroupInstanceSelector _selector;
  private ServerSelectionContext _context;

  @BeforeMethod
  public void setUp() {
    _adaptiveServerSelector = mock(AdaptiveServerSelector.class);
    _selector = new PriorityGroupInstanceSelector(_adaptiveServerSelector);
    _context = mock(ServerSelectionContext.class);
  }

  @Test
  public void testSelectWithEmptyCandidates() {
    SegmentInstanceCandidate result = _selector.select(_context, Collections.emptyList());
    assertNull(result);
  }

  @Test
  public void testSelectWithNullCandidates() {
    SegmentInstanceCandidate result = _selector.select(_context, null);
    assertNull(result);
  }

  @Test
  public void testSelectWithNoPreferredGroups() {
    // Setup
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server1", 1),
        createCandidate("server2", 2),
        createCandidate("server3", 3)
    );
    when(_context.getOrderedPreferredGroups()).thenReturn(Collections.emptyList());
    when(_adaptiveServerSelector.select(any())).thenReturn("server2");

    // Execute
    SegmentInstanceCandidate result = _selector.select(_context, candidates);

    // Verify
    assertNotNull(result);
    assertEquals(result.getInstance(), "server2");
  }

  @Test
  public void testSelectWithPreferredGroupHasServers() {
    // Setup - Example 1 from Javadoc
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server1", 1),
        createCandidate("server2", 2),
        createCandidate("server3", 1)
    );
    when(_context.getOrderedPreferredGroups()).thenReturn(Arrays.asList(2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server2");

    // Execute
    SegmentInstanceCandidate result = _selector.select(_context, candidates);

    // Verify
    assertNotNull(result);
    assertEquals(result.getInstance(), "server2");
    assertEquals(result.getReplicaGroup(), 2);
  }

  @Test
  public void testSelectWithFallbackToSecondPreferredGroup() {
    // Setup - Example 2 from Javadoc
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server1", 1),
        createCandidate("server3", 1),
        createCandidate("server4", 3)
    );
    when(_context.getOrderedPreferredGroups()).thenReturn(Arrays.asList(2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server1");

    // Execute
    SegmentInstanceCandidate result = _selector.select(_context, candidates);

    // Verify
    assertNotNull(result);
    assertEquals(result.getInstance(), "server1");
    assertEquals(result.getReplicaGroup(), 1);
  }

  @Test
  public void testSelectWithFallbackToNonPreferredGroup() {
    // Setup - Example 3 from Javadoc
    List<SegmentInstanceCandidate> candidates = Arrays.asList(
        createCandidate("server4", -1),
        createCandidate("server5", -1)
    );
    when(_context.getOrderedPreferredGroups()).thenReturn(Arrays.asList(2, 1));
    when(_adaptiveServerSelector.select(any())).thenReturn("server4");

    // Execute
    SegmentInstanceCandidate result = _selector.select(_context, candidates);

    // Verify
    assertNotNull(result);
    assertEquals(result.getInstance(), "server4");
    assertEquals(result.getReplicaGroup(), -1);
  }

  @Test
  public void testRankWithEmptyCandidates() {
    List<String> result = _selector.rank(_context, Collections.emptyList());
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
    when(_context.getOrderedPreferredGroups()).thenReturn(Collections.emptyList());
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
    when(_context.getOrderedPreferredGroups()).thenReturn(Arrays.asList(2, 1));
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
    when(candidate.getReplicaGroup()).thenReturn(replicaGroup);
    return candidate;
  }
}
