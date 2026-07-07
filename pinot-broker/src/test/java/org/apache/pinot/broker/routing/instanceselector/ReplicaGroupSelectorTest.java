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

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.HybridSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.config.table.RoutingConfig.REPLICA_GROUP_INSTANCE_SELECTOR_TYPE;
import static org.apache.pinot.spi.config.table.RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link ReplicaGroupInstanceSelector} and {@link StrictReplicaGroupInstanceSelector},
 * including adaptive server selection (pool-level routing for strict replica groups).
 */
@SuppressWarnings("unchecked")
public class ReplicaGroupSelectorTest {
  private AutoCloseable _mocks;

  @Mock
  private TableConfig _tableConfig;

  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final Map<String, ServerInstance> EMPTY_SERVER_MAP = Collections.EMPTY_MAP;
  private static final InstanceSelectorConfig INSTANCE_SELECTOR_CONFIG = new InstanceSelectorConfig(false, 300, false);
  private static final List<String> SEGMENTS =
      Arrays.asList("segment0", "segment1", "segment2", "segment3", "segment4", "segment5", "segment6", "segment7",
          "segment8", "segment9", "segment10", "segment11");

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_tableConfig.getTableName()).thenReturn(TABLE_NAME);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  // --- Shared helpers ---

  static IdealState createIdealState(Map<String, List<Pair<String, String>>> segmentState) {
    IdealState idealState = new IdealState(TABLE_NAME);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    for (Map.Entry<String, List<Pair<String, String>>> entry : segmentState.entrySet()) {
      Map<String, String> instanceStateMap = new TreeMap<>();
      for (Pair<String, String> instanceState : entry.getValue()) {
        instanceStateMap.put(instanceState.getLeft(), instanceState.getRight());
      }
      idealStateSegmentAssignment.put(entry.getKey(), instanceStateMap);
    }
    return idealState;
  }

  static ExternalView createExternalView(Map<String, List<Pair<String, String>>> segmentState) {
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    for (Map.Entry<String, List<Pair<String, String>>> entry : segmentState.entrySet()) {
      Map<String, String> instanceStateMap = new TreeMap<>();
      for (Pair<String, String> instanceState : entry.getValue()) {
        instanceStateMap.put(instanceState.getLeft(), instanceState.getRight());
      }
      externalViewSegmentAssignment.put(entry.getKey(), instanceStateMap);
    }
    return externalView;
  }

  // --- ReplicaGroupInstanceSelector: numReplicaGroupsToQuery tests ---

  @Test
  public void testReplicaGroupInstanceSelectorNumReplicaGroupsToQuery() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    // numReplicas = 3, fanning the query to 2 replica groups
    queryOptions.put("numReplicaGroupsToQuery", "2");
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector = new ReplicaGroupInstanceSelector();

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    // 12 online segments with each segment having all 3 instances as online
    // replicas are 3
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    enabledInstances.add(instance0);
    enabledInstances.add(instance1);
    enabledInstances.add(instance2);

    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();

    for (String instance : enabledInstances) {
      idealStateInstanceStateMap0.put(instance, ONLINE);
      externalViewInstanceStateMap0.put(instance, ONLINE);
    }

    List<String> segments = SEGMENTS;
    // add all segments to both idealStateSegmentAssignment and externalViewSegmentAssignment maps and also to online
    // segments
    for (String segment : segments) {
      idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
      externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      onlineSegments.add(segment);
    }

    replicaGroupInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);

    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(0), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(1), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(2), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(3), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(4), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(5), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(6), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(7), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(8), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(9), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(10), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(11), instance1);
    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testReplicaGroupInstanceSelectorNumReplicaGroupsToQueryGreaterThanReplicas() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("numReplicaGroupsToQuery", "4");

    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector = new ReplicaGroupInstanceSelector();

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    // 12 online segments with each segment having all 3 instances as online
    // replicas are 3
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    enabledInstances.add(instance0);
    enabledInstances.add(instance1);
    enabledInstances.add(instance2);

    List<String> segments = SEGMENTS;

    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();

    for (String instance : enabledInstances) {
      idealStateInstanceStateMap0.put(instance, ONLINE);
      externalViewInstanceStateMap0.put(instance, ONLINE);
    }

    // add all segments to both idealStateSegmentAssignment and externalViewSegmentAssignment maps and also to online
    // segments
    for (String segment : segments) {
      idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
      externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      onlineSegments.add(segment);
    }

    replicaGroupInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);

    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(0), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(1), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(2), instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(3), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(4), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(5), instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(6), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(7), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(8), instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(9), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(10), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(11), instance2);
    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testReplicaGroupInstanceSelectorNumReplicaGroupsNotSet() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();

    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector = new ReplicaGroupInstanceSelector();

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    // 12 online segments with each segment having all 3 instances as online
    // replicas are 3
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    enabledInstances.add(instance0);
    enabledInstances.add(instance1);
    enabledInstances.add(instance2);

    List<String> segments = SEGMENTS;

    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();

    for (String instance : enabledInstances) {
      idealStateInstanceStateMap0.put(instance, ONLINE);
      externalViewInstanceStateMap0.put(instance, ONLINE);
    }

    // add all segments to both idealStateSegmentAssignment and externalViewSegmentAssignment maps and also to online
    // segments
    for (String segment : segments) {
      idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
      externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      onlineSegments.add(segment);
    }

    replicaGroupInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);
    // since numReplicaGroupsToQuery is not set, first query should go to first replica group,
    // 2nd query should go to next replica group

    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    for (String segment : segments) {
      expectedReplicaGroupInstanceSelectorResult.put(segment, instance0);
    }
    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);

    for (String segment : segments) {
      expectedReplicaGroupInstanceSelectorResult.put(segment, instance1);
    }
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
  }

  // --- Adaptive server selection tests ---

  // Shared topology for AR tests: 3 segments across 5 instances in two replica groups.
  // segment2 intentionally has instance4 (unranked) so AR falls back to round-robin for that segment.
  private static final String AR_INSTANCE0 = "instance0";
  private static final String AR_INSTANCE1 = "instance1";
  private static final String AR_INSTANCE2 = "instance2";
  private static final String AR_INSTANCE3 = "instance3";
  private static final String AR_INSTANCE4 = "instance4";
  private static final String AR_SEGMENT0 = "segment0";
  private static final String AR_SEGMENT1 = "segment1";
  private static final String AR_SEGMENT2 = "segment2";
  private static final List<String> AR_SEGMENTS =
      Arrays.asList(AR_SEGMENT0, AR_SEGMENT1, AR_SEGMENT2);
  // Rankings: instance3 best → instance2 → instance1 → instance0 worst; instance4 absent (triggers AR fallback)
  private static final List<Pair<String, Double>> AR_SERVER_RANKS = Arrays.asList(
      new ImmutablePair<>(AR_INSTANCE3, 1.0),
      new ImmutablePair<>(AR_INSTANCE2, 2.0),
      new ImmutablePair<>(AR_INSTANCE1, 3.0),
      new ImmutablePair<>(AR_INSTANCE0, 4.0)
  );

  private SegmentStates buildArSegmentStates() {
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    // segment0 → instance0, instance1
    candidatesMap.put(AR_SEGMENT0, Arrays.asList(
        new SegmentInstanceCandidate(AR_INSTANCE0, true),
        new SegmentInstanceCandidate(AR_INSTANCE1, true)));
    // segment1 → instance2, instance3
    candidatesMap.put(AR_SEGMENT1, Arrays.asList(
        new SegmentInstanceCandidate(AR_INSTANCE2, true),
        new SegmentInstanceCandidate(AR_INSTANCE3, true)));
    // segment2 → instance4 (unranked), instance3
    candidatesMap.put(AR_SEGMENT2, Arrays.asList(
        new SegmentInstanceCandidate(AR_INSTANCE4, true),
        new SegmentInstanceCandidate(AR_INSTANCE3, true)));
    return new SegmentStates(candidatesMap, new HashSet<>(AR_SEGMENTS), null);
  }

  private ReplicaGroupInstanceSelector buildArSelector(String selectorType, HybridSelector hybridSelector) {
    RoutingConfig routingConfig = new RoutingConfig(null, null, selectorType, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setRoutingConfig(routingConfig).build();
    IdealState idealState = createIdealState(Map.of(
        AR_SEGMENT0, List.of(Pair.of(AR_INSTANCE0, ONLINE), Pair.of(AR_INSTANCE1, ONLINE)),
        AR_SEGMENT1, List.of(Pair.of(AR_INSTANCE2, ONLINE), Pair.of(AR_INSTANCE3, ONLINE)),
        AR_SEGMENT2, List.of(Pair.of(AR_INSTANCE3, ONLINE), Pair.of(AR_INSTANCE4, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        AR_SEGMENT0, List.of(Pair.of(AR_INSTANCE0, ONLINE), Pair.of(AR_INSTANCE1, ONLINE)),
        AR_SEGMENT1, List.of(Pair.of(AR_INSTANCE2, ONLINE), Pair.of(AR_INSTANCE3, ONLINE)),
        AR_SEGMENT2, List.of(Pair.of(AR_INSTANCE3, ONLINE), Pair.of(AR_INSTANCE4, ONLINE))));
    return (ReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
        mock(ZkHelixPropertyStore.class), mock(BrokerMetrics.class), hybridSelector,
        mock(PinotConfiguration.class),
        Set.of(AR_INSTANCE0, AR_INSTANCE1, AR_INSTANCE2, AR_INSTANCE3, AR_INSTANCE4),
        Map.of(), idealState, externalView, new HashSet<>(AR_SEGMENTS));
  }

  @Test
  public void testReplicaGroupAdaptiveServerSelector() {
    HybridSelector hybridSelector = mock(HybridSelector.class);
    ReplicaGroupInstanceSelector instanceSelector =
        buildArSelector(REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, hybridSelector);

    assertTrue(instanceSelector instanceof ReplicaGroupInstanceSelector);
    assertFalse(instanceSelector instanceof StrictReplicaGroupInstanceSelector);
    assertNotNull(instanceSelector._adaptiveServerSelector);
    assertNotNull(instanceSelector._priorityPoolInstanceSelector);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(AR_SERVER_RANKS);
    InstanceSelector.InstanceMapping result =
        instanceSelector.select(AR_SEGMENTS, 0, buildArSegmentStates(), null);

    // AR prefers the better-ranked server when all candidates are ranked:
    // segment0: instance1 (rank 3) over instance0 (rank 4)
    // segment1: instance3 (rank 1) over instance2 (rank 2)
    // segment2: instance4 unranked → AR falls back to round-robin → index 0 = instance4
    assertEquals(result.segmentToInstanceMap(), Map.of(
        AR_SEGMENT0, AR_INSTANCE1,
        AR_SEGMENT1, AR_INSTANCE3,
        AR_SEGMENT2, AR_INSTANCE4));
  }

  @Test
  public void testStrictReplicaGroupDoesNotUseAdaptiveServerSelector() {
    HybridSelector hybridSelector = mock(HybridSelector.class);
    ReplicaGroupInstanceSelector instanceSelector =
        buildArSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, hybridSelector);

    assertTrue(instanceSelector instanceof StrictReplicaGroupInstanceSelector);
    assertNull(instanceSelector._adaptiveServerSelector);
    assertNull(instanceSelector._priorityPoolInstanceSelector);

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(AR_SEGMENTS, 0, buildArSegmentStates(), null);

    // hybridSelector never consulted during routing despite being passed to the factory
    verify(hybridSelector, never()).fetchServerRankingsWithScores(any());

    // Round-robin with requestId=0 picks candidate index 0 per segment — opposite of AR result above,
    // which returns {segment0→instance1, segment1→instance3} for the same rankings.
    assertEquals(result.segmentToInstanceMap(), Map.of(
        AR_SEGMENT0, AR_INSTANCE0,
        AR_SEGMENT1, AR_INSTANCE2,
        AR_SEGMENT2, AR_INSTANCE4));
  }
}
