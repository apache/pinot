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
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.config.table.RoutingConfig.REPLICA_GROUP_INSTANCE_SELECTOR_TYPE;
import static org.apache.pinot.spi.config.table.RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
    return buildArSelector(selectorType, hybridSelector, new PinotConfiguration(Collections.emptyMap()));
  }

  private ReplicaGroupInstanceSelector buildArSelector(String selectorType, HybridSelector hybridSelector,
      PinotConfiguration brokerConfig) {
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
        brokerConfig,
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
  public void testStrictReplicaGroupAdaptiveDisabledByFeatureFlag() {
    // when the feature flag is disabled, the factory nulls out the adaptive selector so the
    // selector falls back to round-robin.
    HybridSelector hybridSelector = mock(HybridSelector.class);
    PinotConfiguration brokerConfig = new PinotConfiguration(Map.of(
        CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STRICT_REPLICA_GROUP, "false"));
    ReplicaGroupInstanceSelector instanceSelector =
        buildArSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, hybridSelector, brokerConfig);

    assertTrue(instanceSelector instanceof StrictReplicaGroupInstanceSelector);
    assertNull(instanceSelector._adaptiveServerSelector);
    assertNull(instanceSelector._priorityPoolInstanceSelector);

    BaseInstanceSelector.InstanceMapping result =
        instanceSelector.select(AR_SEGMENTS, 0, buildArSegmentStates(), null);

    // hybridSelector never consulted during routing despite being passed to the factory
    verify(hybridSelector, never()).fetchServerRankingsWithScores(any());

    // Round-robin with requestId=0 picks candidate index 0 per segment
    assertEquals(result.segmentToInstanceMap(), Map.of(
        AR_SEGMENT0, AR_INSTANCE0,
        AR_SEGMENT1, AR_INSTANCE2,
        AR_SEGMENT2, AR_INSTANCE4));
  }

  @Test
  public void testStrictReplicaGroupUsesAdaptiveServerSelector() {
    HybridSelector hybridSelector = mock(HybridSelector.class);
    ReplicaGroupInstanceSelector instanceSelector =
        buildArSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, hybridSelector);

    assertTrue(instanceSelector instanceof StrictReplicaGroupInstanceSelector);
    assertNotNull(instanceSelector._adaptiveServerSelector);
    assertNotNull(instanceSelector._priorityPoolInstanceSelector);
  }

  // --- Strict replica group pool-level adaptive routing tests ---

  // Topology: 2 replica groups (pool 0 and pool 1), 3 segments
  // Pool 0: server_a (segment0, segment1), server_b (segment2)
  // Pool 1: server_c (segment0, segment1), server_d (segment2)
  private static final String SRG_SERVER_A = "server_a";
  private static final String SRG_SERVER_B = "server_b";
  private static final String SRG_SERVER_C = "server_c";
  private static final String SRG_SERVER_D = "server_d";
  private static final String SRG_SEGMENT0 = "seg0";
  private static final String SRG_SEGMENT1 = "seg1";
  private static final String SRG_SEGMENT2 = "seg2";
  private static final List<String> SRG_SEGMENTS = Arrays.asList(SRG_SEGMENT0, SRG_SEGMENT1, SRG_SEGMENT2);

  private SegmentStates buildStrictReplicaGroupSegmentStates() {
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    // segment0 → server_a (pool 0), server_c (pool 1).
    // Pool 0 is inserted into the LinkedHashMap before pool 1 because server_a (pool 0) appears
    // first here. The round-robin fallback test depends on this insertion order.
    candidatesMap.put(SRG_SEGMENT0, Arrays.asList(
        new SegmentInstanceCandidate(SRG_SERVER_A, true, 0, 0),
        new SegmentInstanceCandidate(SRG_SERVER_C, true, 1, 0)));
    // segment1 → server_a (pool 0), server_c (pool 1)
    candidatesMap.put(SRG_SEGMENT1, Arrays.asList(
        new SegmentInstanceCandidate(SRG_SERVER_A, true, 0, 0),
        new SegmentInstanceCandidate(SRG_SERVER_C, true, 1, 0)));
    // segment2 → server_b (pool 0), server_d (pool 1)
    candidatesMap.put(SRG_SEGMENT2, Arrays.asList(
        new SegmentInstanceCandidate(SRG_SERVER_B, true, 0, 0),
        new SegmentInstanceCandidate(SRG_SERVER_D, true, 1, 0)));
    return new SegmentStates(candidatesMap, new HashSet<>(SRG_SEGMENTS), null);
  }

  private StrictReplicaGroupInstanceSelector buildStrictReplicaGroupArSelector(HybridSelector hybridSelector) {
    return buildStrictReplicaGroupArSelector(hybridSelector, mock(BrokerMetrics.class));
  }

  private StrictReplicaGroupInstanceSelector buildStrictReplicaGroupArSelector(HybridSelector hybridSelector,
      BrokerMetrics brokerMetrics) {
    return buildStrictReplicaGroupArSelector(hybridSelector, brokerMetrics,
        new PinotConfiguration(Collections.emptyMap()));
  }

  private StrictReplicaGroupInstanceSelector buildStrictReplicaGroupArSelector(HybridSelector hybridSelector,
      BrokerMetrics brokerMetrics, PinotConfiguration brokerConfig) {
    RoutingConfig routingConfig = new RoutingConfig(null, null, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testUpsertTable")
        .setRoutingConfig(routingConfig).build();
    // Ideal state: mirrors the topology above
    IdealState idealState = createIdealState(Map.of(
        SRG_SEGMENT0, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(SRG_SERVER_C, ONLINE)),
        SRG_SEGMENT1, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(SRG_SERVER_C, ONLINE)),
        SRG_SEGMENT2, List.of(Pair.of(SRG_SERVER_B, ONLINE), Pair.of(SRG_SERVER_D, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        SRG_SEGMENT0, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(SRG_SERVER_C, ONLINE)),
        SRG_SEGMENT1, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(SRG_SERVER_C, ONLINE)),
        SRG_SEGMENT2, List.of(Pair.of(SRG_SERVER_B, ONLINE), Pair.of(SRG_SERVER_D, ONLINE))));
    // Provide server instances with pool assignments
    ServerInstance serverA = mock(ServerInstance.class);
    when(serverA.getPool()).thenReturn(0);
    ServerInstance serverB = mock(ServerInstance.class);
    when(serverB.getPool()).thenReturn(0);
    ServerInstance serverC = mock(ServerInstance.class);
    when(serverC.getPool()).thenReturn(1);
    ServerInstance serverD = mock(ServerInstance.class);
    when(serverD.getPool()).thenReturn(1);
    Map<String, ServerInstance> serverMap = Map.of(
        SRG_SERVER_A, serverA, SRG_SERVER_B, serverB,
        SRG_SERVER_C, serverC, SRG_SERVER_D, serverD);
    return (StrictReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
        mock(ZkHelixPropertyStore.class), brokerMetrics, hybridSelector,
        brokerConfig,
        Set.of(SRG_SERVER_A, SRG_SERVER_B, SRG_SERVER_C, SRG_SERVER_D),
        serverMap, idealState, externalView, new HashSet<>(SRG_SEGMENTS));
  }

  @Test
  public void testStrictReplicaGroupAdaptivePicksBestReplicaGroupByWorstCaseServer() {
    // Pool 0 servers: server_a (rank 2), server_b (rank 3) → worst = 3
    // Pool 1 servers: server_c (rank 0), server_d (rank 1) → worst = 1
    // Pool 1 is better (lower worst-case rank), so all segments should route to pool 1
    HybridSelector hybridSelector = mock(HybridSelector.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    StrictReplicaGroupInstanceSelector instanceSelector = buildStrictReplicaGroupArSelector(hybridSelector,
        brokerMetrics);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(SRG_SERVER_C, 1.0),  // rank 0 (best)
        new ImmutablePair<>(SRG_SERVER_D, 2.0),  // rank 1
        new ImmutablePair<>(SRG_SERVER_A, 3.0),  // rank 2
        new ImmutablePair<>(SRG_SERVER_B, 4.0)   // rank 3 (worst)
    ));

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(SRG_SEGMENTS, 0, buildStrictReplicaGroupSegmentStates(), null);

    // All segments routed to pool 1
    assertEquals(result.segmentToInstanceMap(), Map.of(
        SRG_SEGMENT0, SRG_SERVER_C,
        SRG_SEGMENT1, SRG_SERVER_C,
        SRG_SEGMENT2, SRG_SERVER_D));
    verify(brokerMetrics).addMeteredValue(eq(BrokerMeter.POOL_SEG_QUERIES), eq(3L),
        eq(BrokerMetrics.getTagForPreferredPool(null)), eq("1"));
  }

  @Test
  public void testStrictReplicaGroupAdaptivePicksPool0WhenItHasBetterWorstCase() {
    // Pool 0 servers: server_a (rank 0), server_b (rank 1) → worst = 1
    // Pool 1 servers: server_c (rank 2), server_d (rank 3) → worst = 3
    // Pool 0 is better
    HybridSelector hybridSelector = mock(HybridSelector.class);
    StrictReplicaGroupInstanceSelector instanceSelector = buildStrictReplicaGroupArSelector(hybridSelector);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(SRG_SERVER_A, 1.0),
        new ImmutablePair<>(SRG_SERVER_B, 2.0),
        new ImmutablePair<>(SRG_SERVER_C, 3.0),
        new ImmutablePair<>(SRG_SERVER_D, 4.0)
    ));

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(SRG_SEGMENTS, 0, buildStrictReplicaGroupSegmentStates(), null);

    // All segments routed to pool 0
    assertEquals(result.segmentToInstanceMap(), Map.of(
        SRG_SEGMENT0, SRG_SERVER_A,
        SRG_SEGMENT1, SRG_SERVER_A,
        SRG_SEGMENT2, SRG_SERVER_B));
  }

  @Test
  public void testStrictReplicaGroupAdaptiveNoStatsFallsBackToRoundRobin() {
    // When fetchServerRankingsWithScores returns empty, fall back to round-robin
    HybridSelector hybridSelector = mock(HybridSelector.class);
    StrictReplicaGroupInstanceSelector instanceSelector = buildStrictReplicaGroupArSelector(hybridSelector);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Collections.emptyList());

    // requestId=0 → picks group at index 0 (pool 0)
    InstanceSelector.InstanceMapping result =
        instanceSelector.select(SRG_SEGMENTS, 0, buildStrictReplicaGroupSegmentStates(), null);
    assertEquals(result.segmentToInstanceMap(), Map.of(
        SRG_SEGMENT0, SRG_SERVER_A,
        SRG_SEGMENT1, SRG_SERVER_A,
        SRG_SEGMENT2, SRG_SERVER_B));

    // requestId=1 → picks group at index 1 (pool 1)
    result = instanceSelector.select(SRG_SEGMENTS, 1, buildStrictReplicaGroupSegmentStates(), null);
    assertEquals(result.segmentToInstanceMap(), Map.of(
        SRG_SEGMENT0, SRG_SERVER_C,
        SRG_SEGMENT1, SRG_SERVER_C,
        SRG_SEGMENT2, SRG_SERVER_D));
  }

  @Test
  public void testStrictReplicaGroupAdaptiveConsistencyAllSegmentsSameReplicaGroup() {
    // For any set of adaptive scores, all segments must route to the same replica group
    HybridSelector hybridSelector = mock(HybridSelector.class);
    StrictReplicaGroupInstanceSelector instanceSelector = buildStrictReplicaGroupArSelector(hybridSelector);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(SRG_SERVER_C, 1.0),
        new ImmutablePair<>(SRG_SERVER_D, 2.0),
        new ImmutablePair<>(SRG_SERVER_A, 3.0),
        new ImmutablePair<>(SRG_SERVER_B, 4.0)
    ));

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(SRG_SEGMENTS, 42, buildStrictReplicaGroupSegmentStates(), null);

    // Collect all pool IDs from selected instances
    Map<String, String> selections = result.segmentToInstanceMap();
    Set<Integer> poolsUsed = new HashSet<>();
    SegmentStates states = buildStrictReplicaGroupSegmentStates();
    for (Map.Entry<String, String> entry : selections.entrySet()) {
      String segment = entry.getKey();
      String selectedInstance = entry.getValue();
      for (SegmentInstanceCandidate candidate : states.getCandidates(segment)) {
        if (candidate.getInstance().equals(selectedInstance)) {
          poolsUsed.add(candidate.getPool());
          break;
        }
      }
    }
    // All segments must be from the same pool
    assertEquals(poolsUsed.size(), 1, "All segments should be routed to the same replica group");
  }

  @Test
  public void testStrictReplicaGroupAdaptivePrefersFullCoverageGroup() {
    // Topology: pool 0 has all 3 segments, pool 1 only has segment0 and segment1 (missing segment2).
    // Even though pool 1 has better adaptive ranks, pool 0 should be chosen because it has full coverage.
    // Pool 0: server_a (seg0, seg1), server_b (seg2) — full coverage
    // Pool 1: server_c (seg0, seg1) — missing seg2, partial coverage
    String serverC = "server_c";
    HybridSelector hybridSelector = mock(HybridSelector.class);

    RoutingConfig routingConfig = new RoutingConfig(null, null, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testUpsertTable")
        .setRoutingConfig(routingConfig).build();
    // Pool 1 only has seg0 and seg1 (no seg2)
    IdealState idealState = createIdealState(Map.of(
        SRG_SEGMENT0, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(serverC, ONLINE)),
        SRG_SEGMENT1, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(serverC, ONLINE)),
        SRG_SEGMENT2, List.of(Pair.of(SRG_SERVER_B, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        SRG_SEGMENT0, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(serverC, ONLINE)),
        SRG_SEGMENT1, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(serverC, ONLINE)),
        SRG_SEGMENT2, List.of(Pair.of(SRG_SERVER_B, ONLINE))));
    ServerInstance serverA = mock(ServerInstance.class);
    when(serverA.getPool()).thenReturn(0);
    ServerInstance serverB = mock(ServerInstance.class);
    when(serverB.getPool()).thenReturn(0);
    ServerInstance serverCInstance = mock(ServerInstance.class);
    when(serverCInstance.getPool()).thenReturn(1);
    Map<String, ServerInstance> serverMap = Map.of(
        SRG_SERVER_A, serverA, SRG_SERVER_B, serverB, serverC, serverCInstance);
    StrictReplicaGroupInstanceSelector instanceSelector =
        (StrictReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
            mock(ZkHelixPropertyStore.class), mock(BrokerMetrics.class), hybridSelector,
            new PinotConfiguration(Collections.emptyMap()),
            Set.of(SRG_SERVER_A, SRG_SERVER_B, serverC),
            serverMap, idealState, externalView, new HashSet<>(SRG_SEGMENTS));

    // Pool 1 (server_c) has rank 0 — best rank, but only covers 2 of 3 segments
    // Pool 0 (server_a rank 2, server_b rank 3) — worse ranks, but full coverage
    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(serverC, 1.0),        // rank 0 (best)
        new ImmutablePair<>(SRG_SERVER_A, 3.0),   // rank 1
        new ImmutablePair<>(SRG_SERVER_B, 4.0)    // rank 2
    ));

    // Build segment states where pool 1 is missing seg2
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    candidatesMap.put(SRG_SEGMENT0, Arrays.asList(
        new SegmentInstanceCandidate(SRG_SERVER_A, true, 0, 0),
        new SegmentInstanceCandidate(serverC, true, 1, 0)));
    candidatesMap.put(SRG_SEGMENT1, Arrays.asList(
        new SegmentInstanceCandidate(SRG_SERVER_A, true, 0, 0),
        new SegmentInstanceCandidate(serverC, true, 1, 0)));
    candidatesMap.put(SRG_SEGMENT2, List.of(
        new SegmentInstanceCandidate(SRG_SERVER_B, true, 0, 0)));
    SegmentStates segmentStates = new SegmentStates(candidatesMap, new HashSet<>(SRG_SEGMENTS), null);

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(SRG_SEGMENTS, 0, segmentStates, null);

    // Pool 0 should be chosen (full coverage) even though pool 1 has better ranks
    assertEquals(result.segmentToInstanceMap(), Map.of(
        SRG_SEGMENT0, SRG_SERVER_A,
        SRG_SEGMENT1, SRG_SERVER_A,
        SRG_SEGMENT2, SRG_SERVER_B));
  }

  @Test
  public void testStrictReplicaGroupAdaptiveNewSegmentNotFalselyUnavailable() {
    // Topology: server_a (pool 0) hosts seg0 only. seg1 has no online instance (simulating a
    // new/unassigned segment with null candidates in segmentStates). Pool 0 is chosen (only pool).
    // seg1 should NOT appear in unavailable — it is a new segment, not a dropped one.
    String serverA = "new_server_a";
    String seg0 = "new_seg0";
    String seg1 = "new_seg1";
    List<String> segments = Arrays.asList(seg0, seg1);
    HybridSelector hybridSelector = mock(HybridSelector.class);

    RoutingConfig routingConfig = new RoutingConfig(null, null, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testNewSegTable")
        .setRoutingConfig(routingConfig).build();
    // seg1 intentionally absent from idealState/externalView → null candidates (new segment)
    IdealState idealState = createIdealState(Map.of(
        seg0, List.of(Pair.of(serverA, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        seg0, List.of(Pair.of(serverA, ONLINE))));
    ServerInstance serverAInstance = mock(ServerInstance.class);
    when(serverAInstance.getPool()).thenReturn(0);
    StrictReplicaGroupInstanceSelector instanceSelector =
        (StrictReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
            mock(ZkHelixPropertyStore.class), mock(BrokerMetrics.class), hybridSelector,
            new PinotConfiguration(Collections.emptyMap()),
            Set.of(serverA),
            Map.of(serverA, serverAInstance),
            idealState, externalView, new HashSet<>(List.of(seg0)));

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(serverA, 1.0)));

    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(null);

    InstanceSelector.SelectionResult result = instanceSelector.select(brokerRequest, segments, 0);
    assertEquals(result.getSegmentToInstanceMap(), Map.of(seg0, serverA));
    // seg1 has null candidates (not yet in the selector's state) → must NOT appear as unavailable
    assertTrue(result.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testStrictReplicaGroupAdaptiveDroppedSegmentReportedAsUnavailable() {
    // Topology: no replica group has full coverage.
    // Pool 0: server_a hosts seg0, seg1 (missing seg2)
    // Pool 1: server_c hosts seg1, seg2 (missing seg0)
    // Pool 1 has better ranks -> chosen. seg0 has no candidate in pool 1 -> reported unavailable.
    String serverA = "partial_server_a";
    String serverC = "partial_server_c";
    String seg0 = "partial_seg0";
    String seg1 = "partial_seg1";
    String seg2 = "partial_seg2";
    List<String> segments = Arrays.asList(seg0, seg1, seg2);
    HybridSelector hybridSelector = mock(HybridSelector.class);

    RoutingConfig routingConfig = new RoutingConfig(null, null, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testPartialCoverage")
            .setRoutingConfig(routingConfig).build();
    IdealState idealState = createIdealState(Map.of(
            seg0, List.of(Pair.of(serverA, ONLINE)),
            seg1, List.of(Pair.of(serverA, ONLINE), Pair.of(serverC, ONLINE)),
            seg2, List.of(Pair.of(serverC, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
            seg0, List.of(Pair.of(serverA, ONLINE)),
            seg1, List.of(Pair.of(serverA, ONLINE), Pair.of(serverC, ONLINE)),
            seg2, List.of(Pair.of(serverC, ONLINE))));
    ServerInstance serverAInstance = mock(ServerInstance.class);
    when(serverAInstance.getPool()).thenReturn(0);
    ServerInstance serverCInstance = mock(ServerInstance.class);
    when(serverCInstance.getPool()).thenReturn(1);
    Map<String, ServerInstance> serverMap = Map.of(serverA, serverAInstance, serverC, serverCInstance);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    StrictReplicaGroupInstanceSelector instanceSelector =
            (StrictReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
                    mock(ZkHelixPropertyStore.class), brokerMetrics, hybridSelector,
                    new PinotConfiguration(Collections.emptyMap()),
                    Set.of(serverA, serverC),
                    serverMap, idealState, externalView, new HashSet<>(segments));

    // Pool 1 (server_c) ranked better -> pool 1 chosen
    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
            new ImmutablePair<>(serverC, 1.0),
            new ImmutablePair<>(serverA, 3.0)
    ));

    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(null);

    InstanceSelector.SelectionResult result = instanceSelector.select(brokerRequest, segments, 0);

    // seg1 and seg2 routed to pool 1
    assertEquals(result.getSegmentToInstanceMap(), Map.of(seg1, serverC, seg2, serverC));
    // seg0 has candidates (pool 0 only) but no match in chosen pool 1 -> reported unavailable
    assertEquals(result.getUnavailableSegments(), List.of(seg0));
  }

  @Test
  public void testStrictReplicaGroupAdaptiveOnlyQueryRelevantServersScored() {
    // Topology: pool 1 has an extra server (server_e) that hosts a segment NOT in the query.
    // server_e has a very high rank but should not affect pool 1's score since it hosts no query segments.
    // Pool 0: server_a (rank 2), server_b (rank 3) → worst = 3
    // Pool 1: server_c (rank 0), server_d (rank 1) → worst = 1
    //   (server_e at rank 99 hosts segment "seg_extra" which is not in the query)
    // Pool 1 should win because only query-relevant servers matter (server_e is ignored).
    String serverE = "server_e";
    String segExtra = "seg_extra";
    HybridSelector hybridSelector = mock(HybridSelector.class);

    // Build selector with server_e in pool 1, hosting seg_extra
    RoutingConfig routingConfig = new RoutingConfig(null, null, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testUpsertTable")
        .setRoutingConfig(routingConfig).build();
    IdealState idealState = createIdealState(Map.of(
        SRG_SEGMENT0, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(SRG_SERVER_C, ONLINE)),
        SRG_SEGMENT1, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(SRG_SERVER_C, ONLINE)),
        SRG_SEGMENT2, List.of(Pair.of(SRG_SERVER_B, ONLINE), Pair.of(SRG_SERVER_D, ONLINE)),
        segExtra, List.of(Pair.of(serverE, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        SRG_SEGMENT0, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(SRG_SERVER_C, ONLINE)),
        SRG_SEGMENT1, List.of(Pair.of(SRG_SERVER_A, ONLINE), Pair.of(SRG_SERVER_C, ONLINE)),
        SRG_SEGMENT2, List.of(Pair.of(SRG_SERVER_B, ONLINE), Pair.of(SRG_SERVER_D, ONLINE)),
        segExtra, List.of(Pair.of(serverE, ONLINE))));
    ServerInstance serverA = mock(ServerInstance.class);
    when(serverA.getPool()).thenReturn(0);
    ServerInstance serverB = mock(ServerInstance.class);
    when(serverB.getPool()).thenReturn(0);
    ServerInstance serverC = mock(ServerInstance.class);
    when(serverC.getPool()).thenReturn(1);
    ServerInstance serverD = mock(ServerInstance.class);
    when(serverD.getPool()).thenReturn(1);
    ServerInstance serverEInstance = mock(ServerInstance.class);
    when(serverEInstance.getPool()).thenReturn(1);
    Map<String, ServerInstance> serverMap = Map.of(
        SRG_SERVER_A, serverA, SRG_SERVER_B, serverB,
        SRG_SERVER_C, serverC, SRG_SERVER_D, serverD,
        serverE, serverEInstance);
    Set<String> allSegments = new HashSet<>(SRG_SEGMENTS);
    allSegments.add(segExtra);
    StrictReplicaGroupInstanceSelector instanceSelector =
        (StrictReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
            mock(ZkHelixPropertyStore.class), mock(BrokerMetrics.class), hybridSelector,
            new PinotConfiguration(Collections.emptyMap()),
            Set.of(SRG_SERVER_A, SRG_SERVER_B, SRG_SERVER_C, SRG_SERVER_D, serverE),
            serverMap, idealState, externalView, allSegments);

    // server_e has rank 99 but hosts no query segments — should be irrelevant
    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(SRG_SERVER_C, 1.0),   // rank 0
        new ImmutablePair<>(SRG_SERVER_D, 2.0),   // rank 1
        new ImmutablePair<>(SRG_SERVER_A, 3.0),   // rank 2
        new ImmutablePair<>(SRG_SERVER_B, 4.0),   // rank 3
        new ImmutablePair<>(serverE, 99.0)        // rank 4 (high score, not query-relevant)
    ));

    // Query only SRG_SEGMENTS (not seg_extra), so server_e is not query-relevant
    InstanceSelector.InstanceMapping result =
        instanceSelector.select(SRG_SEGMENTS, 0, buildStrictReplicaGroupSegmentStates(), null);

    // Pool 1 wins (worst rank among query-relevant servers = 1) over pool 0 (worst = 3)
    // server_e's rank 4 is not considered because it hosts no query segments
    assertEquals(result.segmentToInstanceMap(), Map.of(
        SRG_SEGMENT0, SRG_SERVER_C,
        SRG_SEGMENT1, SRG_SERVER_C,
        SRG_SEGMENT2, SRG_SERVER_D));

    ArgumentCaptor<List<String>> rankedCandidatesCaptor = ArgumentCaptor.forClass(List.class);
    verify(hybridSelector).fetchServerRankingsWithScores(rankedCandidatesCaptor.capture());
    assertEquals(new HashSet<>(rankedCandidatesCaptor.getValue()),
        Set.of(SRG_SERVER_A, SRG_SERVER_B, SRG_SERVER_C, SRG_SERVER_D));
    assertEquals(rankedCandidatesCaptor.getValue().size(), 4);
  }
}
