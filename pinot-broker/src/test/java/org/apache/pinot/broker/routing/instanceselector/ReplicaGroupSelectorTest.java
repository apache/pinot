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
import static org.apache.pinot.spi.utils.CommonConstants.Broker.FALLBACK_POOL_ID;
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
 * including adaptive server selection (replica-group-level routing for strict replica groups).
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

  // Shared topology for AR tests: 3 segments across 5 instances in two pools / replica groups.
  // segment2 intentionally has AR_P0_RG0_SERVER_E (unranked) so AR falls back to round-robin for that segment.
  private static final String AR_P0_RG0_SERVER_A = "ar_p0_rg0_server_a";
  private static final String AR_P1_RG1_SERVER_B = "ar_p1_rg1_server_b";
  private static final String AR_P0_RG0_SERVER_C = "ar_p0_rg0_server_c";
  private static final String AR_P1_RG1_SERVER_D = "ar_p1_rg1_server_d";
  private static final String AR_P0_RG0_SERVER_E = "ar_p0_rg0_server_e";
  private static final String AR_SEGMENT0 = "segment0";
  private static final String AR_SEGMENT1 = "segment1";
  private static final String AR_SEGMENT2 = "segment2";
  private static final List<String> AR_SEGMENTS =
      Arrays.asList(AR_SEGMENT0, AR_SEGMENT1, AR_SEGMENT2);
  // Rankings: D best → C → B → A worst; E absent (triggers AR fallback)
  private static final List<Pair<String, Double>> AR_SERVER_RANKS = Arrays.asList(
      new ImmutablePair<>(AR_P1_RG1_SERVER_D, 1.0),
      new ImmutablePair<>(AR_P0_RG0_SERVER_C, 2.0),
      new ImmutablePair<>(AR_P1_RG1_SERVER_B, 3.0),
      new ImmutablePair<>(AR_P0_RG0_SERVER_A, 4.0)
  );

  private SegmentStates buildArSegmentStates() {
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    // segment0 → pool 0 / replica group 0 server A, pool 1 / replica group 1 server B
    candidatesMap.put(AR_SEGMENT0, Arrays.asList(
        new SegmentInstanceCandidate(AR_P0_RG0_SERVER_A, true, 0, 0),
        new SegmentInstanceCandidate(AR_P1_RG1_SERVER_B, true, 1, 1)));
    // segment1 → pool 0 / replica group 0 server C, pool 1 / replica group 1 server D
    candidatesMap.put(AR_SEGMENT1, Arrays.asList(
        new SegmentInstanceCandidate(AR_P0_RG0_SERVER_C, true, 0, 0),
        new SegmentInstanceCandidate(AR_P1_RG1_SERVER_D, true, 1, 1)));
    // segment2 → pool 0 / replica group 0 server E (unranked), pool 1 / replica group 1 server D
    candidatesMap.put(AR_SEGMENT2, Arrays.asList(
        new SegmentInstanceCandidate(AR_P0_RG0_SERVER_E, true, 0, 0),
        new SegmentInstanceCandidate(AR_P1_RG1_SERVER_D, true, 1, 1)));
    return new SegmentStates(candidatesMap, new HashSet<>(AR_SEGMENTS), null);
  }

  private ReplicaGroupInstanceSelector buildArSelector(String selectorType, HybridSelector hybridSelector) {
    return buildArSelector(selectorType, hybridSelector, new PinotConfiguration(Map.of()));
  }

  private ReplicaGroupInstanceSelector buildArSelector(String selectorType, HybridSelector hybridSelector,
      PinotConfiguration brokerConfig) {
    RoutingConfig routingConfig = new RoutingConfig(null, null, selectorType, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setRoutingConfig(routingConfig).build();
    IdealState idealState = createIdealState(Map.of(
        AR_SEGMENT0, List.of(Pair.of(AR_P0_RG0_SERVER_A, ONLINE), Pair.of(AR_P1_RG1_SERVER_B, ONLINE)),
        AR_SEGMENT1, List.of(Pair.of(AR_P0_RG0_SERVER_C, ONLINE), Pair.of(AR_P1_RG1_SERVER_D, ONLINE)),
        AR_SEGMENT2, List.of(Pair.of(AR_P1_RG1_SERVER_D, ONLINE), Pair.of(AR_P0_RG0_SERVER_E, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        AR_SEGMENT0, List.of(Pair.of(AR_P0_RG0_SERVER_A, ONLINE), Pair.of(AR_P1_RG1_SERVER_B, ONLINE)),
        AR_SEGMENT1, List.of(Pair.of(AR_P0_RG0_SERVER_C, ONLINE), Pair.of(AR_P1_RG1_SERVER_D, ONLINE)),
        AR_SEGMENT2, List.of(Pair.of(AR_P1_RG1_SERVER_D, ONLINE), Pair.of(AR_P0_RG0_SERVER_E, ONLINE))));
    ServerInstance serverA = mock(ServerInstance.class);
    when(serverA.getPool()).thenReturn(0);
    ServerInstance serverB = mock(ServerInstance.class);
    when(serverB.getPool()).thenReturn(1);
    ServerInstance serverC = mock(ServerInstance.class);
    when(serverC.getPool()).thenReturn(0);
    ServerInstance serverD = mock(ServerInstance.class);
    when(serverD.getPool()).thenReturn(1);
    ServerInstance serverE = mock(ServerInstance.class);
    when(serverE.getPool()).thenReturn(0);
    Map<String, ServerInstance> serverMap = Map.of(
        AR_P0_RG0_SERVER_A, serverA,
        AR_P1_RG1_SERVER_B, serverB,
        AR_P0_RG0_SERVER_C, serverC,
        AR_P1_RG1_SERVER_D, serverD,
        AR_P0_RG0_SERVER_E, serverE);
    return (ReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
        mock(ZkHelixPropertyStore.class), mock(BrokerMetrics.class), hybridSelector,
        brokerConfig,
        Set.of(AR_P0_RG0_SERVER_A, AR_P1_RG1_SERVER_B, AR_P0_RG0_SERVER_C, AR_P1_RG1_SERVER_D, AR_P0_RG0_SERVER_E),
        serverMap, idealState, externalView, new HashSet<>(AR_SEGMENTS));
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
    // segment0: B (rank 3) over A (rank 4)
    // segment1: D (rank 1) over C (rank 2)
    // segment2: E unranked → AR falls back to round-robin → index 0 = E
    assertEquals(result.segmentToInstanceMap(), Map.of(
        AR_SEGMENT0, AR_P1_RG1_SERVER_B,
        AR_SEGMENT1, AR_P1_RG1_SERVER_D,
        AR_SEGMENT2, AR_P0_RG0_SERVER_E));
  }

  @Test
  public void testStrictReplicaGroupAdaptiveDisabledByFeatureFlag() {
    // When the feature flag is disabled, the factory nulls out the adaptive selector so the selector falls back to the
    // strict selector's non-adaptive, round-robin-by-replica-index path.
    HybridSelector hybridSelector = mock(HybridSelector.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    PinotConfiguration brokerConfig = new PinotConfiguration(Map.of(
        CommonConstants.Broker.AdaptiveServerSelector.CONFIG_OF_ENABLE_STRICT_REPLICA_GROUP, "false"));
    StrictReplicaGroupInstanceSelector instanceSelector =
        buildStrictReplicaGroupArSelector(hybridSelector, brokerMetrics, brokerConfig);

    assertNull(instanceSelector._adaptiveServerSelector);
    assertNull(instanceSelector._priorityPoolInstanceSelector);

    BaseInstanceSelector.InstanceMapping result =
        instanceSelector.select(STRICT_SEGMENTS, 0, buildStrictReplicaGroupSegmentStates(), null);

    verify(hybridSelector, never()).fetchServerRankingsWithScores(any());
    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT0, STRICT_RG0_SERVER_A,
        STRICT_SEGMENT1, STRICT_RG0_SERVER_A,
        STRICT_SEGMENT2, STRICT_RG0_SERVER_B));

    result = instanceSelector.select(STRICT_SEGMENTS, 1, buildStrictReplicaGroupSegmentStates(), null);
    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT0, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT1, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT2, STRICT_RG1_SERVER_D));
  }

  @Test
  public void testStrictReplicaGroupFactoryEnablesAdaptiveRouting() {
    HybridSelector hybridSelector = mock(HybridSelector.class);
    ReplicaGroupInstanceSelector instanceSelector =
        buildArSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, hybridSelector);

    assertTrue(instanceSelector instanceof StrictReplicaGroupInstanceSelector);
    assertNotNull(instanceSelector._adaptiveServerSelector);
    assertNotNull(instanceSelector._priorityPoolInstanceSelector);
  }

  // --- Strict replica group adaptive routing tests ---

  // Topology: 2 replica groups, 3 segments
  // Replica group 0: server_a (segment0, segment1), server_b (segment2)
  // Replica group 1: server_c (segment0, segment1), server_d (segment2)
  // If a strict-fixture server name omits `P#`, its pool is FALLBACK_POOL_ID.
  private static final String STRICT_RG0_SERVER_A = "strict_rg0_server_a";
  private static final String STRICT_RG0_SERVER_B = "strict_rg0_server_b";
  private static final String STRICT_RG1_SERVER_C = "strict_rg1_server_c";
  private static final String STRICT_RG1_SERVER_D = "strict_rg1_server_d";
  private static final String STRICT_SEGMENT0 = "seg0";
  private static final String STRICT_SEGMENT1 = "seg1";
  private static final String STRICT_SEGMENT2 = "seg2";
  private static final List<String> STRICT_SEGMENTS = Arrays.asList(STRICT_SEGMENT0, STRICT_SEGMENT1, STRICT_SEGMENT2);

  private List<SegmentInstanceCandidate> buildStrictReplicaGroupCandidates(String replicaGroup0Instance,
      String replicaGroup1Instance) {
    return Arrays.asList(
        new SegmentInstanceCandidate(replicaGroup0Instance, true, FALLBACK_POOL_ID, 0),
        new SegmentInstanceCandidate(replicaGroup1Instance, true, FALLBACK_POOL_ID, 1));
  }

  private SegmentStates buildStrictReplicaGroupSegmentStates() {
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    // All strict-fixture servers intentionally share FALLBACK_POOL_ID so these tests fail if strict routing regresses
    // back to grouping or filtering by pool instead of idealStateReplicaId.
    candidatesMap.put(STRICT_SEGMENT0,
        buildStrictReplicaGroupCandidates(STRICT_RG0_SERVER_A, STRICT_RG1_SERVER_C));
    candidatesMap.put(STRICT_SEGMENT1,
        buildStrictReplicaGroupCandidates(STRICT_RG0_SERVER_A, STRICT_RG1_SERVER_C));
    candidatesMap.put(STRICT_SEGMENT2,
        buildStrictReplicaGroupCandidates(STRICT_RG0_SERVER_B, STRICT_RG1_SERVER_D));
    return new SegmentStates(candidatesMap, new HashSet<>(STRICT_SEGMENTS), null);
  }

  private StrictReplicaGroupInstanceSelector buildStrictReplicaGroupArSelector(HybridSelector hybridSelector) {
    return buildStrictReplicaGroupArSelector(hybridSelector, mock(BrokerMetrics.class));
  }

  private StrictReplicaGroupInstanceSelector buildStrictReplicaGroupArSelector(HybridSelector hybridSelector,
      BrokerMetrics brokerMetrics) {
    return buildStrictReplicaGroupArSelector(hybridSelector, brokerMetrics,
        new PinotConfiguration(Map.of()));
  }

  private StrictReplicaGroupInstanceSelector buildStrictReplicaGroupArSelector(HybridSelector hybridSelector,
      BrokerMetrics brokerMetrics, PinotConfiguration brokerConfig) {
    RoutingConfig routingConfig = new RoutingConfig(null, null, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testUpsertTable")
        .setRoutingConfig(routingConfig).build();
    // Ideal state: mirrors the topology above
    IdealState idealState = createIdealState(Map.of(
        STRICT_SEGMENT0, List.of(
            Pair.of(STRICT_RG0_SERVER_A, ONLINE),
            Pair.of(STRICT_RG1_SERVER_C, ONLINE)),
        STRICT_SEGMENT1, List.of(
            Pair.of(STRICT_RG0_SERVER_A, ONLINE),
            Pair.of(STRICT_RG1_SERVER_C, ONLINE)),
        STRICT_SEGMENT2, List.of(
            Pair.of(STRICT_RG0_SERVER_B, ONLINE),
            Pair.of(STRICT_RG1_SERVER_D, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        STRICT_SEGMENT0, List.of(
            Pair.of(STRICT_RG0_SERVER_A, ONLINE),
            Pair.of(STRICT_RG1_SERVER_C, ONLINE)),
        STRICT_SEGMENT1, List.of(
            Pair.of(STRICT_RG0_SERVER_A, ONLINE),
            Pair.of(STRICT_RG1_SERVER_C, ONLINE)),
        STRICT_SEGMENT2, List.of(
            Pair.of(STRICT_RG0_SERVER_B, ONLINE),
            Pair.of(STRICT_RG1_SERVER_D, ONLINE))));
    // All servers use the fallback pool to prove strict routing keys off idealStateReplicaId rather than pool.
    ServerInstance serverA = mock(ServerInstance.class);
    when(serverA.getPool()).thenReturn(FALLBACK_POOL_ID);
    ServerInstance serverB = mock(ServerInstance.class);
    when(serverB.getPool()).thenReturn(FALLBACK_POOL_ID);
    ServerInstance serverC = mock(ServerInstance.class);
    when(serverC.getPool()).thenReturn(FALLBACK_POOL_ID);
    ServerInstance serverD = mock(ServerInstance.class);
    when(serverD.getPool()).thenReturn(FALLBACK_POOL_ID);
    Map<String, ServerInstance> serverMap = Map.of(
        STRICT_RG0_SERVER_A, serverA, STRICT_RG0_SERVER_B, serverB,
        STRICT_RG1_SERVER_C, serverC, STRICT_RG1_SERVER_D, serverD);
    return (StrictReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
        mock(ZkHelixPropertyStore.class), brokerMetrics, hybridSelector,
        brokerConfig,
        Set.of(STRICT_RG0_SERVER_A, STRICT_RG0_SERVER_B,
            STRICT_RG1_SERVER_C, STRICT_RG1_SERVER_D),
        serverMap, idealState, externalView, new HashSet<>(STRICT_SEGMENTS));
  }

  @Test
  public void testStrictReplicaGroupAdaptivePicksBestReplicaGroupByWorstCaseServer() {
    // Replica group 0 servers: server_a (rank 2), server_b (rank 3) → worst = 3
    // Replica group 1 servers: server_c (rank 0), server_d (rank 1) → worst = 1
    // Replica group 1 is better (lower worst-case rank), so all segments should route there
    HybridSelector hybridSelector = mock(HybridSelector.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    StrictReplicaGroupInstanceSelector instanceSelector = buildStrictReplicaGroupArSelector(hybridSelector,
        brokerMetrics);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(STRICT_RG1_SERVER_C, 1.0),  // rank 0 (best)
        new ImmutablePair<>(STRICT_RG1_SERVER_D, 2.0),  // rank 1
        new ImmutablePair<>(STRICT_RG0_SERVER_A, 3.0),  // rank 2
        new ImmutablePair<>(STRICT_RG0_SERVER_B, 4.0)   // rank 3 (worst)
    ));

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(STRICT_SEGMENTS, 0, buildStrictReplicaGroupSegmentStates(), null);

    // All segments routed to replica group 1
    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT0, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT1, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT2, STRICT_RG1_SERVER_D));
    // Metrics are still reported per actual pool, even though routing is chosen by replica group.
    verify(brokerMetrics).addMeteredValue(eq(BrokerMeter.POOL_SEG_QUERIES), eq(3L),
        eq(BrokerMetrics.getTagForPreferredPool(null)), eq(String.valueOf(FALLBACK_POOL_ID)));
  }

  @Test
  public void testStrictReplicaGroupAdaptivePicksGroup0WhenItHasBetterWorstCase() {
    // Replica group 0 servers: server_a (rank 0), server_b (rank 1) → worst = 1
    // Replica group 1 servers: server_c (rank 2), server_d (rank 3) → worst = 3
    // Replica group 0 is better
    HybridSelector hybridSelector = mock(HybridSelector.class);
    StrictReplicaGroupInstanceSelector instanceSelector = buildStrictReplicaGroupArSelector(hybridSelector);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(STRICT_RG0_SERVER_A, 1.0),
        new ImmutablePair<>(STRICT_RG0_SERVER_B, 2.0),
        new ImmutablePair<>(STRICT_RG1_SERVER_C, 3.0),
        new ImmutablePair<>(STRICT_RG1_SERVER_D, 4.0)
    ));

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(STRICT_SEGMENTS, 0, buildStrictReplicaGroupSegmentStates(), null);

    // All segments routed to replica group 0
    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT0, STRICT_RG0_SERVER_A,
        STRICT_SEGMENT1, STRICT_RG0_SERVER_A,
        STRICT_SEGMENT2, STRICT_RG0_SERVER_B));
  }

  @Test
  public void testStrictReplicaGroupAdaptiveEmptyRankingsFallBackToRoundRobin() {
    // When adaptive ranking returns no servers, fall back to round-robin by replica-group index.
    HybridSelector hybridSelector = mock(HybridSelector.class);
    StrictReplicaGroupInstanceSelector instanceSelector = buildStrictReplicaGroupArSelector(hybridSelector);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(List.of());

    // requestId=0 → picks group at index 0 (replica group 0)
    InstanceSelector.InstanceMapping result =
        instanceSelector.select(STRICT_SEGMENTS, 0, buildStrictReplicaGroupSegmentStates(), null);
    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT0, STRICT_RG0_SERVER_A,
        STRICT_SEGMENT1, STRICT_RG0_SERVER_A,
        STRICT_SEGMENT2, STRICT_RG0_SERVER_B));

    // requestId=1 → picks group at index 1 (replica group 1)
    result = instanceSelector.select(STRICT_SEGMENTS, 1, buildStrictReplicaGroupSegmentStates(), null);
    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT0, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT1, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT2, STRICT_RG1_SERVER_D));
  }

  @Test
  public void testStrictReplicaGroupAdaptiveConsistencyAllSegmentsSameReplicaGroup() {
    // When adaptive scores favor replica group 1, all segments must route to that same replica group.
    HybridSelector hybridSelector = mock(HybridSelector.class);
    StrictReplicaGroupInstanceSelector instanceSelector = buildStrictReplicaGroupArSelector(hybridSelector);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(STRICT_RG1_SERVER_C, 1.0),
        new ImmutablePair<>(STRICT_RG1_SERVER_D, 2.0),
        new ImmutablePair<>(STRICT_RG0_SERVER_A, 3.0),
        new ImmutablePair<>(STRICT_RG0_SERVER_B, 4.0)
    ));

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(STRICT_SEGMENTS, 42, buildStrictReplicaGroupSegmentStates(), null);

    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT0, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT1, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT2, STRICT_RG1_SERVER_D));
  }

  @Test
  public void testStrictReplicaGroupAdaptivePrefersFullCoverageGroup() {
    // Topology: replica group 0 has all 3 segments, replica group 1 only has segment0 and segment1.
    // Even though replica group 1 has better adaptive ranks, replica group 0 should be chosen (full coverage).
    // Replica group 0: server_a (seg0, seg1), server_b (seg2) — full coverage
    // Replica group 1: server_c (seg0, seg1) — missing seg2, partial coverage
    // Pools intentionally do not match replica groups: server_a and server_c share pool 0, while server_b is pool 1.
    String serverC = "server_c";
    HybridSelector hybridSelector = mock(HybridSelector.class);

    RoutingConfig routingConfig = new RoutingConfig(null, null, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testUpsertTable")
        .setRoutingConfig(routingConfig).build();
    // Replica group 1 only has seg0 and seg1 (no seg2)
    IdealState idealState = createIdealState(Map.of(
        STRICT_SEGMENT0, List.of(Pair.of(STRICT_RG0_SERVER_A, ONLINE), Pair.of(serverC, ONLINE)),
        STRICT_SEGMENT1, List.of(Pair.of(STRICT_RG0_SERVER_A, ONLINE), Pair.of(serverC, ONLINE)),
        STRICT_SEGMENT2, List.of(Pair.of(STRICT_RG0_SERVER_B, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        STRICT_SEGMENT0, List.of(Pair.of(STRICT_RG0_SERVER_A, ONLINE), Pair.of(serverC, ONLINE)),
        STRICT_SEGMENT1, List.of(Pair.of(STRICT_RG0_SERVER_A, ONLINE), Pair.of(serverC, ONLINE)),
        STRICT_SEGMENT2, List.of(Pair.of(STRICT_RG0_SERVER_B, ONLINE))));
    ServerInstance serverA = mock(ServerInstance.class);
    when(serverA.getPool()).thenReturn(0);
    ServerInstance serverB = mock(ServerInstance.class);
    when(serverB.getPool()).thenReturn(1);
    ServerInstance serverCInstance = mock(ServerInstance.class);
    when(serverCInstance.getPool()).thenReturn(0);
    Map<String, ServerInstance> serverMap = Map.of(
        STRICT_RG0_SERVER_A, serverA, STRICT_RG0_SERVER_B, serverB, serverC, serverCInstance);
    StrictReplicaGroupInstanceSelector instanceSelector =
        (StrictReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
            mock(ZkHelixPropertyStore.class), mock(BrokerMetrics.class), hybridSelector,
            new PinotConfiguration(Map.of()),
            Set.of(STRICT_RG0_SERVER_A, STRICT_RG0_SERVER_B, serverC),
            serverMap, idealState, externalView, new HashSet<>(STRICT_SEGMENTS));

    // Replica group 1 (server_c) has rank 0 — best rank, but only covers 2 of 3 segments
    // Replica group 0 (server_a rank 1, server_b rank 2) — worse ranks, but full coverage
    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(serverC, 1.0),        // rank 0 (best)
        new ImmutablePair<>(STRICT_RG0_SERVER_A, 3.0),   // rank 1
        new ImmutablePair<>(STRICT_RG0_SERVER_B, 4.0)    // rank 2
    ));

    // Build segment states where replica group 1 is missing seg2
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    candidatesMap.put(STRICT_SEGMENT0, Arrays.asList(
        new SegmentInstanceCandidate(STRICT_RG0_SERVER_A, true, 0, 0),
        new SegmentInstanceCandidate(serverC, true, 0, 1)));
    candidatesMap.put(STRICT_SEGMENT1, Arrays.asList(
        new SegmentInstanceCandidate(STRICT_RG0_SERVER_A, true, 0, 0),
        new SegmentInstanceCandidate(serverC, true, 0, 1)));
    candidatesMap.put(STRICT_SEGMENT2, List.of(
        new SegmentInstanceCandidate(STRICT_RG0_SERVER_B, true, 1, 0)));
    SegmentStates segmentStates = new SegmentStates(candidatesMap, new HashSet<>(STRICT_SEGMENTS), null);

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(STRICT_SEGMENTS, 0, segmentStates, null);

    // Replica group 0 should be chosen (full coverage) even though replica group 1 has better ranks
    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT0, STRICT_RG0_SERVER_A,
        STRICT_SEGMENT1, STRICT_RG0_SERVER_A,
        STRICT_SEGMENT2, STRICT_RG0_SERVER_B));
  }

  @Test
  public void testStrictReplicaGroupAdaptiveNewSegmentNotFalselyUnavailable() {
    // Topology: server_a (replica group 0) hosts seg0 only. seg1 has no online instance (simulating a
    // new/unassigned segment with null candidates in segmentStates). Replica group 0 is chosen (only group).
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
    when(serverAInstance.getPool()).thenReturn(FALLBACK_POOL_ID);
    StrictReplicaGroupInstanceSelector instanceSelector =
        (StrictReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
            mock(ZkHelixPropertyStore.class), mock(BrokerMetrics.class), hybridSelector,
            new PinotConfiguration(Map.of()),
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
    // Topology: no replica group (idealStateReplicaId) has full coverage.
    // ReplicaId 0 (server_a): covers seg0, seg1 (missing seg2)
    // ReplicaId 1 (server_c, server_d): covers seg1, seg2 (missing seg0)
    // ReplicaId 1 has better rank -> chosen. seg0 has no replicaId-1 candidate -> reported unavailable.
    HybridSelector hybridSelector = mock(HybridSelector.class);
    StrictReplicaGroupInstanceSelector instanceSelector = buildStrictReplicaGroupArSelector(hybridSelector);

    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
            new ImmutablePair<>(STRICT_RG1_SERVER_C, 1.0),
            new ImmutablePair<>(STRICT_RG0_SERVER_A, 3.0)
    ));

    // Craft segment states with partial coverage per replica group:
    //   seg0: only in replicaId 0 (server_a)
    //   seg1: in both replicaId 0 (server_a) and replicaId 1 (server_c)
    //   seg2: only in replicaId 1 (server_d)
    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    candidatesMap.put(STRICT_SEGMENT0, List.of(
        new SegmentInstanceCandidate(STRICT_RG0_SERVER_A, true, FALLBACK_POOL_ID, 0)));
    candidatesMap.put(STRICT_SEGMENT1, Arrays.asList(
        new SegmentInstanceCandidate(STRICT_RG0_SERVER_A, true, FALLBACK_POOL_ID, 0),
        new SegmentInstanceCandidate(STRICT_RG1_SERVER_C, true, FALLBACK_POOL_ID, 1)));
    candidatesMap.put(STRICT_SEGMENT2, List.of(
        new SegmentInstanceCandidate(STRICT_RG1_SERVER_D, true, FALLBACK_POOL_ID, 1)));
    SegmentStates segmentStates = new SegmentStates(candidatesMap, new HashSet<>(STRICT_SEGMENTS), null);

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(STRICT_SEGMENTS, 0, segmentStates, null);

    // seg1 and seg2 routed to replicaId 1
    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT1, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT2, STRICT_RG1_SERVER_D));
    // seg0 has candidates (replicaId 0 only) but no match in chosen replicaId 1 -> reported unavailable
    assertEquals(result.unavailableSegments(), List.of(STRICT_SEGMENT0));
  }

  @Test
  public void testStrictReplicaGroupAdaptiveOnlyQueryRelevantServersScored() {
    // Replica group 1 has an extra server (server_e) that only hosts seg_extra, which is not in the query.
    // server_e should never be scored for a query over STRICT_SEGMENTS.
    // Replica group 0: server_a (rank 2), server_b (rank 3) → worst = 3
    // Replica group 1: server_c (rank 0), server_d (rank 1) → worst = 1
    // Replica group 1 should win because only query-relevant servers matter.
    String rg1ServerE = "server_e";
    String segExtra = "seg_extra";
    HybridSelector hybridSelector = mock(HybridSelector.class);

    // Build selector with server_e in replica group 1, hosting seg_extra.
    RoutingConfig routingConfig = new RoutingConfig(null, null, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testUpsertTable")
        .setRoutingConfig(routingConfig).build();
    // server_e's pool intentionally differs from the queried servers to show pool is irrelevant.
    // segExtra places server_d at position 0 and server_e at position 1 (idealStateReplicaId=1).
    IdealState idealState = createIdealState(Map.of(
        STRICT_SEGMENT0, List.of(Pair.of(STRICT_RG0_SERVER_A, ONLINE), Pair.of(STRICT_RG1_SERVER_C, ONLINE)),
        STRICT_SEGMENT1, List.of(Pair.of(STRICT_RG0_SERVER_A, ONLINE), Pair.of(STRICT_RG1_SERVER_C, ONLINE)),
        STRICT_SEGMENT2, List.of(Pair.of(STRICT_RG0_SERVER_B, ONLINE), Pair.of(STRICT_RG1_SERVER_D, ONLINE)),
        segExtra, List.of(Pair.of(STRICT_RG1_SERVER_D, ONLINE), Pair.of(rg1ServerE, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        STRICT_SEGMENT0, List.of(Pair.of(STRICT_RG0_SERVER_A, ONLINE), Pair.of(STRICT_RG1_SERVER_C, ONLINE)),
        STRICT_SEGMENT1, List.of(Pair.of(STRICT_RG0_SERVER_A, ONLINE), Pair.of(STRICT_RG1_SERVER_C, ONLINE)),
        STRICT_SEGMENT2, List.of(Pair.of(STRICT_RG0_SERVER_B, ONLINE), Pair.of(STRICT_RG1_SERVER_D, ONLINE)),
        segExtra, List.of(Pair.of(STRICT_RG1_SERVER_D, ONLINE), Pair.of(rg1ServerE, ONLINE))));
    ServerInstance serverA = mock(ServerInstance.class);
    when(serverA.getPool()).thenReturn(FALLBACK_POOL_ID);
    ServerInstance serverB = mock(ServerInstance.class);
    when(serverB.getPool()).thenReturn(FALLBACK_POOL_ID);
    ServerInstance serverC = mock(ServerInstance.class);
    when(serverC.getPool()).thenReturn(FALLBACK_POOL_ID);
    ServerInstance serverD = mock(ServerInstance.class);
    when(serverD.getPool()).thenReturn(FALLBACK_POOL_ID);
    ServerInstance serverEInstance = mock(ServerInstance.class);
    when(serverEInstance.getPool()).thenReturn(99);
    Map<String, ServerInstance> serverMap = Map.of(
        STRICT_RG0_SERVER_A, serverA, STRICT_RG0_SERVER_B, serverB,
        STRICT_RG1_SERVER_C, serverC, STRICT_RG1_SERVER_D, serverD,
        rg1ServerE, serverEInstance);
    Set<String> allSegments = new HashSet<>(STRICT_SEGMENTS);
    allSegments.add(segExtra);
    StrictReplicaGroupInstanceSelector instanceSelector =
        (StrictReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig,
            mock(ZkHelixPropertyStore.class), mock(BrokerMetrics.class), hybridSelector,
            new PinotConfiguration(Map.of()),
            Set.of(STRICT_RG0_SERVER_A, STRICT_RG0_SERVER_B,
                STRICT_RG1_SERVER_C, STRICT_RG1_SERVER_D, rg1ServerE),
            serverMap, idealState, externalView, allSegments);

    // server_e has a much worse mocked score but hosts no query segments, so it should never be ranked.
    when(hybridSelector.fetchServerRankingsWithScores(any())).thenReturn(Arrays.asList(
        new ImmutablePair<>(STRICT_RG1_SERVER_C, 1.0),   // rank 0
        new ImmutablePair<>(STRICT_RG1_SERVER_D, 2.0),   // rank 1
        new ImmutablePair<>(STRICT_RG0_SERVER_A, 3.0),   // rank 2
        new ImmutablePair<>(STRICT_RG0_SERVER_B, 4.0),   // rank 3
        new ImmutablePair<>(rg1ServerE, 99.0)        // rank 4 (not query-relevant)
    ));

    Map<String, List<SegmentInstanceCandidate>> candidatesMap = new HashMap<>();
    candidatesMap.put(STRICT_SEGMENT0,
        buildStrictReplicaGroupCandidates(STRICT_RG0_SERVER_A, STRICT_RG1_SERVER_C));
    candidatesMap.put(STRICT_SEGMENT1,
        buildStrictReplicaGroupCandidates(STRICT_RG0_SERVER_A, STRICT_RG1_SERVER_C));
    candidatesMap.put(STRICT_SEGMENT2,
        buildStrictReplicaGroupCandidates(STRICT_RG0_SERVER_B, STRICT_RG1_SERVER_D));
    candidatesMap.put(segExtra, Arrays.asList(
        new SegmentInstanceCandidate(STRICT_RG1_SERVER_D, true, FALLBACK_POOL_ID, 0),
        new SegmentInstanceCandidate(rg1ServerE, true, 99, 1)));
    SegmentStates segmentStates = new SegmentStates(candidatesMap, allSegments, null);

    InstanceSelector.InstanceMapping result =
        instanceSelector.select(STRICT_SEGMENTS, 0, segmentStates, null);

    // Replica group 1 wins (worst rank among query-relevant servers = 1) over replica group 0 (worst = 3)
    // server_e is not considered because it hosts no query segments
    assertEquals(result.segmentToInstanceMap(), Map.of(
        STRICT_SEGMENT0, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT1, STRICT_RG1_SERVER_C,
        STRICT_SEGMENT2, STRICT_RG1_SERVER_D));

    ArgumentCaptor<List<String>> rankedCandidatesCaptor = ArgumentCaptor.forClass(List.class);
    verify(hybridSelector).fetchServerRankingsWithScores(rankedCandidatesCaptor.capture());
    assertEquals(new HashSet<>(rankedCandidatesCaptor.getValue()),
        Set.of(STRICT_RG0_SERVER_A, STRICT_RG0_SERVER_B,
            STRICT_RG1_SERVER_C, STRICT_RG1_SERVER_D));
    assertEquals(rankedCandidatesCaptor.getValue().size(), 4);
  }
}
