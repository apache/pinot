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

import com.google.common.collect.Maps;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector.SelectionResult;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.TestClock;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.broker.routing.instanceselector.SegmentStatesTestUtils.getCandidates;
import static org.apache.pinot.broker.routing.instanceselector.SegmentStatesTestUtils.isNew;
import static org.apache.pinot.broker.routing.instanceselector.SelectionResultTestUtils.getOptionalSegmentToInstanceMap;
import static org.apache.pinot.broker.routing.instanceselector.SelectionResultTestUtils.getSegmentToInstanceMap;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.FALLBACK_POOL_ID;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


/// Differential test of the instance selector state against [ReferenceModel], a straightforward per-segment model of
/// the routing state, plus targeted tests of how segments share candidate groups and of how the state is published.
///
/// The randomized test drives real selectors through `init`, `onAssignmentChange` and `onInstancesChange` with seeded
/// random ideal states, external views, enabled instances and clock advances. After every step, it compares with the
/// model the candidates, the unavailability and the new segment classification of every segment, which segments share
/// a candidate group, the serving instances, the replica health and the selection of all the segments. Every run is
/// reproducible from its seed, and checks that it reaches the [Scenario]s the comparison relies on.
public class SegmentStatesReferenceModelTest {
  private static final String TABLE_NAME = "testTable_REALTIME";
  private static final long NEW_SEGMENT_EXPIRATION_SECONDS = 300;
  private static final long NEW_SEGMENT_EXPIRATION_MS = NEW_SEGMENT_EXPIRATION_SECONDS * 1000;
  private static final InstanceSelectorConfig INSTANCE_SELECTOR_CONFIG =
      new InstanceSelectorConfig(false, NEW_SEGMENT_EXPIRATION_SECONDS, false);
  private static final long START_TIME_MS = 1_800_000_000_000L;
  private static final long RECENT_CREATION_TIME_MS = START_TIME_MS - 1000;
  private static final long EXPIRED_CREATION_TIME_MS = START_TIME_MS - NEW_SEGMENT_EXPIRATION_MS - 1;
  private static final long[] SEEDS = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89};
  private static final int NUM_STEPS = 30;
  private static final int MAX_CLOCK_ADVANCE_MS = 120_000;

  private static final String SERVER0 = "server0";
  private static final String SERVER1 = "server1";
  private static final String SERVER2 = "server2";
  private static final String SERVER3 = "server3";
  private static final String SERVER4 = "server4";
  private static final List<String> SERVERS = List.of(SERVER0, SERVER1, SERVER2, SERVER3, SERVER4);
  // Always enabled, never assigned
  private static final String UNASSIGNED_SERVER = "server5";
  // SERVER4 is missing from the server map, so it gets the fallback pool
  private static final Map<String, Integer> POOLS = Map.of(
      SERVER0, 0,
      SERVER1, 1,
      SERVER2, 0,
      SERVER3, 1
  );
  // Instances of the ideal state of a random segment, unless it gets a random subset of the servers
  private static final List<List<String>> MIRROR_SETS = List.of(
      List.of(SERVER0, SERVER1, SERVER2),
      List.of(SERVER2, SERVER3, SERVER4),
      List.of(SERVER1, SERVER3)
  );

  private static final String SEGMENT0 = "segment0";
  private static final String SEGMENT1 = "segment1";
  private static final String SEGMENT2 = "segment2";
  private static final List<String> SEGMENTS = IntStream.range(0, 16).mapToObj(i -> "segment" + i).toList();
  private static final Assignment EMPTY_ASSIGNMENT = new Assignment(Map.of(), Map.of(), Set.of());

  @DataProvider(name = "seedsAndSelectorKinds")
  public Object[][] seedsAndSelectorKinds() {
    SelectorKind[] kinds = SelectorKind.values();
    Object[][] parameters = new Object[SEEDS.length * kinds.length][];
    int index = 0;
    for (long seed : SEEDS) {
      for (SelectorKind kind : kinds) {
        parameters[index++] = new Object[]{seed, kind};
      }
    }
    return parameters;
  }

  @DataProvider(name = "selectorKinds")
  public Object[] selectorKinds() {
    return SelectorKind.values();
  }

  @DataProvider(name = "strictSelectorKinds")
  public Object[] strictSelectorKinds() {
    return new Object[]{SelectorKind.STRICT_REPLICA_GROUP, SelectorKind.UPSERT_REPLICA_GROUP};
  }

  @Test(dataProvider = "seedsAndSelectorKinds")
  public void testMatchesReferenceModel(long seed, SelectorKind kind) {
    String runContext = kind + ", seed: " + seed;
    Random random = new Random(seed);
    TestClock clock = new TestClock(Instant.ofEpochMilli(START_TIME_MS), ZoneOffset.UTC);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    Set<Scenario> reachedScenarios = EnumSet.noneOf(Scenario.class);
    Assignment assignment = nextAssignment(random, EMPTY_ASSIGNMENT);
    Map<String, Long> creationTimeMsMap = randomCreationTimes(random, assignment.idealState().keySet(), clock.millis());
    Set<String> enabledInstances = randomEnabledInstances(random);
    BaseInstanceSelector selector = createSelector(kind, clock, enabledInstances, assignment, creationTimeMsMap);
    ReferenceModel model = new ReferenceModel(kind._strict);
    model.init(assignment, creationTimeMsMap, clock.millis());
    assertMatchesModel(selector, model, enabledInstances, random, brokerRequest, runContext + ", init",
        reachedScenarios);

    for (int step = 1; step <= NUM_STEPS; step++) {
      String context = runContext + ", step: " + step;
      clock.fastForward(Duration.ofMillis(random.nextInt(MAX_CLOCK_ADVANCE_MS)));
      if (random.nextInt(10) < 6) {
        assignment = nextAssignment(random, assignment);
        selector.onAssignmentChange(createIdealState(assignment), createExternalView(assignment),
            assignment.onlineSegments());
        model.onAssignmentChange(assignment, clock.millis());
      } else {
        Set<String> previousEnabledInstances = enabledInstances;
        enabledInstances = randomEnabledInstances(random);
        SegmentStates previousSegmentStates = selector._segmentStates;
        selector.onInstancesChange(enabledInstances, getChangedInstances(previousEnabledInstances, enabledInstances));
        assertRoutingReused(previousSegmentStates, selector._segmentStates, model, previousEnabledInstances,
            enabledInstances, context, reachedScenarios);
      }
      assertMatchesModel(selector, model, enabledInstances, random, brokerRequest, context, reachedScenarios);
    }

    Set<Scenario> expectedScenarios = EnumSet.allOf(Scenario.class);
    if (!kind._strict) {
      expectedScenarios.remove(Scenario.STRICT_REPLICA_GROUP_EXCLUSION);
    }
    assertEquals(reachedScenarios, expectedScenarios, runContext);
  }

  @Test(dataProvider = "selectorKinds")
  public void testSegmentsDifferingOnlyByConsumingAndOnlineShareGroup(SelectorKind kind) {
    SegmentStates segmentStates = createSelector(kind, Assignment.of(Map.of(
        SEGMENT0, Map.of(
            SERVER0, ONLINE,
            SERVER1, ONLINE
        ),
        SEGMENT1, Map.of(
            SERVER0, CONSUMING,
            SERVER1, CONSUMING
        )
    ), Map.of(
        SEGMENT0, Map.of(
            SERVER0, ONLINE,
            SERVER1, ONLINE
        ),
        SEGMENT1, Map.of(
            SERVER0, CONSUMING,
            SERVER1, ONLINE
        )
    )), Map.of())._segmentStates;
    assertEquals(segmentStates.getGroupId(SEGMENT1), segmentStates.getGroupId(SEGMENT0));
    assertEquals(toCandidates(getCandidates(segmentStates, SEGMENT0)), List.of(
        new Candidate(SERVER0, true, 0, 0),
        new Candidate(SERVER1, true, 1, 1)
    ));
  }

  @Test(dataProvider = "selectorKinds")
  public void testSegmentsWithDifferentExpectedReplicasDoNotShareGroup(SelectorKind kind) {
    BaseInstanceSelector selector = createSelector(kind, Assignment.of(Map.of(
        SEGMENT0, Map.of(
            SERVER0, ONLINE,
            SERVER1, ONLINE,
            SERVER2, OFFLINE
        ),
        SEGMENT1, Map.of(
            SERVER0, ONLINE,
            SERVER1, ONLINE,
            SERVER2, ONLINE
        )
    ), Map.of(
        SEGMENT0, Map.of(
            SERVER0, ONLINE,
            SERVER1, ONLINE
        ),
        SEGMENT1, Map.of(
            SERVER0, ONLINE,
            SERVER1, ONLINE,
            SERVER2, ERROR
        )
    )), Map.of());
    SegmentStates segmentStates = selector._segmentStates;
    List<Candidate> expectedCandidates = List.of(
        new Candidate(SERVER0, true, 0, 0),
        new Candidate(SERVER1, true, 1, 1)
    );
    assertEquals(toCandidates(getCandidates(segmentStates, SEGMENT0)), expectedCandidates);
    assertEquals(toCandidates(getCandidates(segmentStates, SEGMENT1)), expectedCandidates);
    assertNotEquals(segmentStates.getGroupId(SEGMENT1), segmentStates.getGroupId(SEGMENT0));
    // SEGMENT1 is served by 2 of its 3 expected replicas
    assertEquals(toReplicaHealth(selector.getReplicaHealth()), new ReplicaHealth(66, 1, 0));
  }

  @Test(dataProvider = "strictSelectorKinds")
  public void testOldAndNewSegmentsDoNotShareGroup(SelectorKind kind) {
    Map<String, String> idealStateMap = Map.of(
        SERVER0, ONLINE,
        SERVER1, ONLINE
    );
    Map<String, String> externalViewMap = Map.of(
        SERVER0, ONLINE,
        SERVER1, OFFLINE
    );
    // SERVER1 is unavailable for the old SEGMENT0, which excludes it from the candidates of the new SEGMENT1 as well
    SegmentStates segmentStates = createSelector(kind, Assignment.of(Map.of(
        SEGMENT0, idealStateMap,
        SEGMENT1, idealStateMap
    ), Map.of(
        SEGMENT0, externalViewMap,
        SEGMENT1, externalViewMap
    )), Map.of(
        SEGMENT0, EXPIRED_CREATION_TIME_MS,
        SEGMENT1, RECENT_CREATION_TIME_MS
    ))._segmentStates;
    List<Candidate> expectedCandidates = List.of(new Candidate(SERVER0, true, 0, 0));
    assertEquals(toCandidates(getCandidates(segmentStates, SEGMENT0)), expectedCandidates);
    assertEquals(toCandidates(getCandidates(segmentStates, SEGMENT1)), expectedCandidates);
    int oldGroupId = segmentStates.getGroupId(SEGMENT0);
    int newGroupId = segmentStates.getGroupId(SEGMENT1);
    assertNotEquals(newGroupId, oldGroupId);
    assertFalse(isNew(segmentStates, oldGroupId));
    assertTrue(isNew(segmentStates, newGroupId));
  }

  @Test(dataProvider = "selectorKinds")
  public void testSegmentsWithDifferentIdealStateReplicaIdsDoNotShareGroup(SelectorKind kind) {
    SegmentStates segmentStates = createSelector(kind, Assignment.of(Map.of(
        SEGMENT0, Map.of(
            SERVER0, ONLINE,
            SERVER1, ONLINE
        ),
        SEGMENT1, Map.of(
            SERVER1, ONLINE,
            SERVER2, ONLINE
        )
    ), Map.of(
        SEGMENT0, Map.of(
            SERVER0, ERROR,
            SERVER1, ONLINE
        ),
        SEGMENT1, Map.of(
            SERVER1, ONLINE,
            SERVER2, ERROR
        )
    )), Map.of())._segmentStates;
    assertEquals(toCandidates(getCandidates(segmentStates, SEGMENT0)), List.of(new Candidate(SERVER1, true, 1, 1)));
    assertEquals(toCandidates(getCandidates(segmentStates, SEGMENT1)), List.of(new Candidate(SERVER1, true, 1, 0)));
    assertNotEquals(segmentStates.getGroupId(SEGMENT1), segmentStates.getGroupId(SEGMENT0));
  }

  @Test(dataProvider = "selectorKinds")
  public void testInstancesChangeReusesRoutingOfUnchangedGroups(SelectorKind kind) {
    Map<String, String> firstMirrorSetMap = Map.of(
        SERVER0, ONLINE,
        SERVER1, ONLINE
    );
    Map<String, String> secondMirrorSetMap = Map.of(
        SERVER2, ONLINE,
        SERVER3, ONLINE
    );
    Map<String, Map<String, String>> assignment = Map.of(
        SEGMENT0, firstMirrorSetMap,
        SEGMENT1, firstMirrorSetMap,
        SEGMENT2, secondMirrorSetMap
    );
    BaseInstanceSelector selector = createSelector(kind, Assignment.of(assignment, assignment), Map.of());
    SegmentStates initialSegmentStates = selector._segmentStates;
    int firstGroupId = initialSegmentStates.getGroupId(SEGMENT0);
    int secondGroupId = initialSegmentStates.getGroupId(SEGMENT2);

    selector.onInstancesChange(Set.of(SERVER1, SERVER2, SERVER3, SERVER4), List.of(SERVER0));
    SegmentStates segmentStates = selector._segmentStates;
    assertSame(segmentStates._segmentGroups, initialSegmentStates._segmentGroups);
    assertSame(segmentStates.getGroupRouting(secondGroupId), initialSegmentStates.getGroupRouting(secondGroupId));
    assertEquals(toCandidates(segmentStates.getGroupRouting(firstGroupId).getCandidates()),
        List.of(new Candidate(SERVER1, true, 1, 1)));

    selector.onInstancesChange(Set.of(SERVER1, SERVER3, SERVER4), List.of(SERVER2));
    SegmentStates latestSegmentStates = selector._segmentStates;
    assertSame(latestSegmentStates._segmentGroups, initialSegmentStates._segmentGroups);
    assertSame(latestSegmentStates.getGroupRouting(firstGroupId), segmentStates.getGroupRouting(firstGroupId));
    assertEquals(toCandidates(latestSegmentStates.getGroupRouting(secondGroupId).getCandidates()),
        List.of(new Candidate(SERVER3, true, 1, 1)));
  }

  @Test(dataProvider = "selectorKinds")
  public void testOnlyOldSegmentIsUnavailableWhenAllCandidatesAreDisabled(SelectorKind kind) {
    Map<String, String> idealStateMap = Map.of(
        SERVER0, ONLINE,
        SERVER1, ONLINE
    );
    // The old SEGMENT0 and SEGMENT2 share one group
    BaseInstanceSelector selector = createSelector(kind, Assignment.of(Map.of(
        SEGMENT0, idealStateMap,
        SEGMENT1, idealStateMap,
        SEGMENT2, idealStateMap
    ), Map.of(
        SEGMENT0, idealStateMap,
        SEGMENT1, Map.of(SERVER0, ONLINE),
        SEGMENT2, idealStateMap
    )), Map.of(SEGMENT1, RECENT_CREATION_TIME_MS));
    BrokerMetrics brokerMetrics = selector._brokerMetrics;
    clearInvocations(brokerMetrics);

    selector.onInstancesChange(Set.of(SERVER2), List.of(SERVER0, SERVER1, SERVER3, SERVER4));
    SelectionResult selectionResult =
        selector.select(mock(BrokerRequest.class), List.of(SEGMENT0, SEGMENT1, SEGMENT2), 0);
    assertTrue(selectionResult.getInstanceToSegmentsMap().isEmpty());
    assertEquals(selectionResult.getUnavailableSegments(), List.of(SEGMENT0, SEGMENT2));
    assertEquals(toReplicaHealth(selector.getReplicaHealth()), new ReplicaHealth(0, 2, 2));
    ArgumentCaptor<Long> numSegmentsCaptor = ArgumentCaptor.forClass(Long.class);
    verify(brokerMetrics, atLeastOnce()).addMeteredTableValue(eq(TABLE_NAME),
        eq(BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT), numSegmentsCaptor.capture());
    assertEquals(numSegmentsCaptor.getAllValues().stream().mapToLong(Long::longValue).sum(), 2L);
  }

  @Test(dataProvider = "selectorKinds")
  public void testNullInstanceStateIsNotOnline(SelectorKind kind) {
    // SEGMENT0 and SEGMENT1 have equal maps in distinct objects, so that their maps are compared entry by entry.
    // SEGMENT2 has no entry for the instances with a null state.
    Map<String, String> idealStateMap = new HashMap<>();
    idealStateMap.put(SERVER0, ONLINE);
    idealStateMap.put(SERVER1, ONLINE);
    idealStateMap.put(SERVER2, null);
    Map<String, String> externalViewMap = new HashMap<>();
    externalViewMap.put(SERVER0, ONLINE);
    externalViewMap.put(SERVER1, null);
    Assignment assignment = Assignment.of(Map.of(
        SEGMENT0, idealStateMap,
        SEGMENT1, new HashMap<>(idealStateMap),
        SEGMENT2, Map.of(
            SERVER0, ONLINE,
            SERVER1, ONLINE
        )
    ), Map.of(
        SEGMENT0, externalViewMap,
        SEGMENT1, new HashMap<>(externalViewMap),
        SEGMENT2, Map.of(SERVER0, ONLINE)
    ));
    BaseInstanceSelector selector = createSelector(kind, assignment, Map.of());
    assertNullInstanceStateIsNotOnline(selector);
    selector.onAssignmentChange(createIdealState(assignment), createExternalView(assignment),
        assignment.onlineSegments());
    assertNullInstanceStateIsNotOnline(selector);
  }

  /// Asserts that SEGMENT0, SEGMENT1 and SEGMENT2 share one group with SERVER0 as only candidate, and are served by 1
  /// of their 2 expected replicas.
  private static void assertNullInstanceStateIsNotOnline(BaseInstanceSelector selector) {
    SegmentStates segmentStates = selector._segmentStates;
    for (String segment : List.of(SEGMENT0, SEGMENT1, SEGMENT2)) {
      assertEquals(toCandidates(getCandidates(segmentStates, segment)), List.of(new Candidate(SERVER0, true, 0, 0)));
      assertEquals(segmentStates.getGroupId(segment), segmentStates.getGroupId(SEGMENT0));
    }
    assertEquals(toReplicaHealth(selector.getReplicaHealth()), new ReplicaHealth(50, 3, 0));
  }

  @Test(dataProvider = "selectorKinds")
  public void testFailedRebuildKeepsPublishedState(SelectorKind kind) {
    Map<String, String> instanceStateMap = Map.of(
        SERVER0, ONLINE,
        SERVER1, ONLINE
    );
    Map<String, Map<String, String>> assignment = Map.of(
        SEGMENT0, instanceStateMap,
        SEGMENT1, instanceStateMap
    );
    BaseInstanceSelector selector = createSelector(kind, Assignment.of(assignment, assignment), Map.of());
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    List<String> segments = List.of(SEGMENT0, SEGMENT1);
    Map<String, String> segmentToInstanceMap = getSegmentToInstanceMap(selector.select(brokerRequest, segments, 0));

    // The rebuild fails when it looks up the pool of the instance of the new SEGMENT2
    String failingServer = "server6";
    ServerInstance failingServerInstance = mock(ServerInstance.class);
    when(failingServerInstance.getPool()).thenThrow(new IllegalStateException());
    selector._enabledServerStore.put(failingServer, failingServerInstance);
    Map<String, Map<String, String>> failingAssignmentMap = Map.of(
        SEGMENT0, instanceStateMap,
        SEGMENT1, instanceStateMap,
        SEGMENT2, Map.of(failingServer, ONLINE)
    );
    Assignment failingAssignment = Assignment.of(failingAssignmentMap, failingAssignmentMap);
    expectThrows(IllegalStateException.class, () -> selector.onAssignmentChange(createIdealState(failingAssignment),
        createExternalView(failingAssignment), failingAssignment.onlineSegments()));

    // The selector keeps the state of the previous rebuild, including when an instance change refreshes it
    selector.onInstancesChange(Set.copyOf(SERVERS), List.of());
    assertEquals(getSegmentToInstanceMap(selector.select(brokerRequest, segments, 0)), segmentToInstanceMap);
    assertEquals(selector.getServingInstances(), Set.of(SERVER0, SERVER1));
    assertEquals(toReplicaHealth(selector.getReplicaHealth()), new ReplicaHealth(100, 2, 0));
  }

  /// Creates a selector of the given kind on the given assignment at [#START_TIME_MS], with every server enabled.
  private static BaseInstanceSelector createSelector(SelectorKind kind, Assignment assignment,
      Map<String, Long> creationTimeMsMap) {
    return createSelector(kind, new TestClock(Instant.ofEpochMilli(START_TIME_MS), ZoneOffset.UTC),
        Set.copyOf(SERVERS), assignment, creationTimeMsMap);
  }

  /// Creates a selector of the given kind on the given assignment, where the segments in the given creation time map
  /// have ZK metadata with that creation time.
  private static BaseInstanceSelector createSelector(SelectorKind kind, Clock clock, Set<String> enabledInstances,
      Assignment assignment, Map<String, Long> creationTimeMsMap) {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(TABLE_NAME);
    when(tableConfig.isUpsertEnabled()).thenReturn(kind._upsert);
    BaseInstanceSelector selector = kind._selectorSupplier.get();
    selector.init(tableConfig, createPropertyStore(creationTimeMsMap), mock(BrokerMetrics.class), null, clock,
        INSTANCE_SELECTOR_CONFIG, enabledInstances, createServerMap(), createIdealState(assignment),
        createExternalView(assignment), assignment.onlineSegments());
    return selector;
  }

  private static ZkHelixPropertyStore<ZNRecord> createPropertyStore(Map<String, Long> creationTimeMsMap) {
    Map<String, ZNRecord> recordsByPath = new HashMap<>();
    for (Map.Entry<String, Long> entry : creationTimeMsMap.entrySet()) {
      String segment = entry.getKey();
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
      segmentZKMetadata.setCreationTime(entry.getValue());
      recordsByPath.put(ZKMetadataProvider.constructPropertyStorePathForSegment(TABLE_NAME, segment),
          segmentZKMetadata.toZNRecord());
    }
    //noinspection unchecked
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.get(anyList(), any(), anyInt(), anyBoolean())).thenAnswer(invocation -> {
      List<String> paths = invocation.getArgument(0);
      List<ZNRecord> records = new ArrayList<>(paths.size());
      for (String path : paths) {
        records.add(recordsByPath.get(path));
      }
      return records;
    });
    return propertyStore;
  }

  private static Map<String, ServerInstance> createServerMap() {
    Map<String, ServerInstance> serverMap = Maps.newHashMapWithExpectedSize(POOLS.size());
    for (Map.Entry<String, Integer> entry : POOLS.entrySet()) {
      ServerInstance serverInstance = mock(ServerInstance.class);
      when(serverInstance.getPool()).thenReturn(entry.getValue());
      serverMap.put(entry.getKey(), serverInstance);
    }
    return serverMap;
  }

  private static IdealState createIdealState(Assignment assignment) {
    IdealState idealState = new IdealState(TABLE_NAME);
    idealState.getRecord().getMapFields().putAll(assignment.idealState());
    return idealState;
  }

  private static ExternalView createExternalView(Assignment assignment) {
    ExternalView externalView = new ExternalView(TABLE_NAME);
    externalView.getRecord().getMapFields().putAll(assignment.externalView());
    return externalView;
  }

  /// Returns the next random assignment. Each segment of the given assignment is removed, kept with the same instance
  /// state maps, given a new external view, given a new ideal state (the external view not caught up yet) or given new
  /// instance state maps, and each absent segment may be added with new instance state maps. Every instance state map
  /// iterates in a random order.
  private static Assignment nextAssignment(Random random, Assignment previous) {
    int addPercent = previous.idealState().isEmpty() ? 80 : 30;
    Map<String, Map<String, String>> idealState = new TreeMap<>();
    Map<String, Map<String, String>> externalView = new TreeMap<>();
    for (String segment : SEGMENTS) {
      Map<String, String> previousIdealStateMap = previous.idealState().get(segment);
      if (previousIdealStateMap == null) {
        if (random.nextInt(100) < addPercent) {
          putRandomMaps(random, segment, idealState, externalView);
        }
        continue;
      }
      Map<String, String> previousExternalViewMap = previous.externalView().get(segment);
      int action = random.nextInt(10);
      if (action == 0) {
        // The segment is removed
        continue;
      }
      if (action < 5) {
        idealState.put(segment, inRandomOrder(random, previousIdealStateMap));
        if (previousExternalViewMap != null) {
          externalView.put(segment, inRandomOrder(random, previousExternalViewMap));
        }
      } else if (action < 8) {
        idealState.put(segment, inRandomOrder(random, previousIdealStateMap));
        Map<String, String> externalViewMap = randomExternalViewMap(random, previousIdealStateMap);
        if (externalViewMap != null) {
          externalView.put(segment, externalViewMap);
        }
      } else if (action == 8) {
        idealState.put(segment, randomIdealStateMap(random));
        if (previousExternalViewMap != null) {
          externalView.put(segment, inRandomOrder(random, previousExternalViewMap));
        }
      } else {
        putRandomMaps(random, segment, idealState, externalView);
      }
    }
    return new Assignment(idealState, externalView, selectOnlineSegments(random, idealState));
  }

  /// Puts new instance state maps for the given segment: 40% of the time the maps of another segment, so that segments
  /// share maps, otherwise random maps.
  private static void putRandomMaps(Random random, String segment, Map<String, Map<String, String>> idealState,
      Map<String, Map<String, String>> externalView) {
    if (!idealState.isEmpty() && random.nextInt(10) < 4) {
      List<String> otherSegments = new ArrayList<>(idealState.keySet());
      String otherSegment = otherSegments.get(random.nextInt(otherSegments.size()));
      idealState.put(segment, inRandomOrder(random, idealState.get(otherSegment)));
      Map<String, String> otherExternalViewMap = externalView.get(otherSegment);
      if (otherExternalViewMap != null) {
        externalView.put(segment, inRandomOrder(random, otherExternalViewMap));
      }
    } else {
      Map<String, String> idealStateMap = randomIdealStateMap(random);
      idealState.put(segment, idealStateMap);
      Map<String, String> externalViewMap = randomExternalViewMap(random, idealStateMap);
      if (externalViewMap != null) {
        externalView.put(segment, externalViewMap);
      }
    }
  }

  private static Map<String, String> randomIdealStateMap(Random random) {
    List<String> instances;
    if (random.nextBoolean()) {
      instances = MIRROR_SETS.get(random.nextInt(MIRROR_SETS.size()));
    } else {
      List<String> servers = new ArrayList<>(SERVERS);
      Collections.shuffle(servers, random);
      instances = servers.subList(0, 1 + random.nextInt(3));
    }
    Map<String, String> idealStateMap = Maps.newHashMapWithExpectedSize(instances.size());
    for (String instance : instances) {
      idealStateMap.put(instance, randomIdealStateState(random));
    }
    return inRandomOrder(random, idealStateMap);
  }

  private static String randomIdealStateState(Random random) {
    int value = random.nextInt(20);
    if (value < 13) {
      return ONLINE;
    }
    if (value < 17) {
      return CONSUMING;
    }
    return OFFLINE;
  }

  /// Returns a random external view instance state map for the given ideal state instance state map: missing 10% of
  /// the time, converged with the ideal state 45% of the time, otherwise with random states, missing instances, and
  /// sometimes an instance that is not in the ideal state.
  @Nullable
  private static Map<String, String> randomExternalViewMap(Random random, Map<String, String> idealStateMap) {
    int value = random.nextInt(20);
    if (value < 2) {
      return null;
    }
    Map<String, String> externalViewMap = new HashMap<>();
    if (value < 11) {
      for (Map.Entry<String, String> entry : idealStateMap.entrySet()) {
        if (!entry.getValue().equals(OFFLINE)) {
          externalViewMap.put(entry.getKey(), entry.getValue());
        }
      }
    } else {
      for (String instance : idealStateMap.keySet()) {
        String state = randomExternalViewState(random);
        if (state != null) {
          externalViewMap.put(instance, state);
        }
      }
      if (random.nextInt(5) == 0) {
        String instance = SERVERS.get(random.nextInt(SERVERS.size()));
        if (!idealStateMap.containsKey(instance)) {
          externalViewMap.put(instance, ONLINE);
        }
      }
    }
    return inRandomOrder(random, externalViewMap);
  }

  /// Returns a random external view state, or `null` for an instance missing from the external view.
  @Nullable
  private static String randomExternalViewState(Random random) {
    int value = random.nextInt(20);
    if (value < 10) {
      return ONLINE;
    }
    if (value < 13) {
      return CONSUMING;
    }
    if (value < 15) {
      return OFFLINE;
    }
    if (value < 17) {
      return ERROR;
    }
    return null;
  }

  /// Returns a copy of the given map that iterates its entries in a random order.
  private static Map<String, String> inRandomOrder(Random random, Map<String, String> map) {
    List<String> keys = new ArrayList<>(map.keySet());
    Collections.shuffle(keys, random);
    Map<String, String> shuffledMap = Maps.newLinkedHashMapWithExpectedSize(keys.size());
    for (String key : keys) {
      shuffledMap.put(key, map.get(key));
    }
    return shuffledMap;
  }

  /// Returns the segments with an ONLINE/CONSUMING instance in the ideal state, except a few of them excluded as by a
  /// segment pre-selector.
  private static Set<String> selectOnlineSegments(Random random, Map<String, Map<String, String>> idealState) {
    Set<String> onlineSegments = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : idealState.entrySet()) {
      Map<String, String> idealStateMap = entry.getValue();
      if ((idealStateMap.containsValue(ONLINE) || idealStateMap.containsValue(CONSUMING)) && random.nextInt(20) != 0) {
        onlineSegments.add(entry.getKey());
      }
    }
    return onlineSegments;
  }

  /// Returns a ZK metadata creation time for each of the given segments, half of them within the new segment
  /// expiration and half of them expired.
  private static Map<String, Long> randomCreationTimes(Random random, Set<String> segments, long currentTimeMs) {
    Map<String, Long> creationTimeMsMap = Maps.newHashMapWithExpectedSize(segments.size());
    for (String segment : segments) {
      if (random.nextBoolean()) {
        creationTimeMsMap.put(segment, currentTimeMs - random.nextInt((int) NEW_SEGMENT_EXPIRATION_MS + 1));
      } else {
        creationTimeMsMap.put(segment, currentTimeMs - NEW_SEGMENT_EXPIRATION_MS - 1 - random.nextInt(1_000_000));
      }
    }
    return creationTimeMsMap;
  }

  private static Set<String> randomEnabledInstances(Random random) {
    Set<String> enabledInstances = new HashSet<>();
    for (String server : SERVERS) {
      if (random.nextInt(5) != 0) {
        enabledInstances.add(server);
      }
    }
    enabledInstances.add(UNASSIGNED_SERVER);
    return enabledInstances;
  }

  private static List<String> getChangedInstances(Set<String> previousEnabledInstances, Set<String> enabledInstances) {
    List<String> changedInstances = new ArrayList<>();
    for (String server : SERVERS) {
      if (previousEnabledInstances.contains(server) != enabledInstances.contains(server)) {
        changedInstances.add(server);
      }
    }
    return changedInstances;
  }

  /// Asserts that the published state of the given selector matches the given model under the given enabled
  /// instances, then that the selection of all the segments, in a random order, matches the model. Records the
  /// scenarios the state reaches.
  private static void assertMatchesModel(BaseInstanceSelector selector, ReferenceModel model,
      Set<String> enabledInstances, Random random, BrokerRequest brokerRequest, String context,
      Set<Scenario> reachedScenarios) {
    SegmentStates segmentStates = selector._segmentStates;
    for (String segment : SEGMENTS) {
      String segmentContext = context + ", segment: " + segment;
      int groupId = segmentStates.getGroupId(segment);
      if (!model.isTracked(segment)) {
        assertEquals(groupId, SegmentStates.UNKNOWN_SEGMENT, segmentContext);
        continue;
      }
      assertNotEquals(groupId, SegmentStates.UNKNOWN_SEGMENT, segmentContext);
      assertEquals(toCandidates(getCandidates(segmentStates, segment)),
          model.getEnabledCandidates(segment, enabledInstances), segmentContext);
      assertEquals(isNew(segmentStates, groupId), model.isNew(segment), segmentContext);
      assertEquals(segmentStates.isUnavailable(groupId), model.isUnavailable(segment, enabledInstances),
          segmentContext);
      if (isNew(segmentStates, groupId)) {
        reachedScenarios.add(Scenario.NEW_SEGMENT);
      }
      if (segmentStates.isUnavailable(groupId)) {
        reachedScenarios.add(Scenario.UNAVAILABLE_SEGMENT);
      }
      if (model.hasExcludedInstance(segment)) {
        reachedScenarios.add(Scenario.STRICT_REPLICA_GROUP_EXCLUSION);
      }
    }

    List<String> trackedSegments = model.getTrackedSegments();
    int numTrackedSegments = trackedSegments.size();
    for (int i = 0; i < numTrackedSegments; i++) {
      String segment1 = trackedSegments.get(i);
      for (int j = i + 1; j < numTrackedSegments; j++) {
        String segment2 = trackedSegments.get(j);
        boolean sameGroup = segmentStates.getGroupId(segment1) == segmentStates.getGroupId(segment2);
        assertEquals(sameGroup, model.getGroupKey(segment1).equals(model.getGroupKey(segment2)),
            context + ", segments: " + segment1 + ", " + segment2);
        if (sameGroup) {
          reachedScenarios.add(Scenario.SHARED_GROUP);
        }
      }
    }

    assertEquals(selector.getServingInstances(), model.getServingInstances(enabledInstances), context);
    assertEquals(toReplicaHealth(selector.getReplicaHealth()), model.getReplicaHealth(enabledInstances), context);

    List<String> segments = new ArrayList<>(SEGMENTS);
    Collections.shuffle(segments, random);
    assertSelectionMatchesModel(selector.select(brokerRequest, segments, random.nextInt(1000)), segments, model,
        enabledInstances, context);
  }

  /// Asserts that the given selection of the given segments selects every segment with an enabled candidate on one of
  /// them, as an optional segment exactly when that candidate is not online, keeping the order of the given segments
  /// on each instance, and reports the old segments without enabled candidate as unavailable.
  private static void assertSelectionMatchesModel(SelectionResult selectionResult, List<String> segments,
      ReferenceModel model, Set<String> enabledInstances, String context) {
    List<String> expectedUnavailableSegments = new ArrayList<>();
    Set<String> expectedSelectedSegments = new HashSet<>();
    for (String segment : segments) {
      if (model.isTracked(segment)) {
        if (model.getEnabledCandidates(segment, enabledInstances) != null) {
          expectedSelectedSegments.add(segment);
        } else if (!model.isNew(segment)) {
          expectedUnavailableSegments.add(segment);
        }
      }
    }
    assertEquals(selectionResult.getUnavailableSegments(), expectedUnavailableSegments, context);

    Map<String, String> segmentToInstanceMap = getSegmentToInstanceMap(selectionResult);
    Map<String, String> optionalSegmentToInstanceMap = getOptionalSegmentToInstanceMap(selectionResult);
    Set<String> selectedSegments = new HashSet<>(segmentToInstanceMap.keySet());
    selectedSegments.addAll(optionalSegmentToInstanceMap.keySet());
    assertEquals(selectedSegments, expectedSelectedSegments, context);
    for (Map.Entry<String, String> entry : segmentToInstanceMap.entrySet()) {
      String segment = entry.getKey();
      Candidate candidate = findCandidate(model.getEnabledCandidates(segment, enabledInstances), entry.getValue());
      assertNotNull(candidate, context + ", segment: " + segment);
      assertTrue(candidate.online(), context + ", segment: " + segment);
    }
    for (Map.Entry<String, String> entry : optionalSegmentToInstanceMap.entrySet()) {
      String segment = entry.getKey();
      Candidate candidate = findCandidate(model.getEnabledCandidates(segment, enabledInstances), entry.getValue());
      assertNotNull(candidate, context + ", segment: " + segment);
      assertFalse(candidate.online(), context + ", segment: " + segment);
    }

    Map<String, Integer> queryIndexes = Maps.newHashMapWithExpectedSize(segments.size());
    for (int i = 0; i < segments.size(); i++) {
      queryIndexes.put(segments.get(i), i);
    }
    for (SegmentsToQuery segmentsToQuery : selectionResult.getInstanceToSegmentsMap().values()) {
      assertInQueryOrder(segmentsToQuery.getSegments(), queryIndexes, context);
      assertInQueryOrder(segmentsToQuery.getOptionalSegments(), queryIndexes, context);
    }
  }

  private static void assertInQueryOrder(List<String> selectedSegments, Map<String, Integer> queryIndexes,
      String context) {
    int numSelectedSegments = selectedSegments.size();
    for (int i = 1; i < numSelectedSegments; i++) {
      assertTrue(queryIndexes.get(selectedSegments.get(i - 1)) < queryIndexes.get(selectedSegments.get(i)),
          context + ", segments: " + selectedSegments);
    }
  }

  @Nullable
  private static Candidate findCandidate(List<Candidate> candidates, String instance) {
    for (Candidate candidate : candidates) {
      if (candidate.instance().equals(instance)) {
        return candidate;
      }
    }
    return null;
  }

  /// Asserts that an instances change keeps the segment groups, and the routing of every group whose candidates did
  /// not change enabled state. Records [Scenario#REUSED_ROUTING] when the change keeps the routing of a group with an
  /// enabled candidate and recomputes the routing of another group.
  private static void assertRoutingReused(SegmentStates previousSegmentStates, SegmentStates segmentStates,
      ReferenceModel model, Set<String> previousEnabledInstances, Set<String> enabledInstances, String context,
      Set<Scenario> reachedScenarios) {
    assertSame(segmentStates._segmentGroups, previousSegmentStates._segmentGroups, context);
    boolean routingReused = false;
    boolean routingRecomputed = false;
    for (String segment : model.getTrackedSegments()) {
      boolean enabledStateChanged = false;
      for (Candidate candidate : model.getCandidates(segment)) {
        String instance = candidate.instance();
        enabledStateChanged |= previousEnabledInstances.contains(instance) != enabledInstances.contains(instance);
      }
      int groupId = segmentStates.getGroupId(segment);
      GroupRouting routing = segmentStates.getGroupRouting(groupId);
      GroupRouting previousRouting = previousSegmentStates.getGroupRouting(groupId);
      if (enabledStateChanged) {
        routingRecomputed |= routing != previousRouting;
      } else {
        assertSame(routing, previousRouting, context + ", segment: " + segment);
        routingReused |= routing.getNumCandidates() > 0;
      }
    }
    if (routingReused && routingRecomputed) {
      reachedScenarios.add(Scenario.REUSED_ROUTING);
    }
  }

  @Nullable
  private static List<Candidate> toCandidates(@Nullable List<SegmentInstanceCandidate> candidates) {
    return candidates != null ? candidates.stream().map(Candidate::of).toList() : null;
  }

  private static ReplicaHealth toReplicaHealth(TableReplicaHealth replicaHealth) {
    return new ReplicaHealth(replicaHealth.getMinPercentOfReplicas(),
        replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), replicaHealth.getNumUnavailableSegments());
  }

  /// The selectors under test, along with whether the table has upsert enabled and whether the candidates follow the
  /// strict replica-group rules.
  enum SelectorKind {
    BALANCED(BalancedInstanceSelector::new, false, false),
    REPLICA_GROUP(ReplicaGroupInstanceSelector::new, false, false),
    STRICT_REPLICA_GROUP(StrictReplicaGroupInstanceSelector::new, false, true),
    UPSERT_REPLICA_GROUP(ReplicaGroupInstanceSelector::new, true, true);

    private final Supplier<BaseInstanceSelector> _selectorSupplier;
    private final boolean _upsert;
    private final boolean _strict;

    SelectorKind(Supplier<BaseInstanceSelector> selectorSupplier, boolean upsert, boolean strict) {
      _selectorSupplier = selectorSupplier;
      _upsert = upsert;
      _strict = strict;
    }
  }

  /// Scenarios the randomized test relies on, recorded from the state after each step, so that a run fails when the
  /// random generators stop producing one of them. Every run reaches all of them, except
  /// [#STRICT_REPLICA_GROUP_EXCLUSION] which only the runs of the strict kinds reach.
  private enum Scenario {
    /// A tracked segment is new.
    NEW_SEGMENT,
    /// An old segment has no enabled candidate.
    UNAVAILABLE_SEGMENT,
    /// Several segments share a candidate group.
    SHARED_GROUP,
    /// The strict replica-group rules exclude an instance that would otherwise be a candidate of a segment.
    STRICT_REPLICA_GROUP_EXCLUSION,
    /// An instances change keeps the routing of a group with an enabled candidate, and recomputes the routing of
    /// another group.
    REUSED_ROUTING
  }

  /// The ideal state and external view instance state maps of the segments, and the online segments.
  private record Assignment(Map<String, Map<String, String>> idealState, Map<String, Map<String, String>> externalView,
      Set<String> onlineSegments) {
    /// Returns the assignment where every segment in the ideal state is online.
    static Assignment of(Map<String, Map<String, String>> idealState, Map<String, Map<String, String>> externalView) {
      return new Assignment(idealState, externalView, idealState.keySet());
    }
  }

  /// A candidate instance of a segment, compared by content.
  private record Candidate(String instance, boolean online, int pool, int idealStateReplicaId) {
    static Candidate of(SegmentInstanceCandidate candidate) {
      return new Candidate(candidate.getInstance(), candidate.isOnline(), candidate.getPool(),
          candidate.getIdealStateReplicaId());
    }
  }

  /// What segments must have in common to share a candidate group.
  private record GroupKey(List<Candidate> candidates, boolean newSegment, int expectedReplicas) {
  }

  /// The values of a [TableReplicaHealth], compared by content.
  private record ReplicaHealth(int minPercentOfReplicas, int numSegmentsAtMinPercentOfReplicas,
      int numUnavailableSegments) {
  }

  /// Per-segment reference model of the routing state of a table.
  ///
  /// Computes the state of every online segment from its own ideal state and external view instance state maps, without
  /// sharing anything between segments. An instance is *online* for a segment when it is ONLINE/CONSUMING in both the
  /// ideal state and the external view. The candidates of a segment follow the sorted order of its ideal state
  /// instances, and the ideal state replica id of a candidate is the index of its instance in that order, counting
  /// every ideal state instance:
  /// - Old segment: the candidates are the online instances.
  /// - New segment: the candidates are the instances ONLINE/CONSUMING in the ideal state, flagged online when they are
  ///   online.
  /// - Strict replica-group routing: an instance that is not online for an old segment is unavailable for every segment
  ///   with the same ideal state instances. The candidates of an old segment are its online instances that are not
  ///   unavailable, and the candidates of a new segment are its ideal state instances that are not unavailable, flagged
  ///   online when they are online.
  ///
  /// An old segment expects one replica per instance ONLINE/CONSUMING in the ideal state (at least 1), and is
  /// unavailable when none of its candidates is enabled. The replica health measures the old segments expecting at
  /// least 2 replicas, and counts the unavailable ones. A new segment is never unavailable, and never measured.
  ///
  /// A segment is new when it is not settled and its creation time is within the expiration. A segment with an external
  /// view is settled when an instance ONLINE/CONSUMING in the ideal state is in ERROR in the external view, or when
  /// every such instance is ONLINE/CONSUMING in the external view. On initialization, the creation time comes from the
  /// ZK metadata. On assignment change, a segment seen for the first time is created at the current time, a new segment
  /// keeps its creation time, and an old segment stays old.
  private static final class ReferenceModel {
    private static final int MIN_MEASURED_REPLICAS = 2;

    private final boolean _strict;
    // Candidates (enabled or not) of every tracked segment, i.e. every online segment of the last assignment
    private final Map<String, List<Candidate>> _candidatesMap = new HashMap<>();
    // Expected replicas of every tracked segment, 0 for new segments
    private final Map<String, Integer> _expectedReplicasMap = new HashMap<>();
    private final Map<String, Long> _newSegmentCreationTimeMsMap = new HashMap<>();
    // Tracked segments of which the strict replica-group rules exclude an instance that would otherwise be a candidate
    private final Set<String> _segmentsWithExcludedInstance = new HashSet<>();

    ReferenceModel(boolean strict) {
      _strict = strict;
    }

    void init(Assignment assignment, Map<String, Long> zkCreationTimeMsMap, long currentTimeMs) {
      Map<String, Long> newSegmentCreationTimeMsMap = new HashMap<>();
      for (String segment : assignment.onlineSegments()) {
        Long creationTimeMs = zkCreationTimeMsMap.get(segment);
        if (creationTimeMs != null && isNewSegment(assignment, segment, creationTimeMs, currentTimeMs)) {
          newSegmentCreationTimeMsMap.put(segment, creationTimeMs);
        }
      }
      rebuild(assignment, newSegmentCreationTimeMsMap);
    }

    void onAssignmentChange(Assignment assignment, long currentTimeMs) {
      Map<String, Long> newSegmentCreationTimeMsMap = new HashMap<>();
      for (String segment : assignment.onlineSegments()) {
        Long creationTimeMs;
        if (_candidatesMap.containsKey(segment)) {
          creationTimeMs = _newSegmentCreationTimeMsMap.get(segment);
        } else {
          creationTimeMs = currentTimeMs;
        }
        if (creationTimeMs != null && isNewSegment(assignment, segment, creationTimeMs, currentTimeMs)) {
          newSegmentCreationTimeMsMap.put(segment, creationTimeMs);
        }
      }
      rebuild(assignment, newSegmentCreationTimeMsMap);
    }

    private static boolean isNewSegment(Assignment assignment, String segment, long creationTimeMs,
        long currentTimeMs) {
      return creationTimeMs > 0
          && currentTimeMs - creationTimeMs <= NEW_SEGMENT_EXPIRATION_MS
          && !isSettled(assignment, segment);
    }

    private static boolean isSettled(Assignment assignment, String segment) {
      Map<String, String> externalViewMap = assignment.externalView().get(segment);
      if (externalViewMap == null) {
        return false;
      }
      boolean anyError = false;
      boolean allOnline = true;
      for (Map.Entry<String, String> entry : assignment.idealState().get(segment).entrySet()) {
        if (isOnlineState(entry.getValue())) {
          String externalViewState = externalViewMap.get(entry.getKey());
          anyError |= ERROR.equals(externalViewState);
          allOnline &= isOnlineState(externalViewState);
        }
      }
      return anyError || allOnline;
    }

    private static boolean isOnline(Assignment assignment, String segment, String instance) {
      Map<String, String> externalViewMap = assignment.externalView().get(segment);
      return isOnlineState(assignment.idealState().get(segment).get(instance))
          && externalViewMap != null
          && isOnlineState(externalViewMap.get(instance));
    }

    private static boolean isOnlineState(@Nullable String state) {
      return ONLINE.equals(state) || CONSUMING.equals(state);
    }

    private void rebuild(Assignment assignment, Map<String, Long> newSegmentCreationTimeMsMap) {
      Map<Set<String>, Set<String>> unavailableInstancesMap = new HashMap<>();
      if (_strict) {
        for (String segment : assignment.onlineSegments()) {
          if (!newSegmentCreationTimeMsMap.containsKey(segment)) {
            Set<String> instances = assignment.idealState().get(segment).keySet();
            Set<String> unavailableInstances =
                unavailableInstancesMap.computeIfAbsent(Set.copyOf(instances), k -> new HashSet<>());
            for (String instance : instances) {
              if (!isOnline(assignment, segment, instance)) {
                unavailableInstances.add(instance);
              }
            }
          }
        }
      }

      _candidatesMap.clear();
      _expectedReplicasMap.clear();
      _newSegmentCreationTimeMsMap.clear();
      _newSegmentCreationTimeMsMap.putAll(newSegmentCreationTimeMsMap);
      _segmentsWithExcludedInstance.clear();
      for (String segment : assignment.onlineSegments()) {
        boolean newSegment = newSegmentCreationTimeMsMap.containsKey(segment);
        Map<String, String> idealStateMap = assignment.idealState().get(segment);
        Set<String> unavailableInstances =
            unavailableInstancesMap.getOrDefault(Set.copyOf(idealStateMap.keySet()), Set.of());
        List<String> instances = idealStateMap.keySet().stream().sorted().toList();
        List<Candidate> candidates = new ArrayList<>();
        int numAssignedReplicas = 0;
        for (int replicaId = 0; replicaId < instances.size(); replicaId++) {
          String instance = instances.get(replicaId);
          boolean assigned = isOnlineState(idealStateMap.get(instance));
          boolean online = isOnline(assignment, segment, instance);
          if (assigned) {
            numAssignedReplicas++;
          }
          boolean isCandidate;
          if (_strict) {
            isCandidate = newSegment || online;
            if (isCandidate && unavailableInstances.contains(instance)) {
              isCandidate = false;
              _segmentsWithExcludedInstance.add(segment);
            }
          } else {
            isCandidate = newSegment ? assigned : online;
          }
          if (isCandidate) {
            candidates.add(new Candidate(instance, online, POOLS.getOrDefault(instance, FALLBACK_POOL_ID), replicaId));
          }
        }
        _candidatesMap.put(segment, candidates);
        _expectedReplicasMap.put(segment, newSegment ? 0 : Math.max(1, numAssignedReplicas));
      }
    }

    List<String> getTrackedSegments() {
      return _candidatesMap.keySet().stream().sorted().toList();
    }

    boolean isTracked(String segment) {
      return _candidatesMap.containsKey(segment);
    }

    boolean isNew(String segment) {
      return _newSegmentCreationTimeMsMap.containsKey(segment);
    }

    /// Returns whether the strict replica-group rules exclude an instance that would otherwise be a candidate of the
    /// given tracked segment.
    boolean hasExcludedInstance(String segment) {
      return _segmentsWithExcludedInstance.contains(segment);
    }

    /// Returns the candidates of the given tracked segment, enabled or not.
    List<Candidate> getCandidates(String segment) {
      return _candidatesMap.get(segment);
    }

    /// Returns the enabled candidates of the given tracked segment, or `null` if none of its candidates is enabled.
    @Nullable
    List<Candidate> getEnabledCandidates(String segment, Set<String> enabledInstances) {
      List<Candidate> enabledCandidates = new ArrayList<>();
      for (Candidate candidate : _candidatesMap.get(segment)) {
        if (enabledInstances.contains(candidate.instance())) {
          enabledCandidates.add(candidate);
        }
      }
      return !enabledCandidates.isEmpty() ? enabledCandidates : null;
    }

    boolean isUnavailable(String segment, Set<String> enabledInstances) {
      return !isNew(segment) && getEnabledCandidates(segment, enabledInstances) == null;
    }

    GroupKey getGroupKey(String segment) {
      return new GroupKey(_candidatesMap.get(segment), isNew(segment), _expectedReplicasMap.get(segment));
    }

    Set<String> getServingInstances(Set<String> enabledInstances) {
      Set<String> servingInstances = new HashSet<>();
      for (List<Candidate> candidates : _candidatesMap.values()) {
        for (Candidate candidate : candidates) {
          if (enabledInstances.contains(candidate.instance())) {
            servingInstances.add(candidate.instance());
          }
        }
      }
      return servingInstances;
    }

    ReplicaHealth getReplicaHealth(Set<String> enabledInstances) {
      int minPercentOfReplicas = 100;
      int numSegmentsAtMinPercentOfReplicas = 0;
      int numUnavailableSegments = 0;
      for (String segment : _candidatesMap.keySet()) {
        if (isNew(segment)) {
          continue;
        }
        List<Candidate> enabledCandidates = getEnabledCandidates(segment, enabledInstances);
        int numServingReplicas = enabledCandidates != null ? enabledCandidates.size() : 0;
        int expectedReplicas = _expectedReplicasMap.get(segment);
        if (expectedReplicas >= MIN_MEASURED_REPLICAS) {
          int percentOfReplicas = Math.min(100, numServingReplicas * 100 / expectedReplicas);
          if (percentOfReplicas < minPercentOfReplicas) {
            minPercentOfReplicas = percentOfReplicas;
            numSegmentsAtMinPercentOfReplicas = 1;
          } else if (percentOfReplicas == minPercentOfReplicas) {
            numSegmentsAtMinPercentOfReplicas++;
          }
        }
        if (numServingReplicas == 0) {
          numUnavailableSegments++;
        }
      }
      return new ReplicaHealth(minPercentOfReplicas, numSegmentsAtMinPercentOfReplicas, numUnavailableSegments);
    }
  }
}
