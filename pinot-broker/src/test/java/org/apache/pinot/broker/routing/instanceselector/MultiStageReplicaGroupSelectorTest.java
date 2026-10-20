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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.FailOpenPolicy;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.UploadedRealtimeSegmentName;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_ENABLE_FAIL_OPEN;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.USE_FIXED_REPLICA;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


public class MultiStageReplicaGroupSelectorTest {
  private static final String TABLE_NAME = "testTable_REALTIME";
  private final static List<String> SEGMENTS =
      Arrays.asList("segment0", "segment1", "segment2", "segment3", "segment4", "segment5", "segment6", "segment7",
          "segment8", "segment9", "segment10", "segment11");

  private static final Map<String, ServerInstance> EMPTY_SERVER_MAP = Map.of();

  private static final InstanceSelectorConfig INSTANCE_SELECTOR_CONFIG = new InstanceSelectorConfig(false, 300, false);
  private AutoCloseable _mocks;
  @Mock
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  @Mock
  private BrokerMetrics _brokerMetrics;
  @Mock
  private BrokerRequest _brokerRequest;
  @Mock
  private PinotQuery _pinotQuery;
  @Mock
  private TableConfig _tableConfig;

  private static List<String> getSegments() {
    return SEGMENTS;
  }

  private static LLCSegmentName getLLCSegmentName(long epochMillis) {
    return new LLCSegmentName(TABLE_NAME, 1 /* partitionGroup */, 2 /* seqNum */, epochMillis);
  }

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_brokerRequest.getPinotQuery()).thenReturn(_pinotQuery);
    when(_pinotQuery.getQueryOptions()).thenReturn(null);
    when(_tableConfig.getTableName()).thenReturn(TABLE_NAME);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    _mocks.close();
  }

  @Test
  public void testBasicReplicaGroupSelection() {
    // Create instance-partitions with two replica-groups and 1 partition. Each replica-group has 2 instances.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    InstancePartitions instancePartitions = createInstancePartitions(replicaGroup0, replicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    List<String> enabledInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();

    setupBasicTestEnvironment(enabledInstances, idealState, externalView, onlineSegments);
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    // Using requestId=0 should select replica-group 0. Even segments get assigned to instance-0 and odd segments get
    // assigned to instance-1.
    Map<String, String> expectedSelectorResult = createExpectedAssignment(replicaGroup0, getSegments());
    InstanceSelector.SelectionResult selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Using same requestId again should return the same selection
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Using requestId=1 should select replica-group 1
    expectedSelectorResult = createExpectedAssignment(replicaGroup1, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Add a new LLC segment to the list of segments but don't update segment states. The selection result should
    // remain the same.
    List<String> segments = new ArrayList<>(getSegments());
    LLCSegmentName newSegmentName = getLLCSegmentName(System.currentTimeMillis());
    segments.add(newSegmentName.getSegmentName());
    selectionResult = multiStageSelector.select(_brokerRequest, segments, 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Test with an LLC segment that is older than the new segment expiration time.
    long delta = multiStageSelector._newSegmentExpirationTimeInSeconds * 1000 + 10_000;
    long expiredSegmentEpochMs = System.currentTimeMillis() - delta;
    segments.set(segments.size() - 1, getLLCSegmentName(expiredSegmentEpochMs).getSegmentName());
    try {
      multiStageSelector.select(_brokerRequest, segments, 1);
      fail("call should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(segments.get(segments.size() - 1)));
    }

    // Test with a non LLC segment, like UploadedRealtimeSegment, that has just been created. Since it is not present
    // in segmentState, it should throw.
    segments.set(segments.size() - 1, new UploadedRealtimeSegmentName(TABLE_NAME, 1 /* partition */,
        System.currentTimeMillis(), "someprefix", "somesuffix").getSegmentName());
    try {
      multiStageSelector.select(_brokerRequest, segments, 1);
      fail("call should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(segments.get(segments.size() - 1)));
    }
  }

  @Test
  public void testInstanceFailureHandling() {
    // Create instance-partitions with two replica-groups and 1 partition. Each replica-group has 2 instances.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    InstancePartitions instancePartitions = createInstancePartitions(replicaGroup0, replicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    List<String> enabledInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();

    setupBasicTestEnvironment(enabledInstances, idealState, externalView, onlineSegments);
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    // If instance-0 is down, replica-group 1 should be picked even with requestId=0
    enabledInstances.remove("instance-0");
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
    Map<String, String> expectedSelectorResult = createExpectedAssignment(replicaGroup1, getSegments());
    InstanceSelector.SelectionResult selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // If instance-2 also goes down, no replica-group is eligible
    enabledInstances.remove("instance-2");
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
    try {
      multiStageSelector.select(_brokerRequest, getSegments(), 0);
      fail("Method call above should have failed");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testFailOpenPreservesMultiStageReplicaGroupSelection() {
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    Map<String, ServerInstance> enabledServerMap = createServerMap(instances);
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.copyOf(instances));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of(), enabledServerMap, idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult firstResult = selector.select(_brokerRequest, getSegments(), 0);
    InstanceSelector.SelectionResult secondResult = selector.select(_brokerRequest, getSegments(), 1);

    assertEquals(firstResult.getSegmentToInstanceMap(), createExpectedAssignment(replicaGroup0, getSegments()));
    assertEquals(secondResult.getSegmentToInstanceMap(), createExpectedAssignment(replicaGroup1, getSegments()));
    assertTrue(firstResult.getUnavailableSegments().isEmpty());
    assertTrue(secondResult.getUnavailableSegments().isEmpty());
    // Both select() calls above found a fail-open candidate.
    verify(_brokerMetrics, times(2)).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
    verify(failureDetector, times(2)).getUnhealthyServers();
    verify(failureDetector, never()).markServerHealthy(anyString(), any());
    verify(failureDetector, never()).markServerUnhealthy(anyString(), any());
  }

  @Test
  public void testSelectForFailOpenUsesInstancePartitionsSnapshotFromPrecedingSelect() {
    // selectForFailOpen() must reuse the InstancePartitions snapshot that the preceding select() call on the same
    // thread used, rather than re-reading the (possibly since-updated) _instancePartitions field. Simulate a
    // concurrent onAssignmentChange() publishing a brand-new snapshot, with completely disjoint instances, in
    // between the two calls: if selectForFailOpen() picked up the new snapshot, none of the real candidate instances
    // (instance-0..3) would map to a partition in it, and the resulting assignment would be empty.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    InstancePartitions snapshotA = createInstancePartitions(replicaGroup0, replicaGroup1);
    MultiStageReplicaGroupSelector selector = createMultiStageSelector(snapshotA);
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    Map<String, ServerInstance> enabledServerMap = createServerMap(instances);
    Set<String> enabledInstances = Set.copyOf(instances);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        enabledInstances, enabledServerMap, idealState, externalView, onlineSegments);

    // Normal selection for requestId 1 picks replica group 1 and, in doing so, snapshots snapshotA for fail-open.
    InstanceSelector.InstanceMapping normalMapping =
        selector.select(getSegments(), 1, selector._segmentStates, Map.of(), true);
    assertEquals(normalMapping.segmentToInstanceMap(), createExpectedAssignment(replicaGroup1, getSegments()));

    // Publish a new snapshot with disjoint instances, as a concurrent onAssignmentChange() would.
    InstancePartitions snapshotB =
        createInstancePartitions(List.of("instance-10", "instance-11"), List.of("instance-12", "instance-13"));
    doReturn(snapshotB).when(selector).getInstancePartitions();
    selector.onInstancesChange(enabledInstances, instances);

    InstanceSelector.InstanceMapping failOpenMapping =
        selector.selectForFailOpen(getSegments(), 1, selector._segmentStates, Map.of());

    // The fail-open reselection still resolves against snapshotA, not the newly-published snapshotB.
    assertEquals(failOpenMapping.segmentToInstanceMap(), createExpectedAssignment(replicaGroup1, getSegments()));
  }

  @Test
  public void testFailOpenReassignsCompletePartitionToFallbackReplicaGroup() {
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    // Make segment0 available only from replica group 1. Other segments remain healthy in replica group 0.
    // segment0 gets its own maps because setupBasicTestEnvironment shares one map across all even segments.
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.copyOf(replicaGroup1));

    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.copyOf(replicaGroup0), createServerMap(instances), idealState, externalView, onlineSegments);
    FailOpenPolicy failOpenPolicy = FailOpenPolicy.disabled();
    selector.setFailOpenRouting(failOpenPolicy, failureDetector);
    failOpenPolicy.onChange(Set.of(CONFIG_OF_ENABLE_FAIL_OPEN),
        Map.of(CONFIG_OF_ENABLE_FAIL_OPEN, Boolean.TRUE.toString()));
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, getSegments(), 0);

    assertEquals(result.getSegmentToInstanceMap(), createExpectedAssignment(replicaGroup1, getSegments()));
    assertTrue(result.getUnavailableSegments().isEmpty());
    // Fail-open found a candidate for the unavailable segment.
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
    verify(failureDetector, never()).markServerHealthy(anyString(), any());
    verify(failureDetector, never()).markServerUnhealthy(anyString(), any());
  }

  @Test
  public void testFailOpenRebuildsReplicaGroupWhenHealthyCandidatesAreSplit() {
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    // Make segment0 available only from instance-2. segment0 gets its own maps because setupBasicTestEnvironment
    // shares one map across all even segments.
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-2"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-0", "instance-3"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, getSegments(), 0);

    assertEquals(result.getSegmentToInstanceMap(), createExpectedAssignment(replicaGroup1, getSegments()));
    assertTrue(result.getUnavailableSegments().isEmpty());
    // Fail-open found a candidate for the unavailable segment.
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
  }

  @Test
  public void testFailOpenStillThrowsWhenSplitPartitionHasNoUnavailableSegment() {
    // Disable instance-0 and instance-3 (one per replica group), leaving instance-1 and instance-2 enabled. Each
    // segment still has an enabled candidate (odd segments via instance-1, even segments via instance-2), so none of
    // them are tracked as unavailable, but no single replica group covers both the even and odd segments. Fail-open
    // must not suppress the exception here since it would never actually be invoked for this request (no unavailable
    // segment to trigger it in BaseInstanceSelector.select()), and silently dropping the split segments would be
    // worse than throwing.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of());

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-1", "instance-2"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    RuntimeException exception =
        expectThrows(RuntimeException.class, () -> selector.select(_brokerRequest, getSegments(), 0));
    assertTrue(exception.getMessage().contains("Unable to find any replica-group to serve segments"),
        exception.getMessage());
    // Fail-open is never actually invoked for this request (no unavailable segment triggers it), so the failure
    // detector must never be consulted either.
    verify(failureDetector, never()).getUnhealthyServers();
  }

  @Test
  public void testMseFailOpenNotInvokedForUnavailableSegmentOutsideRequest() {
    // Scenario: segment0's only remaining candidate (instance-2) is disabled, making segment0 unavailable, but the
    // query only requests the other 11 segments (segment0 is excluded from the request). Every other segment stays
    // healthy under replica group 0 (instance-0 serves the rest of the even segments, instance-1 serves the odd
    // segments).
    // Expected: fail-open must never engage for a request that doesn't touch any unavailable segment: the failure
    // detector is never consulted, selectForFailOpen() is never reached (the InstancePartitions snapshot ThreadLocal
    // stays unset), no FAIL_OPEN_* meter fires, and the healthy mapping for the 11 requested segments is returned
    // with no unavailable segments reported.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    // segment0 gets its own isolated candidate map (setupBasicTestEnvironment shares one TreeMap across all even
    // segments and another across all odd segments, so segment0 must not reuse that shared map here) with only
    // instance-2 as a candidate, which is not enabled below, making segment0 unavailable without affecting any
    // other even segment.
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.copyOf(replicaGroup0), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    List<String> requestedSegments = new ArrayList<>(getSegments());
    requestedSegments.remove("segment0");

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, requestedSegments, 0);

    Map<String, String> expectedAssignment = createExpectedAssignment(replicaGroup0, getSegments());
    expectedAssignment.remove("segment0");
    assertEquals(result.getSegmentToInstanceMap(), expectedAssignment);
    assertTrue(result.getUnavailableSegments().isEmpty());
    verify(failureDetector, never()).getUnhealthyServers();
    verify(_brokerMetrics, never()).addMeteredGlobalValue(eq(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES), anyInt());
    verify(_brokerMetrics, never()).addMeteredGlobalValue(eq(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES), anyInt());
    verify(_brokerMetrics, never()).addMeteredGlobalValue(eq(BrokerMeter.FAIL_OPEN_ELIGIBLE_REQUESTS), anyInt());
    assertNull(selector._instancePartitionsForFailOpen.get());
  }

  @Test
  public void testFailOpenRecoversPartitionWithUnavailableSegmentAndHealthySplit() {
    // Disable instance-0 and instance-3, leaving instance-1 and instance-2 enabled, same as
    // testFailOpenStillThrowsWhenSplitPartitionHasNoUnavailableSegment: even segments are healthy only via
    // instance-2 (replica group 1) and odd segments are healthy only via instance-1 (replica group 0). Additionally,
    // segment0 (detached from the shared even-segment view so it alone is affected) loses instance-2 entirely,
    // leaving only the disabled instance-0 as a candidate, making it unavailable. Since instance-0 is reported
    // unhealthy, fail-open can restore segment0 via instance-0 and, because reselection covers every segment, the
    // rest of the even segments can also be re-homed onto instance-0, letting replica group 0 serve everything.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    // Detach segment0 from the shared even-segment instance-state maps and remove instance-2 from its view.
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE, "instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-0"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-1", "instance-2"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, getSegments(), 0);

    assertEquals(result.getSegmentToInstanceMap(), createExpectedAssignment(replicaGroup0, getSegments()));
    assertTrue(result.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testThreadLocalsClearedAfterSuccessfulFailOpen() {
    // Scenario: identical topology to testFailOpenRecoversPartitionWithUnavailableSegmentAndHealthySplit, where
    // fail-open successfully reassigns the whole partition to replica group 0.
    // Expected: once select() returns, both _instancePartitionsForFailOpen and _segmentsDeferredToFailOpen must be
    // cleared, exactly like the healthy-selection and thrown-exception paths, so a pooled thread doesn't retain
    // either snapshot after a successful fail-open query.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE, "instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-0"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-1", "instance-2"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, getSegments(), 0);

    assertEquals(result.getSegmentToInstanceMap(), createExpectedAssignment(replicaGroup0, getSegments()));
    assertNull(selector._instancePartitionsForFailOpen.get());
    assertNull(selector._segmentsDeferredToFailOpen.get());
  }

  @Test
  public void testMseFailOpenSkipsCandidateLessNewLlcSegment() {
    // Scenario: identical topology to testFailOpenRecoversPartitionWithUnavailableSegmentAndHealthySplit (segment0 is
    // unavailable, instance-0 is reported unhealthy, so fail-open reassigns the whole partition to replica group 0).
    // In addition, a brand-new LLC segment (created "now", well within the new-segment expiration window) is
    // included in the request but has no entry anywhere in segmentStates, as if real-time segment propagation
    // simply hasn't caught up with it yet.
    // Expected: the new segment is safely skipped by both the normal and the fail-open pass
    // (canSkipSegmentForFailOpen() -> isNewLLCSegment()) instead of causing fail-open to fail outright, and it must
    // not show up in getUnavailableSegments() either, since it was never tracked as unavailable in the first place.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE, "instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-0"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-1", "instance-2"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    List<String> segments = new ArrayList<>(getSegments());
    LLCSegmentName newSegmentName = getLLCSegmentName(System.currentTimeMillis());
    segments.add(newSegmentName.getSegmentName());

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, segments, 0);

    assertEquals(result.getSegmentToInstanceMap(), createExpectedAssignment(replicaGroup0, getSegments()));
    assertTrue(result.getOptionalSegmentToInstanceMap().isEmpty());
    assertTrue(result.getUnavailableSegments().isEmpty());
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
  }

  @Test
  public void testMseFailOpenDoesNotSkipCandidateLessOldSegment() {
    // Scenario: identical topology to testMseFailOpenSkipsCandidateLessNewLlcSegment, except the extra untracked
    // segment uses an LLC creation time well past the new-segment expiration window, so isNewLLCSegment() returns
    // false for it.
    // Expected: canSkipSegmentForFailOpen() must not treat this segment as skippable. It has no candidates
    // anywhere in segmentStates and is not a "new" segment, so it must cause the query to fail loudly (an exception
    // identifying the segment) rather than being silently dropped from every assigned/optional/unavailable
    // collection.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE, "instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-0"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-1", "instance-2"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    List<String> segments = new ArrayList<>(getSegments());
    long delta = selector._newSegmentExpirationTimeInSeconds * 1000 + 10_000;
    long expiredSegmentEpochMs = System.currentTimeMillis() - delta;
    LLCSegmentName oldSegmentName = getLLCSegmentName(expiredSegmentEpochMs);
    segments.add(oldSegmentName.getSegmentName());

    try {
      selector.select(_brokerRequest, segments, 0);
      fail("Method call above should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(oldSegmentName.getSegmentName()), e.getMessage());
    }
  }

  @Test
  public void testMseFailOpenDisabledForUnavailableSegment() {
    // Scenario: segment0 gets its own isolated candidate map (setupBasicTestEnvironment shares one TreeMap across
    // all even segments and another across all odd segments, so segment0 must not reuse that shared map here) with
    // only instance-2 as a candidate, which is not enabled below, making segment0 unavailable while every other
    // segment stays healthy. FailOpenPolicy is disabled.
    // Expected: with fail-open disabled, selectForFailOpen() must never be reached (the failure detector is never
    // consulted and both fail-open ThreadLocals stay unset), and the query must fail with the same
    // unavailable-segment contract MSE already enforces without fail-open (see testInstanceFailureHandling), rather
    // than silently or partially succeeding.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);

    selector.setFailOpenRouting(FailOpenPolicy.disabled(), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.copyOf(replicaGroup0), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    try {
      selector.select(_brokerRequest, getSegments(), 0);
      fail("Method call above should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("segment0"), e.getMessage());
    }
    verify(failureDetector, never()).getUnhealthyServers();
    assertNull(selector._instancePartitionsForFailOpen.get());
    assertNull(selector._segmentsDeferredToFailOpen.get());
  }

  @Test
  public void testFailOpenThrowsWhenSplitPartitionFailOpenAlsoFails() {
    // Same setup as testFailOpenRecoversPartitionWithUnavailableSegmentAndHealthySplit: segment0 is unavailable
    // (only candidate is disabled instance-0) and the remaining segments are split across replica groups (odds via
    // instance-1, evens via instance-2), so no single replica group can cover them and the partition is deferred to
    // fail-open. Unlike that test, no server is reported unhealthy, so fail-open's reselection of segment0 has no
    // eligible candidate and selectFailOpen() returns null. The partition must still fail loudly here rather than
    // falling back to a mismatched-replica-group assignment: colocated joins (for both MSE and SSE queries) rely on
    // every segment in a partition coming from one consistent replica group, so silently serving this partition
    // from a mix of replica groups risks wrong query results instead of a visible failure.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    // Detach segment0 from the shared even-segment instance-state maps and remove instance-2 from its view.
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE, "instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-0", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of());

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-1", "instance-2"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    RuntimeException exception = expectThrows(RuntimeException.class,
        () -> selector.select(_brokerRequest, getSegments(), 0));
    assertTrue(exception.getMessage().contains("was dropped from the selection"), exception.getMessage());
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
  }

  @Test
  public void testFailOpenSkipsNewSegmentWithoutHealthyCandidates() {
    // segment0 is unavailable (only on unhealthy instance-2), so fail-open moves the partition to replica group 1.
    // New segment2's only server, instance-2, is also unhealthy. The normal pass skips segment2, so fail-open must skip
    // it too instead of routing it to instance-2.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    // Make segment0 available only from instance-2. segment0 gets its own maps because setupBasicTestEnvironment
    // shares one map across all even segments.
    idealState.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    externalView.getRecord().setMapField("segment0", new TreeMap<>(Map.of("instance-2", ONLINE)));
    idealState.getRecord().setMapField("segment2", Map.of("instance-2", ONLINE));
    externalView.getRecord().setMapField("segment2", Map.of("instance-2", OFFLINE));
    doReturn(Map.of("segment2", System.currentTimeMillis())).when(selector)
        .getNewSegmentCreationTimeMapFromZK(any(), any(), any());
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.copyOf(replicaGroup1));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.copyOf(replicaGroup0), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, getSegments(), 0);

    Map<String, String> expectedAssignment = createExpectedAssignment(replicaGroup1, getSegments());
    expectedAssignment.remove("segment2");
    assertEquals(result.getSegmentToInstanceMap(), expectedAssignment);
    assertTrue(result.getOptionalSegmentToInstanceMap().isEmpty());
    assertTrue(result.getUnavailableSegments().isEmpty());
    // Fail-open found a candidate for the unavailable segment.
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
  }

  @Test
  public void testFailOpenRejectsCandidatesSplitAcrossReplicaGroups() {
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    FailureDetector failureDetector = mock(FailureDetector.class);
    // Even segments are eligible only in replica group 0 and odd segments only in replica group 1.
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-0", "instance-3"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of(), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, getSegments(), 0);

    assertTrue(result.getSegmentToInstanceMap().isEmpty());
    assertEquals(Set.copyOf(result.getUnavailableSegments()), onlineSegments);
    // Fail-open was attempted but could not find a candidate covering every segment in the split partition.
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
    verify(failureDetector, never()).markServerHealthy(anyString(), any());
    verify(failureDetector, never()).markServerUnhealthy(anyString(), any());
  }

  @Test
  public void testFailedFailOpenReturnsPartialNormalAssignment() {
    // Even segments (segment0, segment2, ...) are assigned to partition 0 (instance-0/instance-2) and odd segments
    // (segment1, segment3, ...) to partition 1 (instance-1/instance-3), per setupBasicTestEnvironment. Disabling
    // instance-1 and instance-3 (below), rather than removing them from the external view, keeps them as real
    // fail-open candidates so every odd segment is unavailable but selectFailOpen() still has something to attempt
    // and reject, instead of skipping fail-open entirely for lack of any cached candidate.
    InstancePartitions instancePartitions = new InstancePartitions(TABLE_NAME);
    instancePartitions.setInstances(0, 0, List.of("instance-0"));
    instancePartitions.setInstances(0, 1, List.of("instance-2"));
    instancePartitions.setInstances(1, 0, List.of("instance-1"));
    instancePartitions.setInstances(1, 1, List.of("instance-3"));
    MultiStageReplicaGroupSelector selector = createMultiStageSelector(instancePartitions);
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    FailureDetector failureDetector = mock(FailureDetector.class);
    // No unhealthy servers means selectFailOpen() is attempted for the odd segments (real candidates instance-1 and
    // instance-3 exist) but rejects every candidate, since none of them is reported unhealthy.
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of());

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    // instance-0 is disabled, so even segments (whose preferred replica group is instance-0) fall back to
    // instance-2 (the other replica group for that instance-partition) via the normal replica-group fallback logic.
    // instance-1 and instance-3 are also disabled (but left in the external view), making every odd segment
    // unavailable.
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-2"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, getSegments(), 0);

    List<String> evenSegments = List.of("segment0", "segment2", "segment4", "segment6", "segment8", "segment10");
    List<String> oddSegments = List.of("segment1", "segment3", "segment5", "segment7", "segment9", "segment11");
    Map<String, String> expectedAssignment = new HashMap<>();
    for (String segment : evenSegments) {
      expectedAssignment.put(segment, "instance-2");
    }
    // Fail-open selection is attempted but fails (no unhealthy servers to fail open onto), so the fallback is the
    // normal mapping, which already excludes the unavailable odd segments rather than being discarded entirely.
    assertEquals(result.getSegmentToInstanceMap(), expectedAssignment);
    assertEquals(Set.copyOf(result.getUnavailableSegments()), Set.copyOf(oddSegments));
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    // Even though fail-open was attempted and failed, falling back to the partial normal mapping must not leave
    // either ThreadLocal behind for the next query on this (possibly pooled) thread.
    assertNull(selector._instancePartitionsForFailOpen.get());
    assertNull(selector._segmentsDeferredToFailOpen.get());
  }

  @Test
  public void testFailOpenReselectionHonorsFixedReplicaStickyRouting() {
    // Partition 0 (even segments) is served by instance-0 (replica group 0) or instance-2 (replica group 1); both
    // are enabled and online, so normal selection is a genuine tie that fixed-replica/sticky routing must resolve to
    // replica group 0 (instance-0) for every request, regardless of requestId. Partition 1 (odd segments) is served
    // by instance-1 (replica group 0) or instance-3 (replica group 1).
    InstancePartitions instancePartitions = new InstancePartitions(TABLE_NAME);
    instancePartitions.setInstances(0, 0, List.of("instance-0"));
    instancePartitions.setInstances(0, 1, List.of("instance-2"));
    instancePartitions.setInstances(1, 0, List.of("instance-1"));
    instancePartitions.setInstances(1, 1, List.of("instance-3"));
    MultiStageReplicaGroupSelector selector = createMultiStageSelector(instancePartitions);
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    // Detach segment1 so its only candidate is instance-1, which we then disable below: this makes segment1 the
    // sole unavailable segment, which is enough to force the whole query through fail-open reselection, while every
    // other segment (including the rest of partition 1) stays reachable without needing fail-open itself.
    idealState.getRecord().setMapField("segment1", new TreeMap<>(Map.of("instance-1", ONLINE)));
    externalView.getRecord().setMapField("segment1", new TreeMap<>(Map.of("instance-1", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-1"));

    // Fixed-replica/sticky routing is enabled on the table.
    InstanceSelectorConfig fixedReplicaConfig = new InstanceSelectorConfig(true, 300, false);
    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), fixedReplicaConfig,
        Set.of("instance-0", "instance-2", "instance-3"), createServerMap(instances), idealState, externalView,
        onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult firstResult = selector.select(_brokerRequest, getSegments(), 0);
    InstanceSelector.SelectionResult secondResult = selector.select(_brokerRequest, getSegments(), 1);

    List<String> evenSegments = List.of("segment0", "segment2", "segment4", "segment6", "segment8", "segment10");
    List<String> oddSegments = List.of("segment1", "segment3", "segment5", "segment7", "segment9", "segment11");
    Map<String, String> expectedAssignment = new HashMap<>();
    for (String segment : evenSegments) {
      expectedAssignment.put(segment, "instance-0");
    }
    for (String segment : oddSegments) {
      expectedAssignment.put(segment, "instance-1");
    }
    // Partition 0's tie must resolve to replica group 0 (instance-0) for every request when fixed-replica/sticky
    // routing is enabled, exactly like healthy (non-fail-open) selection does. If selectForFailOpen() ever computed
    // its preferred replica group as `requestId % numReplicaGroups` unconditionally instead of honoring the table's
    // fixed-replica config, requestId=1 would resolve the tie to replica group 1 (instance-2) instead, failing this
    // assertion.
    assertEquals(firstResult.getSegmentToInstanceMap(), expectedAssignment);
    assertEquals(secondResult.getSegmentToInstanceMap(), expectedAssignment);
    assertTrue(firstResult.getUnavailableSegments().isEmpty());
    assertTrue(secondResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testFailOpenOnlyMovesHealthySegmentsOntoUnhealthyServersAsLastResort() {
    // Partition 0 (even segments) is served by instance-0 (replica group 0) or instance-2 (replica group 1), and
    // partition 1 (odd segments) by instance-1 (replica group 0) or instance-3 (replica group 1). instance-0 and
    // instance-1 are unhealthy. segment1's only candidate is instance-1, making it unavailable and forcing fail-open.
    // - Partition 0 has a healthy route via replica group 1, so it must stay on instance-2 even when replica group 0 is
    //   preferred, rather than being moved back onto unhealthy instance-0.
    // - Partition 1 has no healthy route: segment1 can only be served by replica group 0, while the other odd segments
    //   are only healthy on replica group 1. As a last resort, they all move onto instance-1 to keep the partition on
    //   one replica group.
    InstancePartitions instancePartitions = new InstancePartitions(TABLE_NAME);
    instancePartitions.setInstances(0, 0, List.of("instance-0"));
    instancePartitions.setInstances(0, 1, List.of("instance-2"));
    instancePartitions.setInstances(1, 0, List.of("instance-1"));
    instancePartitions.setInstances(1, 1, List.of("instance-3"));
    MultiStageReplicaGroupSelector selector = createMultiStageSelector(instancePartitions);
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    idealState.getRecord().setMapField("segment1", new TreeMap<>(Map.of("instance-1", ONLINE)));
    externalView.getRecord().setMapField("segment1", new TreeMap<>(Map.of("instance-1", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-0", "instance-1"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-2", "instance-3"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    Map<String, String> expectedAssignment = new HashMap<>();
    for (int i = 0; i < getSegments().size(); i++) {
      expectedAssignment.put(getSegments().get(i), i % 2 == 0 ? "instance-2" : "instance-1");
    }
    // requestId=0 prefers replica group 0 and requestId=1 prefers replica group 1.
    for (int requestId = 0; requestId < 2; requestId++) {
      InstanceSelector.SelectionResult result = selector.select(_brokerRequest, getSegments(), requestId);

      assertEquals(result.getSegmentToInstanceMap(), expectedAssignment, "requestId=" + requestId);
      assertTrue(result.getUnavailableSegments().isEmpty());
    }
  }

  @Test
  public void testFailOpenNewSegmentWithoutHealthyCandidatesDoesNotMoveHealthyPartition() {
    // Same layout as testFailOpenOnlyMovesHealthySegmentsOntoUnhealthyServersAsLastResort: partition 0 (even segments)
    // on instance-0 (replica group 0) / instance-2 (replica group 1), partition 1 (odd segments) on instance-1 /
    // instance-3. instance-1 and instance-2 are unhealthy, and unavailable segment1 (only on instance-1) forces
    // fail-open.
    // New segment segmentNew sits only on unhealthy instance-2, so the normal pass skips it and routes partition 0 to
    // its healthy route, instance-0. Fail-open must also skip segmentNew rather than moving partition 0 onto
    // instance-2 to serve it.
    InstancePartitions instancePartitions = new InstancePartitions(TABLE_NAME);
    instancePartitions.setInstances(0, 0, List.of("instance-0"));
    instancePartitions.setInstances(0, 1, List.of("instance-2"));
    instancePartitions.setInstances(1, 0, List.of("instance-1"));
    instancePartitions.setInstances(1, 1, List.of("instance-3"));
    MultiStageReplicaGroupSelector selector = createMultiStageSelector(instancePartitions);
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    idealState.getRecord().setMapField("segment1", new TreeMap<>(Map.of("instance-1", ONLINE)));
    externalView.getRecord().setMapField("segment1", new TreeMap<>(Map.of("instance-1", ONLINE)));
    String newSegment = "segmentNew";
    idealState.getRecord().setMapField(newSegment, new TreeMap<>(Map.of("instance-2", ONLINE)));
    externalView.getRecord().setMapField(newSegment, new TreeMap<>(Map.of("instance-2", ONLINE)));
    onlineSegments.add(newSegment);
    doReturn(Map.of(newSegment, System.currentTimeMillis())).when(selector)
        .getNewSegmentCreationTimeMapFromZK(any(), any(), any());
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-1", "instance-2"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-0", "instance-3"), createServerMap(instances), idealState, externalView, onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    List<String> segments = new ArrayList<>(getSegments());
    segments.add(newSegment);
    Map<String, String> expectedAssignment = new HashMap<>();
    for (int i = 0; i < getSegments().size(); i++) {
      expectedAssignment.put(getSegments().get(i), i % 2 == 0 ? "instance-0" : "instance-1");
    }
    // requestId=0 prefers replica group 0 and requestId=1 prefers replica group 1.
    for (int requestId = 0; requestId < 2; requestId++) {
      InstanceSelector.SelectionResult result = selector.select(_brokerRequest, segments, requestId);

      assertEquals(result.getSegmentToInstanceMap(), expectedAssignment, "requestId=" + requestId);
      assertTrue(result.getOptionalSegmentToInstanceMap().isEmpty(), "requestId=" + requestId);
      assertTrue(result.getUnavailableSegments().isEmpty());
    }
    verify(_brokerMetrics, times(2)).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
  }

  @Test
  public void testFailOpenLastResortPrefersHealthyReplicaWithinReplicaGroup() {
    // Replica group 0 = [instance-0, instance-1], replica group 1 = [instance-2, instance-3]; instance-1 is unhealthy.
    // - segmentA is unavailable (only on instance-1), forcing fail-open and requiring replica group 0.
    // - segmentB has two replicas in replica group 0: healthy instance-0 and unhealthy instance-1.
    // - segmentC is healthy only on instance-2 (replica group 1), with a fallback on instance-1.
    // No replica group covers all three with healthy candidates alone, so the last resort picks replica group 0.
    // segmentC has to move onto instance-1, but segmentB must stay on its healthy replica instance-0.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    MultiStageReplicaGroupSelector selector =
        createMultiStageSelector(createInstancePartitions(replicaGroup0, replicaGroup1));
    List<String> instances = createEnabledInstances(4);
    Map<String, Map<String, String>> assignment = Map.of(
        "segmentA", Map.of("instance-1", ONLINE),
        "segmentB", Map.of("instance-0", ONLINE, "instance-1", ONLINE),
        "segmentC", Map.of("instance-1", ONLINE, "instance-2", ONLINE));
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    for (Map.Entry<String, Map<String, String>> entry : assignment.entrySet()) {
      idealState.getRecord().setMapField(entry.getKey(), new TreeMap<>(entry.getValue()));
      externalView.getRecord().setMapField(entry.getKey(), new TreeMap<>(entry.getValue()));
    }
    Set<String> onlineSegments = new HashSet<>(assignment.keySet());
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-1"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-0", "instance-2", "instance-3"), createServerMap(instances), idealState, externalView,
        onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result =
        selector.select(_brokerRequest, List.of("segmentA", "segmentB", "segmentC"), 0);

    assertEquals(result.getSegmentToInstanceMap(),
        Map.of("segmentA", "instance-1", "segmentB", "instance-0", "segmentC", "instance-1"));
    assertTrue(result.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testMseFailOpenPreservesFixedReplicaQueryOption() {
    // Scenario: identical topology to testFailOpenReselectionHonorsFixedReplicaStickyRouting, but fixed-replica/
    // sticky routing is requested via the USE_FIXED_REPLICA query option instead of table config, and the table is
    // initialized with the default (non-fixed-replica) InstanceSelectorConfig. resolvePreferredReplicaGroup() reads
    // the combined ServerSelectionContext (query options merged with InstanceSelectorConfig), so the query option
    // alone must be enough to pin partition 0's tie to replica group 0 (instance-0) on both the normal and
    // fail-open (segment1 is the sole unavailable segment, forcing reselection) passes, for every requestId.
    // Expected: requestId=0 and requestId=1 both resolve to the exact same assignment.
    InstancePartitions instancePartitions = new InstancePartitions(TABLE_NAME);
    instancePartitions.setInstances(0, 0, List.of("instance-0"));
    instancePartitions.setInstances(0, 1, List.of("instance-2"));
    instancePartitions.setInstances(1, 0, List.of("instance-1"));
    instancePartitions.setInstances(1, 1, List.of("instance-3"));
    MultiStageReplicaGroupSelector selector = createMultiStageSelector(instancePartitions);
    List<String> instances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(instances, idealState, externalView, onlineSegments);
    idealState.getRecord().setMapField("segment1", new TreeMap<>(Map.of("instance-1", ONLINE)));
    externalView.getRecord().setMapField("segment1", new TreeMap<>(Map.of("instance-1", ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of("instance-1"));

    selector.setFailOpenRouting(new FailOpenPolicy(false, true), failureDetector);
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), INSTANCE_SELECTOR_CONFIG,
        Set.of("instance-0", "instance-2", "instance-3"), createServerMap(instances), idealState, externalView,
        onlineSegments);
    when(_pinotQuery.getQueryOptions()).thenReturn(
        Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString(), USE_FIXED_REPLICA, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult firstResult = selector.select(_brokerRequest, getSegments(), 0);
    InstanceSelector.SelectionResult secondResult = selector.select(_brokerRequest, getSegments(), 1);

    List<String> evenSegments = List.of("segment0", "segment2", "segment4", "segment6", "segment8", "segment10");
    List<String> oddSegments = List.of("segment1", "segment3", "segment5", "segment7", "segment9", "segment11");
    Map<String, String> expectedAssignment = new HashMap<>();
    for (String segment : evenSegments) {
      expectedAssignment.put(segment, "instance-0");
    }
    for (String segment : oddSegments) {
      expectedAssignment.put(segment, "instance-1");
    }
    // If selectForFailOpen() ever computed its preferred replica group as `requestId % numReplicaGroups`
    // unconditionally instead of honoring the USE_FIXED_REPLICA query option, requestId=1 would resolve the tie to
    // replica group 1 (instance-2) instead, failing this assertion.
    assertEquals(firstResult.getSegmentToInstanceMap(), expectedAssignment);
    assertEquals(secondResult.getSegmentToInstanceMap(), expectedAssignment);
    assertTrue(firstResult.getUnavailableSegments().isEmpty());
    assertTrue(secondResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testErrorSegmentHandling() {
    // Create instance-partitions with two replica-groups and 2 partitions. Each replica-group has 2 instances.
    Map<String, List<String>> partitionToInstances = Map.of(
        "0_0", List.of("instance-0"),
        "0_1", List.of("instance-2"),
        "1_0", List.of("instance-1"),
        "1_1", List.of("instance-3"));
    InstancePartitions instancePartitions = new InstancePartitions(TABLE_NAME);
    instancePartitions.setInstances(0, 0, partitionToInstances.get("0_0"));
    instancePartitions.setInstances(0, 1, partitionToInstances.get("0_1"));
    instancePartitions.setInstances(1, 0, partitionToInstances.get("1_0"));
    instancePartitions.setInstances(1, 1, partitionToInstances.get("1_1"));

    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    List<String> enabledInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();

    setupBasicTestEnvironment(enabledInstances, idealState, externalView, onlineSegments);

    // Set one segment in each replica group to ERROR state.
    externalView.getRecord().getMapFields().get("segment1").put("instance-1", "ERROR");
    externalView.getRecord().getMapFields().get("segment2").put("instance-2", "ERROR");

    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    // Even though instance-0 and instance-3 belong to different replica groups, they handle exclusive sets of segments
    // and hence they can together serve all segments.
    Map<String, String> expectedSelectorResult = new HashMap<>();
    for (int segmentNum = 0; segmentNum < getSegments().size(); segmentNum++) {
      if (segmentNum % 2 == 0) {
        expectedSelectorResult.put(getSegments().get(segmentNum), "instance-0");
      } else {
        expectedSelectorResult.put(getSegments().get(segmentNum), "instance-3");
      }
    }
    InstanceSelector.SelectionResult selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // If instance-3 has an error segment as well, there is no replica group available to serve complete set of
    // segments.
    externalView.getRecord().getMapFields().get("segment3").put("instance-3", "ERROR");
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
    try {
      multiStageSelector.select(_brokerRequest, getSegments(), 0);
      fail("Method call above should have failed");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testInstancePartitionsIdealStateMismatchSegmentsDropped() {
    // TODO: Fix this issue (described in https://github.com/apache/pinot/issues/17179) and update or remove this test.

    // InstancePartitions updated to a new set of instances, while IdealState/ExternalView still reference old ones.
    // This scenario can occur during a rebalance with instance reassignment, where the InstancePartitions in ZK are
    // updated first, and then the IdealState is updated in batches.

    // IP contains instances [instance-100..103], but IS/EV map segments to [instance-0..3].
    List<String> newReplicaGroup0 = List.of("instance-100", "instance-101");
    List<String> newReplicaGroup1 = List.of("instance-102", "instance-103");
    InstancePartitions instancePartitions = createInstancePartitions(newReplicaGroup0, newReplicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    // Enabled instances and segment assignments still reference the old instances [instance-0..3]
    List<String> enabledInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();

    setupBasicTestEnvironment(enabledInstances, idealState, externalView, onlineSegments);
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    InstanceSelector.SelectionResult selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);

    // Because instanceToPartitionMap is unaware of instances in IS/EV, segments get grouped under a null partition
    // and are never considered in the [0..numPartitions) loop, resulting in dropped segments.
    assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
    assertTrue(selectionResult.getOptionalSegmentToInstanceMap().isEmpty());
    // Old segments are available in IS/EV, so they should not be marked as unavailable; they are effectively dropped.
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testInstancesChangeRefreshesInstancePartitionsForBothOverloads() {
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    InstancePartitions instancePartitions = createInstancePartitions(replicaGroup0, replicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    List<String> enabledInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(enabledInstances, idealState, externalView, onlineSegments);

    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
    verify(multiStageSelector, times(1)).getInstancePartitions();

    // Direct callers of the legacy two-argument overload must still refresh _instancePartitions.
    multiStageSelector.onInstancesChange(new HashSet<>(enabledInstances), enabledInstances);
    verify(multiStageSelector, times(2)).getInstancePartitions();

    // The three-argument overload (used by the routing manager) dispatches through the two-argument override, so it
    // must also refresh _instancePartitions.
    multiStageSelector.onInstancesChange(new HashSet<>(enabledInstances), new HashSet<>(enabledInstances),
        enabledInstances);
    verify(multiStageSelector, times(3)).getInstancePartitions();
  }

  @Test
  public void testThreadLocalClearedAfterHealthySelectionWithFailOpenDisabled() {
    // A healthy request never reaches selectForFailOpen(), so the ThreadLocal snapshot must be cleared by the outer
    // select(BrokerRequest, ...) flow itself rather than relying on the next fail-open call to clear it.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    InstancePartitions instancePartitions = createInstancePartitions(replicaGroup0, replicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    List<String> enabledInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(enabledInstances, idealState, externalView, onlineSegments);
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    multiStageSelector.select(_brokerRequest, getSegments(), 0);

    assertNull(multiStageSelector._instancePartitionsForFailOpen.get());
    // No partition was deferred to fail-open on this healthy path either, so this ThreadLocal must be cleared too.
    assertNull(multiStageSelector._segmentsDeferredToFailOpen.get());
  }

  @Test
  public void testThreadLocalClearedAfterSelectionThrows() {
    // The internal select() call can throw (e.g. no replica group covers a split partition) before
    // selectForFailOpen() ever runs. The outer select(BrokerRequest, ...) must still clear the ThreadLocal via its
    // finally block so a pooled thread doesn't retain the snapshot afterward.
    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    InstancePartitions instancePartitions = createInstancePartitions(replicaGroup0, replicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    List<String> enabledInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();
    setupBasicTestEnvironment(enabledInstances, idealState, externalView, onlineSegments);
    enabledInstances.remove("instance-0");
    enabledInstances.remove("instance-2");
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    Exception e = expectThrows(IllegalStateException.class,
        () -> multiStageSelector.select(_brokerRequest, getSegments(), 0));
    assertTrue(e.getMessage().contains("Failed to find servers for segment"), e.getMessage());

    assertNull(multiStageSelector._instancePartitionsForFailOpen.get());
    // A thrown selection must also leave no deferred-segments state behind for the next query on this thread.
    assertNull(multiStageSelector._segmentsDeferredToFailOpen.get());
  }

  private MultiStageReplicaGroupSelector createMultiStageSelector(InstancePartitions instancePartitions) {
    MultiStageReplicaGroupSelector multiStageSelector = new MultiStageReplicaGroupSelector();
    multiStageSelector = spy(multiStageSelector);
    doReturn(instancePartitions).when(multiStageSelector).getInstancePartitions();
    return multiStageSelector;
  }

  private Map<String, ServerInstance> createServerMap(List<String> instances) {
    Map<String, ServerInstance> serverMap = new HashMap<>();
    for (String instance : instances) {
      serverMap.put(instance, mock(ServerInstance.class));
    }
    return serverMap;
  }

  private InstancePartitions createInstancePartitions(List<String> replicaGroup0, List<String> replicaGroup1) {
    Map<String, List<String>> partitionToInstances = Map.of("0_0", replicaGroup0, "0_1", replicaGroup1);
    InstancePartitions instancePartitions = new InstancePartitions(TABLE_NAME);
    instancePartitions.setInstances(0, 0, partitionToInstances.get("0_0"));
    instancePartitions.setInstances(0, 1, partitionToInstances.get("0_1"));
    return instancePartitions;
  }

  private void setupInstanceStates(List<String> enabledInstances, Map<String, String> idealStateInstanceStateMap0,
      Map<String, String> externalViewInstanceStateMap0, Map<String, String> idealStateInstanceStateMap1,
      Map<String, String> externalViewInstanceStateMap1) {
    for (int i = 0; i < enabledInstances.size(); i++) {
      String instance = enabledInstances.get(i);
      if (i % 2 == 0) {
        idealStateInstanceStateMap0.put(instance, ONLINE);
        externalViewInstanceStateMap0.put(instance, ONLINE);
      } else {
        idealStateInstanceStateMap1.put(instance, ONLINE);
        externalViewInstanceStateMap1.put(instance, ONLINE);
      }
    }
  }

  private void setupSegmentAssignments(List<String> segments,
      Map<String, Map<String, String>> idealStateSegmentAssignment,
      Map<String, Map<String, String>> externalViewSegmentAssignment, Map<String, String> idealStateInstanceStateMap0,
      Map<String, String> externalViewInstanceStateMap0, Map<String, String> idealStateInstanceStateMap1,
      Map<String, String> externalViewInstanceStateMap1, Set<String> onlineSegments) {
    for (int segmentNum = 0; segmentNum < segments.size(); segmentNum++) {
      String segment = segments.get(segmentNum);
      if (segmentNum % 2 == 0) {
        idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
        externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      } else {
        idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap1);
        externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap1);
      }
      onlineSegments.add(segment);
    }
  }

  private Map<String, String> createExpectedAssignment(List<String> replicaGroup, List<String> segments) {
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    for (int segmentNum = 0; segmentNum < segments.size(); segmentNum++) {
      expectedReplicaGroupInstanceSelectorResult.put(segments.get(segmentNum), replicaGroup.get(segmentNum % 2));
    }
    return expectedReplicaGroupInstanceSelectorResult;
  }

  private void setupBasicTestEnvironment(List<String> enabledInstances, IdealState idealState,
      ExternalView externalView, Set<String> onlineSegments) {
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();

    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();
    Map<String, String> idealStateInstanceStateMap1 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap1 = new TreeMap<>();

    setupInstanceStates(enabledInstances, idealStateInstanceStateMap0, externalViewInstanceStateMap0,
        idealStateInstanceStateMap1, externalViewInstanceStateMap1);
    setupSegmentAssignments(getSegments(), idealStateSegmentAssignment, externalViewSegmentAssignment,
        idealStateInstanceStateMap0, externalViewInstanceStateMap0, idealStateInstanceStateMap1,
        externalViewInstanceStateMap1, onlineSegments);
  }

  private List<String> createEnabledInstances(int count) {
    List<String> enabledInstances = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      enabledInstances.add(String.format("instance-%d", i));
    }
    return enabledInstances;
  }
}
