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
import java.util.Collections;
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
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
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

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class MultiStageReplicaGroupSelectorTest {
  private static final String TABLE_NAME = "testTable_REALTIME";
  private final static List<String> SEGMENTS =
      Arrays.asList("segment0", "segment1", "segment2", "segment3", "segment4", "segment5", "segment6", "segment7",
          "segment8", "segment9", "segment10", "segment11");
  private static final Map<String, ServerInstance> EMPTY_SERVER_MAP = Collections.emptyMap();
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
    Exception e =
        expectThrows(IllegalStateException.class, () -> multiStageSelector.select(_brokerRequest, segments, 1));
    assertTrue(e.getMessage().contains(segments.get(segments.size() - 1)));

    // Test with a non LLC segment, like UploadedRealtimeSegment, that has just been created. Since it is not present
    // in segmentState, it should throw.
    segments.set(segments.size() - 1, new UploadedRealtimeSegmentName(TABLE_NAME, 1 /* partition */,
        System.currentTimeMillis(), "someprefix", "somesuffix").getSegmentName());
    e = expectThrows(IllegalStateException.class, () -> multiStageSelector.select(_brokerRequest, segments, 1));
    assertTrue(e.getMessage().contains(segments.get(segments.size() - 1)));
  }

  @Test
  public void testBasicReplicaGroupSelectionUsingIdealStateInstancePartitions() {
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
    InstancePartitionsUtils.combineInstancePartitionsInIdealState(idealState, List.of(instancePartitions));
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
    Exception e =
        expectThrows(IllegalStateException.class, () -> multiStageSelector.select(_brokerRequest, segments, 1));
    assertTrue(e.getMessage().contains(segments.get(segments.size() - 1)));

    // Test with a non LLC segment, like UploadedRealtimeSegment, that has just been created. Since it is not present
    // in segmentState, it should throw.
    segments.set(segments.size() - 1, new UploadedRealtimeSegmentName(TABLE_NAME, 1 /* partition */,
        System.currentTimeMillis(), "someprefix", "somesuffix").getSegmentName());
    e = expectThrows(IllegalStateException.class, () -> multiStageSelector.select(_brokerRequest, segments, 1));
    assertTrue(e.getMessage().contains(segments.get(segments.size() - 1)));
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
    assertThrows(IllegalStateException.class, () -> multiStageSelector.select(_brokerRequest, getSegments(), 0));
  }

  @Test
  public void testInstanceFailureHandlingUsingIdealStateInstancePartitions() {
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
    InstancePartitionsUtils.combineInstancePartitionsInIdealState(idealState, List.of(instancePartitions));
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    // If instance-0 is down, requestId=0 will select instances across the replica groups
    enabledInstances.remove("instance-0");
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
    Map<String, String> expectedSelectorResult =
        createExpectedAssignment(List.of("instance-2", "instance-1"), getSegments());
    InstanceSelector.SelectionResult selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // If instance-2 also goes down, no replica-group is eligible
    enabledInstances.remove("instance-2");
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
    assertThrows(IllegalStateException.class, () -> multiStageSelector.select(_brokerRequest, getSegments(), 0));
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
  public void testRebalanceInstancePartitionsIdealStateMismatch1() {
    // InstancePartitions updated to a new set of instances, while IdealState/ExternalView still reference old ones.
    // This scenario can occur during a rebalance when the InstancePartitions in ZK are updated first, and then the
    // IdealState is updated in batches.

    // IP contains instances [instance-100..103], but IS/EV map segments to [instance-0..3].
    List<String> newReplicaGroup0 = List.of("instance-100", "instance-101");
    List<String> newReplicaGroup1 = List.of("instance-102", "instance-103");
    InstancePartitions instancePartitions = createInstancePartitions(newReplicaGroup0, newReplicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    // Segment assignments still reference the old instances [instance-0..3]
    List<String> initialInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();

    setupBasicTestEnvironment(initialInstances, idealState, externalView, onlineSegments);
    List<String> oldAndNewInstances = new ArrayList<>(initialInstances);
    oldAndNewInstances.addAll(List.of("instance-100", "instance-101", "instance-102", "instance-103"));

    // When the instance partitions are updated during a rebalance with instance reassignment, the table rebalancer
    // makes sure to combine the old and new set of instances in the ideal state instance partitions so that query
    // routing is unaffected.
    InstancePartitionsUtils.combineInstancePartitionsInIdealState(idealState, List.of(
        createInstancePartitions(List.of("instance-0", "instance-1", "instance-100", "instance-101"),
            List.of("instance-2", "instance-3", "instance-102", "instance-103"))));
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(oldAndNewInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    // Using requestId=0 should select replica-group 0. Even segments get assigned to instance-0 and odd segments get
    // assigned to instance-1.
    Map<String, String> expectedSelectorResult = createExpectedAssignment(replicaGroup0, getSegments());
    InstanceSelector.SelectionResult selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Using requestId=1 should select replica-group 1. Even segments get assigned to instance-2 and odd segments get
    // assigned to instance-3.
    expectedSelectorResult = createExpectedAssignment(replicaGroup1, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Update ideal state to replace instance-0 with instance-100 and instance-1 with instance-101
    Map<String, String> oldInstanceMap1 = Map.of("instance-0", ONLINE, "instance-2", ONLINE);
    Map<String, String> newInstanceMap1 = Map.of("instance-100", ONLINE, "instance-2", ONLINE);
    Map<String, String> oldInstanceMap2 = Map.of("instance-1", ONLINE, "instance-3", ONLINE);
    Map<String, String> newInstanceMap2 = Map.of("instance-101", ONLINE, "instance-3", ONLINE);

    idealState.getRecord().getMapFields().replaceAll((k, v) -> {
      if (v.equals(oldInstanceMap1)) {
        return newInstanceMap1;
      } else if (v.equals(oldInstanceMap2)) {
        return newInstanceMap2;
      } else {
        return v;
      }
    });

    multiStageSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Now, since instance-100 and instance-101 aren't available as per the external view, all requests should use
    // replica-group 1 (i.e., instance-2 and instance-3)
    expectedSelectorResult = createExpectedAssignment(replicaGroup1, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    expectedSelectorResult = createExpectedAssignment(replicaGroup1, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // External view updated
    externalView.getRecord().getMapFields().replaceAll((k, v) -> {
      if (v.equals(oldInstanceMap1)) {
        return newInstanceMap1;
      } else if (v.equals(oldInstanceMap2)) {
        return newInstanceMap2;
      } else {
        return v;
      }
    });

    multiStageSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Now, even request IDs should use the new replica-group 0 (instance-100, instance-101)
    expectedSelectorResult = createExpectedAssignment(newReplicaGroup0, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Odd request IDs should still be able to use the old replica-group 1
    expectedSelectorResult = createExpectedAssignment(replicaGroup1, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Update ideal state to replace old replica-group 1 with new replica-group 1
    Map<String, String> finalInstanceMap1 = Map.of("instance-100", ONLINE, "instance-102", ONLINE);
    Map<String, String> finalInstanceMap2 = Map.of("instance-101", ONLINE, "instance-103", ONLINE);

    idealState.getRecord().getMapFields().replaceAll((k, v) -> {
      if (v.equals(newInstanceMap1)) {
        return finalInstanceMap1;
      } else if (v.equals(newInstanceMap2)) {
        return finalInstanceMap2;
      } else {
        return v;
      }
    });

    multiStageSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Now, since instance-102 and instance-103 aren't available as per the external view, all requests should use
    // new replica-group 0 (i.e., instance-100 and instance-101)
    expectedSelectorResult = createExpectedAssignment(newReplicaGroup0, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    expectedSelectorResult = createExpectedAssignment(newReplicaGroup0, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // External view updated
    externalView.getRecord().getMapFields().replaceAll((k, v) -> {
      if (v.equals(newInstanceMap1)) {
        return finalInstanceMap1;
      } else if (v.equals(newInstanceMap2)) {
        return finalInstanceMap2;
      } else {
        return v;
      }
    });

    multiStageSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Even request IDs should still use the new replica-group 0 (instance-100, instance-101)
    expectedSelectorResult = createExpectedAssignment(newReplicaGroup0, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Odd request IDs should now be able to use the new replica-group 1
    expectedSelectorResult = createExpectedAssignment(newReplicaGroup1, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);
  }

  @Test
  public void testRebalanceInstancePartitionsIdealStateMismatch2() {
    // InstancePartitions updated to a new set of instances, while IdealState/ExternalView still reference old ones.
    // This scenario can occur during a rebalance when the InstancePartitions in ZK are updated first, and then the
    // IdealState is updated in batches.

    // IP contains instances [instance-100..103], but IS/EV map segments to [instance-0..3].
    List<String> newReplicaGroup0 = List.of("instance-100", "instance-101");
    List<String> newReplicaGroup1 = List.of("instance-102", "instance-103");
    InstancePartitions instancePartitions = createInstancePartitions(newReplicaGroup0, newReplicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    // Segment assignments still reference the old instances [instance-0..3]
    List<String> initialInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();

    setupBasicTestEnvironment(initialInstances, idealState, externalView, onlineSegments);
    List<String> oldAndNewInstances = new ArrayList<>(initialInstances);
    oldAndNewInstances.addAll(List.of("instance-100", "instance-101", "instance-102", "instance-103"));

    // When the instance partitions are updated during a rebalance with instance reassignment, the table rebalancer
    // makes sure to combine the old and new set of instances in the ideal state instance partitions so that query
    // routing is unaffected.
    InstancePartitionsUtils.combineInstancePartitionsInIdealState(idealState, List.of(
        createInstancePartitions(List.of("instance-0", "instance-1", "instance-100", "instance-101"),
            List.of("instance-2", "instance-3", "instance-102", "instance-103"))));
    multiStageSelector.init(_tableConfig, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, new HashSet<>(oldAndNewInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    List<String> replicaGroup0 = List.of("instance-0", "instance-1");
    List<String> replicaGroup1 = List.of("instance-2", "instance-3");
    // Using requestId=0 should select replica-group 0. Even segments get assigned to instance-0 and odd segments get
    // assigned to instance-1.
    Map<String, String> expectedSelectorResult = createExpectedAssignment(replicaGroup0, getSegments());
    InstanceSelector.SelectionResult selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Using requestId=1 should select replica-group 1. Even segments get assigned to instance-2 and odd segments get
    // assigned to instance-3.
    expectedSelectorResult = createExpectedAssignment(replicaGroup1, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Update ideal state to replace instance-0 with instance-100 and instance-3 with instance-103
    Map<String, String> oldInstanceMap1 = Map.of("instance-0", ONLINE, "instance-2", ONLINE);
    Map<String, String> newInstanceMap1 = Map.of("instance-100", ONLINE, "instance-2", ONLINE);
    Map<String, String> oldInstanceMap2 = Map.of("instance-1", ONLINE, "instance-3", ONLINE);
    Map<String, String> newInstanceMap2 = Map.of("instance-1", ONLINE, "instance-103", ONLINE);

    idealState.getRecord().getMapFields().replaceAll((k, v) -> {
      if (v.equals(oldInstanceMap1)) {
        return newInstanceMap1;
      } else if (v.equals(oldInstanceMap2)) {
        return newInstanceMap2;
      } else {
        return v;
      }
    });

    multiStageSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Now, since instance-100 and instance-103 aren't available as per the external view, all requests should use
    // a combination of replica-group 0 and 1 (i.e., the available instances - instance-2 and instance-1)
    expectedSelectorResult = createExpectedAssignment(List.of("instance-2", "instance-1"), getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    expectedSelectorResult = createExpectedAssignment(List.of("instance-2", "instance-1"), getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // External view updated
    externalView.getRecord().getMapFields().replaceAll((k, v) -> {
      if (v.equals(oldInstanceMap1)) {
        return newInstanceMap1;
      } else if (v.equals(oldInstanceMap2)) {
        return newInstanceMap2;
      } else {
        return v;
      }
    });

    multiStageSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Now, even request IDs should use replica-group 0 (instance-100, instance-1)
    expectedSelectorResult = createExpectedAssignment(List.of("instance-100", "instance-1"), getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Now, odd request IDs should use replica-group 1 (instance-2, instance-103)
    expectedSelectorResult = createExpectedAssignment(List.of("instance-2", "instance-103"), getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Update ideal state to replace instance-2 with instance-102 and instance-1 with instance-101
    Map<String, String> finalInstanceMap1 = Map.of("instance-100", ONLINE, "instance-102", ONLINE);
    Map<String, String> finalInstanceMap2 = Map.of("instance-101", ONLINE, "instance-103", ONLINE);

    idealState.getRecord().getMapFields().replaceAll((k, v) -> {
      if (v.equals(newInstanceMap1)) {
        return finalInstanceMap1;
      } else if (v.equals(newInstanceMap2)) {
        return finalInstanceMap2;
      } else {
        return v;
      }
    });

    multiStageSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Now, since instance-102 and instance-101 aren't available as per the external view, all requests should use
    // a combination of replica-group 0 and 1 (i.e., the available instances - instance-100 and instance-103)
    expectedSelectorResult = createExpectedAssignment(List.of("instance-100", "instance-103"), getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    expectedSelectorResult = createExpectedAssignment(List.of("instance-100", "instance-103"), getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // External view updated
    externalView.getRecord().getMapFields().replaceAll((k, v) -> {
      if (v.equals(newInstanceMap1)) {
        return finalInstanceMap1;
      } else if (v.equals(newInstanceMap2)) {
        return finalInstanceMap2;
      } else {
        return v;
      }
    });

    multiStageSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Even request IDs should now use the new replica-group 0 (instance-100, instance-101)
    expectedSelectorResult = createExpectedAssignment(newReplicaGroup0, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // Odd request IDs should now use the new replica-group 1 (instance-102, instance-103)
    expectedSelectorResult = createExpectedAssignment(newReplicaGroup1, getSegments());
    selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);
  }

  @Test
  public void testInstancePartitionsIdealStateMismatchSegmentsDropped() {
    // This issue (described in https://github.com/apache/pinot/issues/17179) is fixed by using the instance partitions
    // stored alongside the ideal state which will also reflect intermediate states during a rebalance accurately.
    // However, the older algorithm, based on the INSTANCE_PARTITIONS ZNode is still retained as a fallback because
    // the new ideal state based instance partitions mechanism for query routing can only be used for new tables or
    // tables that are rebalanced with an instance assignment change.

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

  private MultiStageReplicaGroupSelector createMultiStageSelector(InstancePartitions instancePartitions) {
    MultiStageReplicaGroupSelector multiStageSelector = new MultiStageReplicaGroupSelector();
    multiStageSelector = spy(multiStageSelector);
    doReturn(instancePartitions).when(multiStageSelector).getInstancePartitions();
    return multiStageSelector;
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
