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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class MultiStageReplicaGroupSelectorTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private final static List<String> SEGMENTS =
      Arrays.asList("segment0", "segment1", "segment2", "segment3", "segment4", "segment5", "segment6", "segment7",
          "segment8", "segment9", "segment10", "segment11");
  private static final Map<String, ServerInstance> EMPTY_SERVER_MAP = Collections.EMPTY_MAP;
  private AutoCloseable _mocks;
  @Mock
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  @Mock
  private BrokerMetrics _brokerMetrics;
  @Mock
  private BrokerRequest _brokerRequest;
  @Mock
  private PinotQuery _pinotQuery;

  private static List<String> getSegments() {
    return SEGMENTS;
  }

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_brokerRequest.getPinotQuery()).thenReturn(_pinotQuery);
    when(_pinotQuery.getQueryOptions()).thenReturn(null);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    _mocks.close();
  }

  @Test
  public void testBasicReplicaGroupSelection() {
    // Create instance-partitions with two replica-groups and 1 partition. Each replica-group has 2 instances.
    List<String> replicaGroup0 = ImmutableList.of("instance-0", "instance-1");
    List<String> replicaGroup1 = ImmutableList.of("instance-2", "instance-3");
    InstancePartitions instancePartitions = createInstancePartitions(replicaGroup0, replicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    List<String> enabledInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();

    setupBasicTestEnvironment(enabledInstances, idealState, externalView, onlineSegments);
    multiStageSelector.init(new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
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
  }

  @Test
  public void testInstanceFailureHandling() {
    // Create instance-partitions with two replica-groups and 1 partition. Each replica-group has 2 instances.
    List<String> replicaGroup0 = ImmutableList.of("instance-0", "instance-1");
    List<String> replicaGroup1 = ImmutableList.of("instance-2", "instance-3");
    InstancePartitions instancePartitions = createInstancePartitions(replicaGroup0, replicaGroup1);
    MultiStageReplicaGroupSelector multiStageSelector = createMultiStageSelector(instancePartitions);

    List<String> enabledInstances = createEnabledInstances(4);
    IdealState idealState = new IdealState(TABLE_NAME);
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Set<String> onlineSegments = new HashSet<>();

    setupBasicTestEnvironment(enabledInstances, idealState, externalView, onlineSegments);
    multiStageSelector.init(new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);

    // If instance-0 is down, replica-group 1 should be picked even with requestId=0
    enabledInstances.remove("instance-0");
    multiStageSelector.init(new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
    Map<String, String> expectedSelectorResult = createExpectedAssignment(replicaGroup1, getSegments());
    InstanceSelector.SelectionResult selectionResult = multiStageSelector.select(_brokerRequest, getSegments(), 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectorResult);

    // If instance-2 also goes down, no replica-group is eligible
    enabledInstances.remove("instance-2");
    multiStageSelector.init(new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
    try {
      multiStageSelector.select(_brokerRequest, getSegments(), 0);
      fail("Method call above should have failed");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testErrorSegmentHandling() {
    // Create instance-partitions with two replica-groups and 2 partitions. Each replica-group has 2 instances.
    Map<String, List<String>> partitionToInstances = ImmutableMap.of(
        "0_0", ImmutableList.of("instance-0"),
        "0_1", ImmutableList.of("instance-2"),
        "1_0", ImmutableList.of("instance-1"),
        "1_1", ImmutableList.of("instance-3"));
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

    multiStageSelector.init(new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
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
    multiStageSelector.init(new HashSet<>(enabledInstances), EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
    try {
      multiStageSelector.select(_brokerRequest, getSegments(), 0);
      fail("Method call above should have failed");
    } catch (Exception ignored) {
    }
  }

  private MultiStageReplicaGroupSelector createMultiStageSelector(InstancePartitions instancePartitions) {
    MultiStageReplicaGroupSelector multiStageSelector =
        new MultiStageReplicaGroupSelector(TABLE_NAME, _propertyStore, _brokerMetrics, null, Clock.systemUTC(),
            false, 300);
    multiStageSelector = spy(multiStageSelector);
    doReturn(instancePartitions).when(multiStageSelector).getInstancePartitions();
    return multiStageSelector;
  }

  private InstancePartitions createInstancePartitions(List<String> replicaGroup0, List<String> replicaGroup1) {
    Map<String, List<String>> partitionToInstances = ImmutableMap.of("0_0", replicaGroup0, "0_1", replicaGroup1);
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
