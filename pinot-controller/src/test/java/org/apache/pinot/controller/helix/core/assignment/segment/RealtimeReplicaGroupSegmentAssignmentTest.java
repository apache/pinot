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
package org.apache.pinot.controller.helix.core.assignment.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RealtimeReplicaGroupSegmentAssignmentTest {
  private static final int NUM_REPLICAS = 3;
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_SEGMENTS = 24;
  private static final String CONSUMING_INSTANCE_NAME_PREFIX = "consumingInstance_";
  private static final int NUM_CONSUMING_INSTANCES = 9;
  private static final List<String> CONSUMING_INSTANCES =
      SegmentAssignmentTestUtils.getNameList(CONSUMING_INSTANCE_NAME_PREFIX, NUM_CONSUMING_INSTANCES);
  private static final String COMPLETED_INSTANCE_NAME_PREFIX = "completedInstance_";
  private static final int NUM_COMPLETED_INSTANCES = 12;
  private static final List<String> COMPLETED_INSTANCES =
      SegmentAssignmentTestUtils.getNameList(COMPLETED_INSTANCE_NAME_PREFIX, NUM_COMPLETED_INSTANCES);
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String CONSUMING_INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.CONSUMING.getInstancePartitionsName(RAW_TABLE_NAME);
  private static final String COMPLETED_INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.COMPLETED.getInstancePartitionsName(RAW_TABLE_NAME);

  private List<String> _segments;
  private SegmentAssignment _segmentAssignment;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMap;

  @BeforeClass
  public void setUp() {
    _segments = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      _segments.add(new LLCSegmentName(RAW_TABLE_NAME, segmentId % NUM_PARTITIONS, segmentId / NUM_PARTITIONS,
          System.currentTimeMillis()).getSegmentName());
    }

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setLLC(true).setSegmentAssignmentStrategy(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY)
            .build();
    _segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(createHelixManager(), tableConfig);

    _instancePartitionsMap = new TreeMap<>();
    // CONSUMING instances:
    // {
    //   0_0=[instance_0, instance_1, instance_2],
    //   0_1=[instance_3, instance_4, instance_5],
    //   0_2=[instance_6, instance_7, instance_8]
    // }
    //        p0          p1          p2
    //        p3
    InstancePartitions consumingInstancePartitions = new InstancePartitions(CONSUMING_INSTANCE_PARTITIONS_NAME);
    int numConsumingInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    int consumingInstanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> consumingInstancesForReplicaGroup = new ArrayList<>(numConsumingInstancesPerReplicaGroup);
      for (int i = 0; i < numConsumingInstancesPerReplicaGroup; i++) {
        consumingInstancesForReplicaGroup.add(CONSUMING_INSTANCES.get(consumingInstanceIdToAdd++));
      }
      consumingInstancePartitions.setInstances(0, replicaGroupId, consumingInstancesForReplicaGroup);
    }
    _instancePartitionsMap.put(InstancePartitionsType.CONSUMING, consumingInstancePartitions);

    // COMPLETED instances:
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3],
    //   0_1=[instance_4, instance_5, instance_6, instance_7],
    //   0_2=[instance_8, instance_9, instance_10, instance_11]
    // }
    InstancePartitions completedInstancePartitions = new InstancePartitions(COMPLETED_INSTANCE_PARTITIONS_NAME);
    int numCompletedInstancesPerReplicaGroup = NUM_COMPLETED_INSTANCES / NUM_REPLICAS;
    int completedInstanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> completedInstancesForReplicaGroup = new ArrayList<>(numCompletedInstancesPerReplicaGroup);
      for (int i = 0; i < numCompletedInstancesPerReplicaGroup; i++) {
        completedInstancesForReplicaGroup.add(COMPLETED_INSTANCES.get(completedInstanceIdToAdd++));
      }
      completedInstancePartitions.setInstances(0, replicaGroupId, completedInstancesForReplicaGroup);
    }
    _instancePartitionsMap.put(InstancePartitionsType.COMPLETED, completedInstancePartitions);
  }

  @Test
  public void testAssignSegment() {
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        ImmutableMap.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    int numInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);

      // Segment 0 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 1 (partition 1) should be assigned to instance 1, 4, 7
      // Segment 2 (partition 2) should be assigned to instance 2, 5, 8
      // Segment 3 (partition 3) should be assigned to instance 0, 3, 6
      // Segment 4 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 5 (partition 1) should be assigned to instance 1, 4, 7
      // ...
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int partitionId = segmentId % NUM_PARTITIONS;
        int expectedAssignedInstanceId =
            partitionId % numInstancesPerReplicaGroup + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
      }

      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }
  }

  @Test
  public void testRelocateCompletedSegments() {
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        ImmutableMap.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }

    // There should be 100 segments assigned
    assertEquals(currentAssignment.size(), NUM_SEGMENTS);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }

    // Add an OFFLINE segment on some bad instances
    String offlineSegmentName = "offlineSegment";
    Map<String, String> offlineSegmentInstanceStateMap = SegmentAssignmentUtils
        .getInstanceStateMap(SegmentAssignmentTestUtils.getNameList("badInstance_", NUM_REPLICAS),
            SegmentStateModel.OFFLINE);
    currentAssignment.put(offlineSegmentName, offlineSegmentInstanceStateMap);

    // Add 3 uploaded ONLINE segments to the consuming instances (i.e. no separation between consuming & completed)
    List<String> uploadedSegmentNames = ImmutableList.of("UploadedSegment0", "UploadedSegment1", "UploadedSegment2");
    onlyConsumingInstancePartitionMap =
        ImmutableMap.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    for (String uploadedSegName : uploadedSegmentNames) {
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(uploadedSegName, currentAssignment, onlyConsumingInstancePartitionMap);
      currentAssignment.put(uploadedSegName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    assertEquals(currentAssignment.size(), NUM_SEGMENTS + uploadedSegmentNames.size() + 1);
    // Each segment should have 3 replicas and all assigned instances should be prefixed with consuming
    currentAssignment.forEach((type, instanceStateMap) -> {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
      instanceStateMap.forEach((instance, state) -> {
        if (!instance.startsWith("badInstance_")) {
          assertTrue(instance.startsWith(CONSUMING_INSTANCE_NAME_PREFIX));
        }
      });
    });

    // Rebalance without COMPLETED instance partitions should not change the segment assignment
    Map<InstancePartitionsType, InstancePartitions> noRelocationInstancePartitionsMap =
        ImmutableMap.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    assertEquals(_segmentAssignment
            .rebalanceTable(currentAssignment, noRelocationInstancePartitionsMap, null, null, new BaseConfiguration()),
        currentAssignment);

    // Rebalance with COMPLETED instance partitions should relocate all COMPLETED (ONLINE) segments to the COMPLETED
    // instances
    Map<String, Map<String, String>> newAssignment = _segmentAssignment
        .rebalanceTable(currentAssignment, _instancePartitionsMap, null, null, new BaseConfiguration());
    assertEquals(newAssignment.size(), NUM_SEGMENTS + uploadedSegmentNames.size() + 1);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      if (segmentId < NUM_SEGMENTS - NUM_PARTITIONS) {
        // check COMPLETED (ONLINE) segments
        newAssignment.get(_segments.get(segmentId)).forEach((instance, state) -> {
          assertTrue(instance.startsWith(COMPLETED_INSTANCE_NAME_PREFIX));
          assertEquals(state, SegmentStateModel.ONLINE);
        });
      } else {
        // check CONSUMING segments
        newAssignment.get(_segments.get(segmentId)).forEach((instance, state) -> {
          assertTrue(instance.startsWith(CONSUMING_INSTANCE_NAME_PREFIX));
          assertEquals(state, SegmentStateModel.CONSUMING);
        });
      }
    }
    // check the uploaded segments
    for (String uploadedSegName : uploadedSegmentNames) {
      newAssignment.get(uploadedSegName).forEach((instance, state) -> {
        assertTrue(instance.startsWith(COMPLETED_INSTANCE_NAME_PREFIX));
        assertEquals(state, SegmentStateModel.ONLINE);
      });
    }

    // Relocated segments should be balanced
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, COMPLETED_INSTANCES);
    int expectedNumSegmentsPerInstance = (NUM_SEGMENTS - NUM_PARTITIONS) * NUM_REPLICAS / NUM_COMPLETED_INSTANCES;
    for (int actualNumSegments : numSegmentsAssignedPerInstance) {
      assertTrue(actualNumSegments == expectedNumSegmentsPerInstance
          || actualNumSegments == expectedNumSegmentsPerInstance + 1);
    }

    // Rebalance with COMPLETED instance partitions including CONSUMING segments should give the same assignment
    BaseConfiguration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.INCLUDE_CONSUMING, true);
    assertEquals(
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, null, null, rebalanceConfig),
        newAssignment);

    // Rebalance without COMPLETED instance partitions again should change the segment assignment back
    assertEquals(_segmentAssignment
            .rebalanceTable(newAssignment, noRelocationInstancePartitionsMap, null, null, new BaseConfiguration()),
        currentAssignment);

    // Bootstrap table without COMPLETED instance partitions should be the same as regular rebalance
    rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.BOOTSTRAP, true);
    assertEquals(_segmentAssignment
            .rebalanceTable(currentAssignment, noRelocationInstancePartitionsMap, null, null, rebalanceConfig),
        currentAssignment);
    assertEquals(_segmentAssignment
            .rebalanceTable(newAssignment, noRelocationInstancePartitionsMap, null, null, rebalanceConfig),
        currentAssignment);

    // Bootstrap table with COMPLETED instance partitions should reassign all COMPLETED segments based on their
    // alphabetical order
    newAssignment =
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, null, null, rebalanceConfig);
    int numCompletedInstancesPerReplicaGroup = NUM_COMPLETED_INSTANCES / NUM_REPLICAS;
    int index = 0;
    for (Map.Entry<String, Map<String, String>> entry : newAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      if (instanceStateMap.containsValue(SegmentStateModel.ONLINE)) {
        int expectedInstanceId = index++ % numCompletedInstancesPerReplicaGroup;
        for (int i = 0; i < NUM_REPLICAS; i++) {
          String expectedInstance =
              COMPLETED_INSTANCES.get(expectedInstanceId + i * numCompletedInstancesPerReplicaGroup);
          assertEquals(instanceStateMap.get(expectedInstance), SegmentStateModel.ONLINE);
        }
      } else {
        // CONSUMING and OFFLINE segments should not be reassigned
        assertEquals(instanceStateMap, currentAssignment.get(segmentName));
      }
    }
  }

  @Test
  public void testAssignSegmentForUploadedSegments() {
    // CONSUMING instance partition has been tested in previous method, only test COMPLETED here
    Map<InstancePartitionsType, InstancePartitions> onlyCompletedInstancePartitionMap =
        ImmutableMap.of(InstancePartitionsType.COMPLETED, _instancePartitionsMap.get(InstancePartitionsType.COMPLETED));
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    // COMPLETED instances:
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3],
    //   0_1=[instance_4, instance_5, instance_6, instance_7],
    //   0_2=[instance_8, instance_9, instance_10, instance_11]
    // }
    Map<String, List<String>> expectedUploadedSegmentToInstances = ImmutableMap.of(
        "uploadedSegment_0", ImmutableList.of("completedInstance_0", "completedInstance_4", "completedInstance_8"),
        "uploadedSegment_1", ImmutableList.of("completedInstance_1", "completedInstance_5", "completedInstance_9"),
        "uploadedSegment_2", ImmutableList.of("completedInstance_2", "completedInstance_6", "completedInstance_10"),
        "uploadedSegment_3", ImmutableList.of("completedInstance_3", "completedInstance_7", "completedInstance_11"),
        "uploadedSegment_4", ImmutableList.of("completedInstance_0", "completedInstance_4", "completedInstance_8")
    );
    expectedUploadedSegmentToInstances.forEach((segmentName, expectedInstances) -> {
      List<String> actualInstances =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, onlyCompletedInstancePartitionMap);
      assertEquals(actualInstances, expectedInstances);
      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(actualInstances, SegmentStateModel.ONLINE));
    });
  }

  private void addToAssignment(Map<String, Map<String, String>> currentAssignment, int segmentId,
      List<String> instancesAssigned) {
    // Change the state of the last segment in the same partition from CONSUMING to ONLINE if exists
    if (segmentId >= NUM_PARTITIONS) {
      String lastSegmentInPartition = _segments.get(segmentId - NUM_PARTITIONS);
      Map<String, String> instanceStateMap = currentAssignment.get(lastSegmentInPartition);
      currentAssignment.put(lastSegmentInPartition, SegmentAssignmentUtils
          .getInstanceStateMap(new ArrayList<>(instanceStateMap.keySet()), SegmentStateModel.ONLINE));
    }

    // Add the new segment into the assignment as CONSUMING
    currentAssignment.put(_segments.get(segmentId),
        SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.CONSUMING));
  }

  private HelixManager createHelixManager() {
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    when(propertyStore.get(anyString(), isNull(), anyInt())).thenReturn(new ZNRecord("0"));
    return helixManager;
  }
}
