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
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
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


public class RealtimeNonReplicaGroupSegmentAssignmentTest {
  private static final int NUM_REPLICAS = 3;
  private static final String NUM_REPLICAS_PER_PARTITION = "4";
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_SEGMENTS = 100;
  private static final String CONSUMING_INSTANCE_NAME_PREFIX = "consumingInstance_";
  private static final int NUM_CONSUMING_INSTANCES = 9;
  private static final List<String> CONSUMING_INSTANCES =
      SegmentAssignmentTestUtils.getNameList(CONSUMING_INSTANCE_NAME_PREFIX, NUM_CONSUMING_INSTANCES);
  private static final String COMPLETED_INSTANCE_NAME_PREFIX = "completedInstance_";
  private static final int NUM_COMPLETED_INSTANCES = 10;
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

    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setStreamConfigs(streamConfigs).build();
    _segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(createHelixManager(), tableConfig);

    _instancePartitionsMap = new TreeMap<>();
    // CONSUMING instances:
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7,
    //   instance_8]
    // }
    //        p0r0        p0r1        p0r2        p1r0        p1r1        p1r2        p2r0        p2r1        p2r2
    //        p3r0        p3r1        p3r2
    InstancePartitions consumingInstancePartitions = new InstancePartitions(CONSUMING_INSTANCE_PARTITIONS_NAME);
    consumingInstancePartitions.setInstances(0, 0, CONSUMING_INSTANCES);
    _instancePartitionsMap.put(InstancePartitionsType.CONSUMING, consumingInstancePartitions);

    // COMPLETED instances:
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7,
    //   instance_8, instance_9]
    // }
    InstancePartitions completedInstancePartitions = new InstancePartitions(COMPLETED_INSTANCE_PARTITIONS_NAME);
    completedInstancePartitions.setInstances(0, 0, COMPLETED_INSTANCES);
    _instancePartitionsMap.put(InstancePartitionsType.COMPLETED, completedInstancePartitions);
  }

  @Test
  public void testFactory() {
    assertTrue(_segmentAssignment instanceof RealtimeSegmentAssignment);
  }

  @Test
  public void testReplicationForSegmentAssignment() {
    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setStreamConfigs(streamConfigs).build();
    // Update the replication by changing the NUM_REPLICAS_PER_PARTITION
    tableConfig.getValidationConfig().setReplicasPerPartition(NUM_REPLICAS_PER_PARTITION);
    SegmentAssignment segmentAssignment =
        SegmentAssignmentFactory.getSegmentAssignment(createHelixManager(), tableConfig);

    Map<InstancePartitionsType, InstancePartitions> onlyCompletedInstancePartitionMap =
        ImmutableMap.of(InstancePartitionsType.COMPLETED, _instancePartitionsMap.get(InstancePartitionsType.COMPLETED));
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    Map<String, List<String>> expectedUploadedSegmentToInstances = ImmutableMap.of("uploadedSegment_0",
        ImmutableList.of("completedInstance_0", "completedInstance_1", "completedInstance_2", "completedInstance_3"),
        "uploadedSegment_1",
        ImmutableList.of("completedInstance_4", "completedInstance_5", "completedInstance_6", "completedInstance_7"),
        "uploadedSegment_2",
        ImmutableList.of("completedInstance_8", "completedInstance_9", "completedInstance_0", "completedInstance_1"),
        "uploadedSegment_3",
        ImmutableList.of("completedInstance_2", "completedInstance_3", "completedInstance_4", "completedInstance_5"));

    expectedUploadedSegmentToInstances.forEach((segmentName, expectedInstances) -> {
      List<String> actualInstances =
          segmentAssignment.assignSegment(segmentName, currentAssignment, onlyCompletedInstancePartitionMap);
      assertEquals(actualInstances, expectedInstances);
      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(actualInstances, SegmentStateModel.ONLINE));
    });
  }

  @Test
  public void testAssignSegment() {
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        ImmutableMap.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);

      // Segment 0 (partition 0) should be assigned to instance 0, 1, 2
      // Segment 1 (partition 1) should be assigned to instance 3, 4, 5
      // Segment 2 (partition 2) should be assigned to instance 6, 7, 8
      // Segment 3 (partition 3) should be assigned to instance 0, 1, 2
      // Segment 4 (partition 0) should be assigned to instance 0, 1, 2
      // Segment 5 (partition 1) should be assigned to instance 3, 4, 5
      // ...
      int partitionId = segmentId % NUM_PARTITIONS;
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        int expectedAssignedInstanceId = (partitionId * NUM_REPLICAS + replicaId) % NUM_CONSUMING_INSTANCES;
        assertEquals(instancesAssigned.get(replicaId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
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
    Map<String, String> offlineSegmentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(SegmentAssignmentTestUtils.getNameList("badInstance_", NUM_REPLICAS),
            SegmentStateModel.OFFLINE);
    currentAssignment.put(offlineSegmentName, offlineSegmentInstanceStateMap);

    // Add an UPLOADED segment to the CONSUMING instances (i.e. no separation between CONSUMING & COMPLETED)
    List<String> uploadedSegments = ImmutableList.of("UploadedSegment1", "UploadedSegment2");
    int numUploadedSegments = uploadedSegments.size();
    onlyConsumingInstancePartitionMap =
        ImmutableMap.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    for (String segmentName : uploadedSegments) {
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
    // Now there should be 103 segments assigned
    assertEquals(currentAssignment.size(), NUM_SEGMENTS + numUploadedSegments + 1);
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
    assertEquals(_segmentAssignment.rebalanceTable(currentAssignment, noRelocationInstancePartitionsMap, null, null,
        new BaseConfiguration()), currentAssignment);

    // Rebalance with COMPLETED instance partitions should relocate all COMPLETED (ONLINE) segments to the COMPLETED
    // instances
    Map<String, Map<String, String>> newAssignment =
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, null, null,
            new BaseConfiguration());
    assertEquals(newAssignment.size(), NUM_SEGMENTS + numUploadedSegments + 1);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      if (segmentId < NUM_SEGMENTS - NUM_PARTITIONS) {
        // Check the COMPLETED (ONLINE) segments
        newAssignment.get(_segments.get(segmentId)).forEach((instance, state) -> {
          assertTrue(instance.startsWith(COMPLETED_INSTANCE_NAME_PREFIX));
          assertEquals(state, SegmentStateModel.ONLINE);
        });
      } else {
        // Check the CONSUMING segments
        newAssignment.get(_segments.get(segmentId)).forEach((instance, state) -> {
          assertTrue(instance.startsWith(CONSUMING_INSTANCE_NAME_PREFIX));
          assertEquals(state, SegmentStateModel.CONSUMING);
        });
      }
    }
    // Check the UPLOADED (ONLINE) segments
    for (String segmentName : uploadedSegments) {
      newAssignment.get(segmentName).forEach((instance, state) -> {
        assertTrue(instance.startsWith(COMPLETED_INSTANCE_NAME_PREFIX));
        assertEquals(state, SegmentStateModel.ONLINE);
      });
    }

    // The number of relocated segments should match with the number of the original segments.
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, COMPLETED_INSTANCES);
    assertEquals(numSegmentsAssignedPerInstance.length, NUM_COMPLETED_INSTANCES);
    int expectedTotalNumSegmentsAssigned = (NUM_SEGMENTS - NUM_PARTITIONS + numUploadedSegments) * NUM_REPLICAS;
    int totalNumSegmentsAssigned = 0;
    for (int numSegmentsAssigned : numSegmentsAssignedPerInstance) {
      totalNumSegmentsAssigned += numSegmentsAssigned;
    }
    assertEquals(totalNumSegmentsAssigned, expectedTotalNumSegmentsAssigned);

    // Rebalance with COMPLETED instance partitions including CONSUMING segments should give the same assignment
    BaseConfiguration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.INCLUDE_CONSUMING, true);
    assertEquals(
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, null, null, rebalanceConfig),
        newAssignment);

    // Rebalance without COMPLETED instance partitions again should change the segment assignment back
    currentAssignment.put(offlineSegmentName, offlineSegmentInstanceStateMap);
    assertEquals(_segmentAssignment.rebalanceTable(newAssignment, noRelocationInstancePartitionsMap, null, null,
        new BaseConfiguration()), currentAssignment);

    // Bootstrap table without COMPLETED instance partitions should be the same as regular rebalance
    rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.BOOTSTRAP, true);
    assertEquals(_segmentAssignment.rebalanceTable(currentAssignment, noRelocationInstancePartitionsMap, null, null,
        rebalanceConfig), currentAssignment);
    assertEquals(_segmentAssignment.rebalanceTable(newAssignment, noRelocationInstancePartitionsMap, null, null,
        rebalanceConfig), currentAssignment);

    // Bootstrap table with COMPLETED instance partitions should reassign all COMPLETED segments based on their
    // alphabetical order
    newAssignment =
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, null, null, rebalanceConfig);
    int index = 0;
    for (Map.Entry<String, Map<String, String>> entry : newAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      if (instanceStateMap.containsValue(SegmentStateModel.ONLINE)) {
        for (int i = 0; i < NUM_REPLICAS; i++) {
          String expectedInstance = COMPLETED_INSTANCES.get(index++ % NUM_COMPLETED_INSTANCES);
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

    //@formatter:off
    Map<String, List<String>> expectedUploadedSegmentToInstances = ImmutableMap.of(
        "uploadedSegment_0", ImmutableList.of("completedInstance_0", "completedInstance_1", "completedInstance_2"),
        "uploadedSegment_1", ImmutableList.of("completedInstance_3", "completedInstance_4", "completedInstance_5"),
        "uploadedSegment_2", ImmutableList.of("completedInstance_6", "completedInstance_7", "completedInstance_8"),
        "uploadedSegment_3", ImmutableList.of("completedInstance_9", "completedInstance_0", "completedInstance_1"),
        "uploadedSegment_4", ImmutableList.of("completedInstance_2", "completedInstance_3", "completedInstance_4")
    );
    //@formatter:on
    expectedUploadedSegmentToInstances.forEach((segmentName, expectedInstances) -> {
      List<String> actualInstances =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, onlyCompletedInstancePartitionMap);
      assertEquals(actualInstances, expectedInstances);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(actualInstances, SegmentStateModel.ONLINE));
    });
  }

  private void addToAssignment(Map<String, Map<String, String>> currentAssignment, int segmentId,
      List<String> instancesAssigned) {
    // Change the state of the last segment in the same partition from CONSUMING to ONLINE if exists
    if (segmentId >= NUM_PARTITIONS) {
      String lastSegmentInPartition = _segments.get(segmentId - NUM_PARTITIONS);
      Map<String, String> instanceStateMap = currentAssignment.get(lastSegmentInPartition);
      currentAssignment.put(lastSegmentInPartition,
          SegmentAssignmentUtils.getInstanceStateMap(new ArrayList<>(instanceStateMap.keySet()),
              SegmentStateModel.ONLINE));
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
