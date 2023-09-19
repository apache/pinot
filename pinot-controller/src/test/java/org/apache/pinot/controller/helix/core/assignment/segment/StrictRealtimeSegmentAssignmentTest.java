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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
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


@SuppressWarnings("unchecked")
public class StrictRealtimeSegmentAssignmentTest {
  private static final int NUM_REPLICAS = 3;
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_SEGMENTS = 24;
  private static final String CONSUMING_INSTANCE_NAME_PREFIX = "consumingInstance_";
  private static final int NUM_CONSUMING_INSTANCES = 9;
  private static final List<String> CONSUMING_INSTANCES =
      SegmentAssignmentTestUtils.getNameList(CONSUMING_INSTANCE_NAME_PREFIX, NUM_CONSUMING_INSTANCES);
  private static final List<String> NEW_CONSUMING_INSTANCES =
      SegmentAssignmentTestUtils.getNameList("new_" + CONSUMING_INSTANCE_NAME_PREFIX, NUM_CONSUMING_INSTANCES);
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String CONSUMING_INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.CONSUMING.getInstancePartitionsName(RAW_TABLE_NAME);

  private List<String> _segments;
  private SegmentAssignment _segmentAssignment;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMap;
  private InstancePartitions _newConsumingInstancePartitions;

  @BeforeClass
  public void setUp() {
    _segments = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      _segments.add(new LLCSegmentName(RAW_TABLE_NAME, segmentId % NUM_PARTITIONS, segmentId / NUM_PARTITIONS,
          System.currentTimeMillis()).getSegmentName());
    }

    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setStreamConfigs(streamConfigs).setUpsertConfig(upsertConfig)
            .setSegmentAssignmentStrategy(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY)
            .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(PARTITION_COLUMN, 1)).build();
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

    // new CONSUMING instances:
    // {
    //   0_0=[new_instance_0, new_instance_1, new_instance_2],
    //   0_1=[new_instance_3, new_instance_4, new_instance_5],
    //   0_2=[new_instance_6, new_instance_7, new_instance_8]
    // }
    consumingInstanceIdToAdd = 0;
    _newConsumingInstancePartitions = new InstancePartitions(CONSUMING_INSTANCE_PARTITIONS_NAME);
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> consumingInstancesForReplicaGroup = new ArrayList<>(numConsumingInstancesPerReplicaGroup);
      for (int i = 0; i < numConsumingInstancesPerReplicaGroup; i++) {
        consumingInstancesForReplicaGroup.add(NEW_CONSUMING_INSTANCES.get(consumingInstanceIdToAdd++));
      }
      _newConsumingInstancePartitions.setInstances(0, replicaGroupId, consumingInstancesForReplicaGroup);
    }
  }

  @Test
  public void testFactory() {
    assertTrue(_segmentAssignment instanceof StrictRealtimeSegmentAssignment);
  }

  @Test
  public void testAssignSegment() {
    assertTrue(_segmentAssignment instanceof StrictRealtimeSegmentAssignment);
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        ImmutableMap.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    int numInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    // Add segments for partition 0/1/2, but add no segment for partition 3.
    for (int segmentId = 0; segmentId < 3; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      // Segment 0 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 1 (partition 1) should be assigned to instance 1, 4, 7
      // Segment 2 (partition 2) should be assigned to instance 2, 5, 8
      // Following segments are assigned to those instances if continue to use the same instancePartition
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
    // Use new instancePartition to assign the new segments below.
    ImmutableMap<InstancePartitionsType, InstancePartitions> newConsumingInstancePartitionMap =
        ImmutableMap.of(InstancePartitionsType.CONSUMING, _newConsumingInstancePartitions);

    // No existing segments for partition 3, so use the assignment decided by new instancePartition.
    // So segment 3 (partition 3) should be assigned to instance new_0, new_3, new_6
    int segmentId = 3;
    String segmentName = _segments.get(segmentId);
    List<String> instancesAssigned =
        _segmentAssignment.assignSegment(segmentName, currentAssignment, newConsumingInstancePartitionMap);
    assertEquals(instancesAssigned,
        Arrays.asList("new_consumingInstance_0", "new_consumingInstance_3", "new_consumingInstance_6"));

    // Use existing assignment for partition 0/1/2, instead of the one decided by new instancePartition.
    for (segmentId = 4; segmentId < 7; segmentId++) {
      segmentName = _segments.get(segmentId);
      instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, newConsumingInstancePartitionMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);

      // Those segments are assigned according to the assignment from idealState, instead of using new_xxx instances
      // Segment 4 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 5 (partition 1) should be assigned to instance 1, 4, 7
      // Segment 6 (partition 2) should be assigned to instance 2, 5, 8
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int partitionId = segmentId % NUM_PARTITIONS;
        int expectedAssignedInstanceId =
            partitionId % numInstancesPerReplicaGroup + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
      }
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testAssignSegmentToCompletedServers() {
    _segmentAssignment.assignSegment("seg01", new TreeMap<>(), new TreeMap<>());
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
