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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RealtimeReplicaGroupSegmentAssignmentStrategyTest {
  private static final int NUM_REPLICAS = 3;
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_SEGMENTS = 100;
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
  private SegmentAssignmentStrategy _strategy;

  @BeforeClass
  public void setUp() {
    _segments = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      _segments.add(new LLCSegmentName(RAW_TABLE_NAME, segmentId % NUM_PARTITIONS, segmentId / NUM_PARTITIONS,
          System.currentTimeMillis()).getSegmentName());
    }

    // Consuming instances:
    // {
    //   0_0=[instance_0, instance_1, instance_2],
    //   0_1=[instance_3, instance_4, instance_5],
    //   0_2=[instance_6, instance_7, instance_8]
    // }
    //        p0          p1          p2
    //        p3
    InstancePartitions consumingInstancePartitions = new InstancePartitions(CONSUMING_INSTANCE_PARTITIONS_NAME);
    int numConsumingInstancesPerReplica = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    int consumingInstanceIdToAdd = 0;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> consumingInstancesForReplica = new ArrayList<>(numConsumingInstancesPerReplica);
      for (int i = 0; i < numConsumingInstancesPerReplica; i++) {
        consumingInstancesForReplica.add(CONSUMING_INSTANCES.get(consumingInstanceIdToAdd++));
      }
      consumingInstancePartitions.setInstances(0, replicaId, consumingInstancesForReplica);
    }

    // Completed instances:
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3],
    //   0_1=[instance_4, instance_5, instance_6, instance_7],
    //   0_2=[instance_8, instance_9, instance_10, instance_11]
    // }
    InstancePartitions completedInstancePartitions = new InstancePartitions(COMPLETED_INSTANCE_PARTITIONS_NAME);
    int numCompletedInstancesPerReplica = NUM_COMPLETED_INSTANCES / NUM_REPLICAS;
    int completedInstanceIdToAdd = 0;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> completedInstancesForReplica = new ArrayList<>(numCompletedInstancesPerReplica);
      for (int i = 0; i < numCompletedInstancesPerReplica; i++) {
        completedInstancesForReplica.add(COMPLETED_INSTANCES.get(completedInstanceIdToAdd++));
      }
      completedInstancePartitions.setInstances(0, replicaId, completedInstancesForReplica);
    }

    // Mock HelixManager
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore
        .get(eq(ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(CONSUMING_INSTANCE_PARTITIONS_NAME)),
            any(), anyInt())).thenReturn(consumingInstancePartitions.toZNRecord());
    when(propertyStore
        .get(eq(ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(COMPLETED_INSTANCE_PARTITIONS_NAME)),
            any(), anyInt())).thenReturn(completedInstancePartitions.toZNRecord());
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    TableConfig tableConfig =
        new TableConfig.Builder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setLLC(true).setSegmentAssignmentStrategy(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY)
            .build();
    _strategy = SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(helixManager, tableConfig);
  }

  @Test
  public void testFactory() {
    assertTrue(_strategy instanceof RealtimeReplicaGroupSegmentAssignmentStrategy);
  }

  @Test
  public void testAssignSegment() {
    int numInstancesPerReplica = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned = _strategy.assignSegment(segmentName, currentAssignment);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {

        // Segment 0 (partition 0) should be assigned to instance 0, 3, 6
        // Segment 1 (partition 1) should be assigned to instance 1, 4, 7
        // Segment 2 (partition 2) should be assigned to instance 2, 5, 8
        // Segment 3 (partition 3) should be assigned to instance 0, 3, 6
        // Segment 4 (partition 0) should be assigned to instance 0, 3, 6
        // Segment 5 (partition 1) should be assigned to instance 1, 4, 7
        // ...
        int partitionId = segmentId % NUM_PARTITIONS;
        int expectedAssignedInstanceId = partitionId % numInstancesPerReplica + replicaId * numInstancesPerReplica;
        assertEquals(instancesAssigned.get(replicaId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
      }
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }
  }

  @Test
  public void testRelocateCompletedSegments() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned = _strategy.assignSegment(segmentName, currentAssignment);
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }

    // There should be 100 segments assigned
    assertEquals(currentAssignment.size(), NUM_SEGMENTS);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }

    // Rebalance should relocate all completed (ONLINE) segments to the completed instances
    Map<String, Map<String, String>> newAssignment =
        _strategy.rebalanceTable(currentAssignment, new BaseConfiguration());
    assertEquals(newAssignment.size(), NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      if (segmentId < NUM_SEGMENTS - NUM_PARTITIONS) {
        // Completed (ONLINE) segments
        Map<String, String> instanceStateMap = newAssignment.get(_segments.get(segmentId));
        for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
          assertTrue(entry.getKey().startsWith(COMPLETED_INSTANCE_NAME_PREFIX));
          assertEquals(entry.getValue(), RealtimeSegmentOnlineOfflineStateModel.ONLINE);
        }
      } else {
        // Consuming (CONSUMING) segments
        Map<String, String> instanceStateMap = newAssignment.get(_segments.get(segmentId));
        for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
          assertTrue(entry.getKey().startsWith(CONSUMING_INSTANCE_NAME_PREFIX));
          assertEquals(entry.getValue(), RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
        }
      }
    }
    // Relocated segments should be balanced (each instance should have 24 segments assigned)
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, COMPLETED_INSTANCES);
    int[] expectedNumSegmentsAssignedPerInstance = new int[NUM_COMPLETED_INSTANCES];
    int numSegmentsPerInstance = (NUM_SEGMENTS - NUM_PARTITIONS) * NUM_REPLICAS / NUM_COMPLETED_INSTANCES;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);

    // Rebalance all segments (both completed and consuming) should give the same assignment
    BaseConfiguration config = new BaseConfiguration();
    config.setProperty(RebalanceUserConfigConstants.INCLUDE_CONSUMING, true);
    assertEquals(_strategy.rebalanceTable(currentAssignment, config), newAssignment);

    // Rebalance should not change the assignment for the OFFLINE segments
    String offlineSegmentName = "offlineSegment";
    Map<String, String> offlineSegmentInstanceStateMap = SegmentAssignmentUtils
        .getInstanceStateMap(SegmentAssignmentTestUtils.getNameList("badInstance_", NUM_REPLICAS),
            RealtimeSegmentOnlineOfflineStateModel.OFFLINE);
    currentAssignment.put(offlineSegmentName, offlineSegmentInstanceStateMap);
    newAssignment.put(offlineSegmentName, offlineSegmentInstanceStateMap);
    assertEquals(_strategy.rebalanceTable(currentAssignment, config), newAssignment);
  }

  private void addToAssignment(Map<String, Map<String, String>> currentAssignment, int segmentId,
      List<String> instancesAssigned) {
    // Change the state of the last segment in the same partition from CONSUMING to ONLINE if exists
    if (segmentId >= NUM_PARTITIONS) {
      String lastSegmentInPartition = _segments.get(segmentId - NUM_PARTITIONS);
      Map<String, String> instanceStateMap = currentAssignment.get(lastSegmentInPartition);
      currentAssignment.put(lastSegmentInPartition, SegmentAssignmentUtils
          .getInstanceStateMap(new ArrayList<>(instanceStateMap.keySet()),
              RealtimeSegmentOnlineOfflineStateModel.ONLINE));
    }

    // Add the new segment into the assignment as CONSUMING
    currentAssignment.put(_segments.get(segmentId), SegmentAssignmentUtils
        .getInstanceStateMap(instancesAssigned, RealtimeSegmentOnlineOfflineStateModel.CONSUMING));
  }
}
