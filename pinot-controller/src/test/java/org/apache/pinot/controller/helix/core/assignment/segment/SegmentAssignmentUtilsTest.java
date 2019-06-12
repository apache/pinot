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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentAssignmentUtilsTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final String INSTANCE_NAME_PREFIX = "instance_";

  @Test
  public void testRebalanceTableWithHelixAutoRebalanceStrategy() {
    int numSegments = 100;
    List<String> segments = SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, numSegments);
    int numInstances = 10;
    List<String> instances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, numInstances);

    // Uniformly spray segments to the instances:
    // [instance_0,   instance_1,   instance_2,   instance_3,   instance_4,   instance_5,   instance_6,   instance_7,   instance_8,   instance_9]
    //  segment_0(r0) segment_0(r1) segment_0(r2) segment_1(r0) segment_1(r1) segment_1(r2) segment_2(r0) segment_2(r1) segment_2(r2) segment_3(r0)
    //  segment_3(r1) segment_3(r2) segment_4(r0) segment_4(r1) segment_4(r2) segment_5(r0) segment_5(r1) segment_5(r2) segment_6(r0) segment_6(r1)
    //  ...
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    int assignedInstanceId = 0;
    for (String segmentName : segments) {
      List<String> instancesAssigned = new ArrayList<>(NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        instancesAssigned.add(instances.get(assignedInstanceId));
        assignedInstanceId = (assignedInstanceId + 1) % numInstances;
      }
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }

    // There should be 100 segments assigned
    assertEquals(currentAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, instances);
    int[] expectedNumSegmentsAssignedPerInstance = new int[numInstances];
    int numSegmentsPerInstance = numSegments * NUM_REPLICAS / numInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Current assignment should already be balanced
    assertEquals(
        SegmentAssignmentUtils.rebalanceTableWithHelixAutoRebalanceStrategy(currentAssignment, instances, NUM_REPLICAS),
        currentAssignment);

    // Replace instance_0 with instance_10
    // {
    //   0_0=[instance_10, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7, instance_8, instance_9]
    // }
    List<String> newInstances = new ArrayList<>(instances);
    String newInstanceName = INSTANCE_NAME_PREFIX + 10;
    newInstances.set(0, newInstanceName);
    Map<String, Map<String, String>> newAssignment = SegmentAssignmentUtils
        .rebalanceTableWithHelixAutoRebalanceStrategy(currentAssignment, newInstances, NUM_REPLICAS);
    // There should be 100 segments assigned
    assertEquals(currentAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // All segments on instance_0 should be moved to instance_10
    Map<String, Integer> numSegmentsToBeMovedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMovedPerInstance.size(), 1);
    assertEquals((int) numSegmentsToBeMovedPerInstance.get(newInstanceName), numSegmentsPerInstance);
    String oldInstanceName = INSTANCE_NAME_PREFIX + 0;
    for (String segmentName : segments) {
      if (currentAssignment.get(segmentName).containsKey(oldInstanceName)) {
        assertTrue(newAssignment.get(segmentName).containsKey(newInstanceName));
      }
    }

    // Remove 5 instances
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4]
    // }
    int newNumInstances = numInstances - 5;
    newInstances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    newAssignment = SegmentAssignmentUtils
        .rebalanceTableWithHelixAutoRebalanceStrategy(currentAssignment, newInstances, NUM_REPLICAS);
    // There should be 100 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // The segments are not perfectly balanced, but should be deterministic
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    assertEquals(numSegmentsAssignedPerInstance[0], 56);
    assertEquals(numSegmentsAssignedPerInstance[1], 60);
    assertEquals(numSegmentsAssignedPerInstance[2], 60);
    assertEquals(numSegmentsAssignedPerInstance[3], 60);
    assertEquals(numSegmentsAssignedPerInstance[4], 64);
    numSegmentsToBeMovedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMovedPerInstance.size(), newNumInstances);
    assertEquals((int) numSegmentsToBeMovedPerInstance.get(newInstances.get(0)), 26);
    assertEquals((int) numSegmentsToBeMovedPerInstance.get(newInstances.get(1)), 30);
    assertEquals((int) numSegmentsToBeMovedPerInstance.get(newInstances.get(2)), 30);
    assertEquals((int) numSegmentsToBeMovedPerInstance.get(newInstances.get(3)), 30);
    assertEquals((int) numSegmentsToBeMovedPerInstance.get(newInstances.get(4)), 34);

    // Add 5 instances
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7, instance_8, instance_9, instance_10, instance_11, instance_12, instance_13, instance_14]
    // }
    newNumInstances = numInstances + 5;
    newInstances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    newAssignment = SegmentAssignmentUtils
        .rebalanceTableWithHelixAutoRebalanceStrategy(currentAssignment, newInstances, NUM_REPLICAS);
    // There should be 100 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 20 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[newNumInstances];
    int newNumSegmentsPerInstance = numSegments * NUM_REPLICAS / newNumInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, newNumSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each new added instance should have 20 segments to be moved to it
    numSegmentsToBeMovedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMovedPerInstance.size(), 5);
    for (int instanceId = numInstances; instanceId < newNumInstances; instanceId++) {
      assertEquals((int) numSegmentsToBeMovedPerInstance.get(newInstances.get(instanceId)), newNumSegmentsPerInstance);
    }

    // Change all instances
    // {
    //   0_0=[i_0, i_1, i_2, i_3, i_4, i_5, i_6, i_7, i_8, i_9]
    // }
    String newInstanceNamePrefix = "i_";
    newInstances = SegmentAssignmentTestUtils.getNameList(newInstanceNamePrefix, numInstances);
    newAssignment = SegmentAssignmentUtils
        .rebalanceTableWithHelixAutoRebalanceStrategy(currentAssignment, newInstances, NUM_REPLICAS);
    // There should be 100 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[numInstances];
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each instance should have 30 segments to be moved to it
    numSegmentsToBeMovedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMovedPerInstance.size(), numInstances);
    for (String instanceName : newInstances) {
      assertEquals((int) numSegmentsToBeMovedPerInstance.get(instanceName), numSegmentsPerInstance);
    }
  }

  @Test
  public void testRebalanceReplicaGroupBasedTable() {
    // Table is rebalanced on a per partition basis, so testing rebalancing one partition is enough

    int numSegments = 90;
    List<String> segments = SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, numSegments);
    Map<Integer, Set<String>> partitionIdToSegmentsMap = Collections.singletonMap(0, new HashSet<>(segments));
    int numInstances = 9;
    List<String> instances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, numInstances);

    // {
    //   0_0=[instance_0, instance_1, instance_2],
    //   0_1=[instance_3, instance_4, instance_5],
    //   0_2=[instance_6, instance_7, instance_8]
    // }
    InstancePartitions instancePartitions = new InstancePartitions(null);
    int numInstancesPerReplica = numInstances / NUM_REPLICAS;
    int instanceIdToAdd = 0;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> instancesForReplica = new ArrayList<>(numInstancesPerReplica);
      for (int i = 0; i < numInstancesPerReplica; i++) {
        instancesForReplica.add(instances.get(instanceIdToAdd++));
      }
      instancePartitions.setInstances(0, replicaId, instancesForReplica);
    }

    // Uniformly spray segments to the instances:
    // Replica group 0: [instance_0, instance_1, instance_2],
    // Replica group 1: [instance_3, instance_4, instance_5],
    // Replica group 2: [instance_6, instance_7, instance_8]
    //                   segment_0   segment_1   segment_2
    //                   segment_3   segment_4   segment_5
    //                   ...
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < numSegments; segmentId++) {
      List<String> instancesAssigned = new ArrayList<>(NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        int assignedInstanceId = segmentId % numInstancesPerReplica + replicaId * numInstancesPerReplica;
        instancesAssigned.add(instances.get(assignedInstanceId));
      }
      currentAssignment.put(segments.get(segmentId),
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }

    // There should be 90 segments assigned
    assertEquals(currentAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, instances);
    int[] expectedNumSegmentsAssignedPerInstance = new int[numInstances];
    int numSegmentsPerInstance = numSegments * NUM_REPLICAS / numInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Current assignment should already be balanced
    assertEquals(SegmentAssignmentUtils
            .rebalanceReplicaGroupBasedTable(currentAssignment, instancePartitions, partitionIdToSegmentsMap),
        currentAssignment);

    // Replace instance_0 with instance_9, instance_4 with instance_10
    // {
    //   0_0=[instance_9, instance_1, instance_2],
    //   0_1=[instance_3, instance_10, instance_5],
    //   0_2=[instance_6, instance_7, instance_8]
    // }
    List<String> newInstances = new ArrayList<>(numInstances);
    List<String> newReplica0Instances = new ArrayList<>(instancePartitions.getInstances(0, 0));
    String newReplica0Instance = INSTANCE_NAME_PREFIX + 9;
    newReplica0Instances.set(0, newReplica0Instance);
    newInstances.addAll(newReplica0Instances);
    List<String> newReplica1Instances = new ArrayList<>(instancePartitions.getInstances(0, 1));
    String newReplica1Instance = INSTANCE_NAME_PREFIX + 10;
    newReplica1Instances.set(1, newReplica1Instance);
    newInstances.addAll(newReplica1Instances);
    List<String> newReplica2Instances = instancePartitions.getInstances(0, 2);
    newInstances.addAll(newReplica2Instances);
    InstancePartitions newInstancePartitions = new InstancePartitions(null);
    newInstancePartitions.setInstances(0, 0, newReplica0Instances);
    newInstancePartitions.setInstances(0, 1, newReplica1Instances);
    newInstancePartitions.setInstances(0, 2, newReplica2Instances);
    Map<String, Map<String, String>> newAssignment = SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedTable(currentAssignment, newInstancePartitions, partitionIdToSegmentsMap);
    // There should be 90 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // All segments on instance_0 should be moved to instance_9, all segments on instance_4 should be moved to
    // instance_10
    Map<String, Integer> numSegmentsToBeMovedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMovedPerInstance.size(), 2);
    assertEquals((int) numSegmentsToBeMovedPerInstance.get(newReplica0Instance), numSegmentsPerInstance);
    assertEquals((int) numSegmentsToBeMovedPerInstance.get(newReplica1Instance), numSegmentsPerInstance);
    String replica0OldInstanceName = INSTANCE_NAME_PREFIX + 0;
    String replica1OldInstanceName = INSTANCE_NAME_PREFIX + 4;
    for (String segmentName : segments) {
      Map<String, String> oldInstanceStateMap = currentAssignment.get(segmentName);
      if (oldInstanceStateMap.containsKey(replica0OldInstanceName)) {
        assertTrue(newAssignment.get(segmentName).containsKey(newReplica0Instance));
      }
      if (oldInstanceStateMap.containsKey(replica1OldInstanceName)) {
        assertTrue(newAssignment.get(segmentName).containsKey(newReplica1Instance));
      }
    }

    // Remove 3 instances (1 from each replica)
    // {
    //   0_0=[instance_0, instance_1],
    //   0_1=[instance_3, instance_4],
    //   0_2=[instance_6, instance_7]
    // }
    int newNumInstances = numInstances - 3;
    int newNumInstancesPerReplica = newNumInstances / NUM_REPLICAS;
    newInstances = new ArrayList<>(newNumInstances);
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> newInstancesForReplica =
          instancePartitions.getInstances(0, replicaId).subList(0, newNumInstancesPerReplica);
      newInstancePartitions.setInstances(0, replicaId, newInstancesForReplica);
      newInstances.addAll(newInstancesForReplica);
    }
    newAssignment = SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedTable(currentAssignment, newInstancePartitions, partitionIdToSegmentsMap);
    // There should be 90 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 45 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[newNumInstances];
    int newNumSegmentsPerInstance = numSegments * NUM_REPLICAS / newNumInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, newNumSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each instance should have 15 segments to be moved to it
    numSegmentsToBeMovedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMovedPerInstance.size(), newNumInstances);
    for (String instanceName : newInstances) {
      assertEquals((int) numSegmentsToBeMovedPerInstance.get(instanceName),
          newNumSegmentsPerInstance - numSegmentsPerInstance);
    }

    // Add 6 instances (2 to each replica)
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_9, instance_10],
    //   0_1=[instance_3, instance_4, instance_5, instance_11, instance_12],
    //   0_2=[instance_6, instance_7, instance_8, instance_13, instance_14]
    // }
    newNumInstances = numInstances + 6;
    newNumInstancesPerReplica = newNumInstances / NUM_REPLICAS;
    newInstances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    instanceIdToAdd = numInstances;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> newInstancesForReplica = new ArrayList<>(instancePartitions.getInstances(0, replicaId));
      for (int i = 0; i < newNumInstancesPerReplica - numInstancesPerReplica; i++) {
        newInstancesForReplica.add(newInstances.get(instanceIdToAdd++));
      }
      newInstancePartitions.setInstances(0, replicaId, newInstancesForReplica);
    }
    newAssignment = SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedTable(currentAssignment, newInstancePartitions, partitionIdToSegmentsMap);
    // There should be 90 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 18 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[newNumInstances];
    newNumSegmentsPerInstance = numSegments * NUM_REPLICAS / newNumInstances;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, newNumSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each new added instance should have 18 segments to be moved to it
    numSegmentsToBeMovedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMovedPerInstance.size(), 6);
    for (int instanceId = numInstances; instanceId < newNumInstances; instanceId++) {
      assertEquals((int) numSegmentsToBeMovedPerInstance.get(newInstances.get(instanceId)), newNumSegmentsPerInstance);
    }

    // Change all instances
    // {
    //   0_0=[i_0, i_1, i_2],
    //   0_1=[i_3, i_4, i_5],
    //   0_2=[i_6, i_7, i_8]
    // }
    newInstances = SegmentAssignmentTestUtils.getNameList("i_", numInstances);
    instanceIdToAdd = 0;
    for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
      List<String> instancesForReplica = new ArrayList<>(numInstancesPerReplica);
      for (int i = 0; i < numInstancesPerReplica; i++) {
        instancesForReplica.add(newInstances.get(instanceIdToAdd++));
      }
      newInstancePartitions.setInstances(0, replicaId, instancesForReplica);
    }
    newAssignment = SegmentAssignmentUtils
        .rebalanceReplicaGroupBasedTable(currentAssignment, newInstancePartitions, partitionIdToSegmentsMap);
    // There should be 90 segments assigned
    assertEquals(newAssignment.size(), numSegments);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, newInstances);
    expectedNumSegmentsAssignedPerInstance = new int[numInstances];
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Each instance should have 30 segments to be moved to it
    numSegmentsToBeMovedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsToBeMovedPerInstance(currentAssignment, newAssignment);
    assertEquals(numSegmentsToBeMovedPerInstance.size(), numInstances);
    for (String instanceName : newInstances) {
      assertEquals((int) numSegmentsToBeMovedPerInstance.get(instanceName), numSegmentsPerInstance);
    }
  }
}
