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
package org.apache.pinot.controller.helix.core.rebalance;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class TableRebalancerTest {

  @Test
  public void testDowntimeMode() {
    // With common instance, next assignment should be the same as target assignment
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE);
    TableRebalancer.SingleSegmentAssignment assignment =
        getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));

    // Without common instance, next assignment should be the same as target assignment
    targetInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host3", "host4"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertTrue(assignment._availableInstances.isEmpty());

    // With increasing number of replicas, next assignment should be the same as target assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host3", "host4", "host5"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertTrue(assignment._availableInstances.isEmpty());

    // With decreasing number of replicas, next assignment should be the same as target assignment
    currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE);
    targetInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertTrue(assignment._availableInstances.isEmpty());
  }

  @Test
  public void testOneMinAvailableReplicas() {
    // With 2 common instances, next assignment should be the same as target assignment
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host4"), ONLINE);
    TableRebalancer.SingleSegmentAssignment assignment =
        getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));

    // With 1 common instance, next assignment should be the same as target assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));

    // Without common instance, next assignment should have 1 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5", "host6"), ONLINE);
    // [host1, host4, host5]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Next round should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host4", "host5")));

    // With increasing number of replicas, next assignment should have 1 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5", "host6", "host7"), ONLINE);
    // [host1, host4, host5, host6]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Next round should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host4", "host5", "host6")));

    // With decreasing number of replicas, next assignment should have 1 common instances with current assignment
    currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host4"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7"), ONLINE);
    // [host1, host5, host6]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Next round should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));

    // With increasing from 1 replica, next assignment should have 1 common instances with current assignment
    currentInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Collections.singletonList("host1"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE);
    // [host1, host2, host3]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Next round should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host2", "host3")));
  }

  @Test
  public void testTwoMinAvailableReplicas() {
    // With 3 common instances, next assignment should be the same as target assignment
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host4"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host5"), ONLINE);
    TableRebalancer.SingleSegmentAssignment assignment =
        getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2", "host3")));

    // With 2 common instances, next assignment should be the same as target assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));

    // With 1 common instance, next assignment should have 2 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5", "host6", "host7"), ONLINE);
    // [host1, host2, host5, host6]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Next round should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host5", "host6")));

    // Without common instance, next assignment should have 2 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7", "host8"), ONLINE);
    // [host1, host2, host5, host6]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Next round should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));

    // With increasing number of replicas, next assignment should have 1 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7", "host8", "host9"), ONLINE);
    // [host1, host2, host5, host6, host7]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Next round should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6", "host7")));

    // With decreasing number of replicas, next assignment should have 2 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7"), ONLINE);
    // [host1, host2, host5]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Next round should have 2 common instances with first round assignment
    // [host1, host5, host6]
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host5")));
    // Next round should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));

    // With increasing from 1 replica, next assignment should have 1 common instances with current assignment
    // NOTE: This is the best we can do because we don't have 2 replicas available
    currentInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Collections.singletonList("host1"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE);
    // [host1, host2, host3]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Next round should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host2", "host3")));
  }

  private TableRebalancer.SingleSegmentAssignment getNextSingleSegmentAssignment(
      Map<String, String> currentInstanceStateMap, Map<String, String> targetInstanceStateMap,
      int minAvailableReplicas) {
    Map<String, Integer> numSegmentsToOffloadMap = new HashMap<>();
    for (String currentInstance : currentInstanceStateMap.keySet()) {
      numSegmentsToOffloadMap.put(currentInstance, 1);
    }
    for (String targetInstance : targetInstanceStateMap.keySet()) {
      numSegmentsToOffloadMap.merge(targetInstance, -1, Integer::sum);
    }
    Map<Pair<Set<String>, Set<String>>, Set<String>> assignmentMap = new HashMap<>();
    return TableRebalancer.getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap,
        minAvailableReplicas, numSegmentsToOffloadMap, assignmentMap);
  }

  @Test
  public void testAssignment() {
    // Current assignment:
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment2": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment4": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    currentAssignment.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));
    currentAssignment.put("segment3",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment4",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));

    // Target assignment 1:
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment2": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment4": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   }
    // }
    Map<String, Map<String, String>> targetAssignment = new TreeMap<>();
    targetAssignment.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment3",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment4",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));

    // Number of segments to offload:
    // {
    //   "host1": 0,
    //   "host2": 2,
    //   "host3": 2,
    //   "host4": 0,
    //   "host5": -2,
    //   "host6": -2
    // }
    Map<String, Integer> numSegmentsToOffloadMap =
        TableRebalancer.getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    assertEquals(numSegmentsToOffloadMap.size(), 6);
    assertEquals((int) numSegmentsToOffloadMap.get("host1"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host2"), 2);
    assertEquals((int) numSegmentsToOffloadMap.get("host3"), 2);
    assertEquals((int) numSegmentsToOffloadMap.get("host4"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host5"), -2);
    assertEquals((int) numSegmentsToOffloadMap.get("host6"), -2);

    // Next assignment with 2 minimum available replicas with or without strict replica-group should reach the target
    // assignment
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Map<String, Map<String, String>> nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup);
      assertEquals(nextAssignment, targetAssignment);
    }

    // Target assignment 2:
    // {
    //   "segment1": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment2": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment3": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment4": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    targetAssignment = new TreeMap<>();
    targetAssignment.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE));
    targetAssignment.put("segment3",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment4",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE));

    // Number of segments to offload:
    // {
    //   "host1": 0,
    //   "host2": 2,
    //   "host3": 4,
    //   "host4": -2,
    //   "host5": -2,
    //   "host6": -2
    // }
    numSegmentsToOffloadMap = TableRebalancer.getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    assertEquals(numSegmentsToOffloadMap.size(), 6);
    assertEquals((int) numSegmentsToOffloadMap.get("host1"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host2"), 2);
    assertEquals((int) numSegmentsToOffloadMap.get("host3"), 4);
    assertEquals((int) numSegmentsToOffloadMap.get("host4"), -2);
    assertEquals((int) numSegmentsToOffloadMap.get("host5"), -2);
    assertEquals((int) numSegmentsToOffloadMap.get("host6"), -2);

    // Next assignment with 2 minimum available replicas with or without strict replica-group should finish in 2 steps:
    //
    // The first assignment will move "segment1" and "segment3" from "host3" (with the most segments to offload) to
    // "host4" (with the least segments to offload), and move "segment2" and "segment4" from "host3" (with the most
    // segments to offload) to "host5" (with the least segments to offload):
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment2": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment4": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    //
    // The second assignment should reach the target assignment
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Map<String, Map<String, String>> nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup);
      assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
      assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
      assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
      assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host2", "host4", "host5")));

      nextAssignment = TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup);
      assertEquals(nextAssignment, targetAssignment);
    }

    // Target assignment 3:
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment2": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment4": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    targetAssignment = new TreeMap<>();
    targetAssignment.put("segment1",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4"), ONLINE));
    targetAssignment.put("segment2",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4"), ONLINE));
    targetAssignment.put("segment3",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4"), ONLINE));
    targetAssignment.put("segment4",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4"), ONLINE));

    // Number of segments to offload:
    // {
    //   "host1": -2,
    //   "host2": 4,
    //   "host3": 0,
    //   "host4": -2
    // }
    numSegmentsToOffloadMap = TableRebalancer.getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    assertEquals(numSegmentsToOffloadMap.size(), 4);
    assertEquals((int) numSegmentsToOffloadMap.get("host1"), -2);
    assertEquals((int) numSegmentsToOffloadMap.get("host2"), 4);
    assertEquals((int) numSegmentsToOffloadMap.get("host3"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host4"), -2);

    // Next assignment with 2 minimum available replicas without strict replica-group should reach the target assignment
    Map<String, Map<String, String>> nextAssignment =
        TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, false);
    assertEquals(nextAssignment, targetAssignment);

    // Next assignment with 2 minimum available replicas with strict replica-group should finish in 2 steps:
    //
    // The first assignment will bring "segment1" and "segment3" to the target state. It cannot bring "segment2" and
    // "segment4" to the target state because "host1" and "host4" might be unavailable for strict replica-group routing,
    // which breaks the minimum available replicas requirement:
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment2": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment4": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    //
    // The second assignment should reach the target assignment
    nextAssignment = TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, true);
    assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host3", "host4")));
    assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
    assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host3", "host4")));
    assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
    nextAssignment = TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, true);
    assertEquals(nextAssignment, targetAssignment);
  }

  @Test
  public void testIsExternalViewConverged() {
    String offlineTableName = "testTable_OFFLINE";
    Map<String, Map<String, String>> externalViewSegmentStates = new TreeMap<>();
    Map<String, Map<String, String>> idealStateSegmentStates = new TreeMap<>();

    // Empty segment states should match
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            false));
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            true));

    // Do not check segment that does not exist in IdealState
    Map<String, String> instanceStateMap = new TreeMap<>();
    instanceStateMap.put("instance1", ONLINE);
    externalViewSegmentStates.put("segment1", instanceStateMap);
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            false));
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            true));

    // Do not check segment that is OFFLINE in IdealState
    instanceStateMap = new TreeMap<>();
    instanceStateMap.put("instance1", OFFLINE);
    idealStateSegmentStates.put("segment2", instanceStateMap);
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            false));
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            true));

    // Should fail when a segment has CONSUMING instance in IdealState but does not exist in ExternalView
    instanceStateMap.put("instance2", CONSUMING);
    assertFalse(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            false));
    assertFalse(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            true));

    // Should fail when instance state does not exist
    instanceStateMap = new TreeMap<>();
    externalViewSegmentStates.put("segment2", instanceStateMap);
    assertFalse(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            false));
    assertFalse(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            true));

    // Should fail when instance state does not match
    instanceStateMap.put("instance2", OFFLINE);
    assertFalse(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            false));
    assertFalse(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            true));

    // Should pass when instance state matches
    instanceStateMap.put("instance2", CONSUMING);
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            false));
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            true));

    // Should pass when there are extra instances in ExternalView
    instanceStateMap.put("instance3", CONSUMING);
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            false));
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            true));

    // Should throw exception when instance state is ERROR in ExternalView and best-efforts is disabled
    instanceStateMap.put("instance2", ERROR);
    try {
      TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
          false);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Should pass when instance state is ERROR in ExternalView and best-efforts is enabled
    assertTrue(
        TableRebalancer.isExternalViewConverged(offlineTableName, externalViewSegmentStates, idealStateSegmentStates,
            true));
  }
}
