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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class TableRebalancerTest {

  private static final TableRebalancer.PartitionIdFetcher DUMMY_PARTITION_FETCHER = (segmentName, isConsuming) -> 0;
  private static final TableRebalancer.PartitionIdFetcher SIMPLE_PARTITION_FETCHER = (segmentName, isConsuming) -> {
    LLCSegmentName name = LLCSegmentName.of(segmentName);
    return name == null ? -1 : name.getPartitionGroupId();
  };

  @Test
  public void testDowntimeMode() {
    // With common instance, first assignment should be the same as target assignment
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE);
    TableRebalancer.SingleSegmentAssignment assignment =
        getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));

    // Without common instance, first assignment should be the same as target assignment
    targetInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host3", "host4"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertTrue(assignment._availableInstances.isEmpty());

    // With increasing number of replicas, first assignment should be the same as target assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host3", "host4", "host5"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertTrue(assignment._availableInstances.isEmpty());

    // With decreasing number of replicas, first assignment should be the same as target assignment
    currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE);
    targetInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertTrue(assignment._availableInstances.isEmpty());
  }

  @Test
  public void testDowntimeWithLowDiskMode() {
    // With common instance, first assignment should keep the common instance and remove the not common instance
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE);
    TableRebalancer.SingleSegmentAssignment assignment =
        getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0, true);
    assertEquals(assignment._instanceStateMap, Collections.singletonMap("host1", ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 0, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));

    // Without common instance, first assignment should drop all instances
    targetInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host3", "host4"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0, true);
    assertTrue(assignment._instanceStateMap.isEmpty());
    assertTrue(assignment._availableInstances.isEmpty());
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 0, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertTrue(assignment._availableInstances.isEmpty());

    // With increasing number of replicas, first assignment should drop all instances
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host3", "host4", "host5"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0, true);
    assertTrue(assignment._instanceStateMap.isEmpty());
    assertTrue(assignment._availableInstances.isEmpty());
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 0, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertTrue(assignment._availableInstances.isEmpty());

    // With decreasing number of replicas, first assignment should drop all instances
    currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE);
    targetInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 0, true);
    assertTrue(assignment._instanceStateMap.isEmpty());
    assertTrue(assignment._availableInstances.isEmpty());
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 0, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertTrue(assignment._availableInstances.isEmpty());
  }

  @Test
  public void testOneMinAvailableReplicas() {
    // With 2 common instances, first assignment should be the same as target assignment
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host4"), ONLINE);
    TableRebalancer.SingleSegmentAssignment assignment =
        getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));

    // With 1 common instance, first assignment should be the same as target assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));

    // Without common instance, first assignment should have 1 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5", "host6"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host4", "host5")));

    // With increasing number of replicas, first assignment should have 1 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5", "host6", "host7"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host4", "host5", "host6")));

    // With decreasing number of replicas, first assignment should have 1 common instances with current assignment
    currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host4"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));

    // With increasing from 1 replica, first assignment should have 1 common instances with current assignment
    currentInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Collections.singletonList("host1"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host2", "host3")));
  }

  @Test
  public void testOneMinAvailableReplicasWithLowDiskMode() {
    // With 2 common instances, first assignment should keep the common instances and remove the not common instance
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host4"), ONLINE);
    TableRebalancer.SingleSegmentAssignment assignment =
        getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));

    // With 1 common instance, first assignment should keep the common instance and remove the not common instances
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, Collections.singletonMap("host1", ONLINE));
    assertEquals(assignment._availableInstances, Collections.singletonList("host1"));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));

    // Without common instance, fist assignment should keep 1 instance from current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5", "host6"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, Collections.singletonMap("host1", ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should add 2 instances from target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Third assignment should remove the old instance from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host4", "host5")));
    // Fourth assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host4", "host5")));

    // With increasing number of replicas, fist assignment should keep 1 instance from current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5", "host6", "host7"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, Collections.singletonMap("host1", ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should add 3 instances from target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Third assignment should remove the old instance from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host4", "host5", "host6")));
    // Fourth assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host4", "host5", "host6")));

    // With decreasing number of replicas, fist assignment should keep 1 instance from current assignment
    currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host4"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, Collections.singletonMap("host1", ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should add 2 instances from target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Third assignment should remove the old instance from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));
    // Fourth assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));

    // With increasing from 1 replica, fist assignment should keep the instance from current assignment, and add 2
    // instances from target assignment
    currentInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Collections.singletonList("host1"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should remove the old instance from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host2", "host3")));
    // Third assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 1, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host2", "host3")));
  }

  @Test
  public void testTwoMinAvailableReplicas() {
    // With 3 common instances, first assignment should be the same as target assignment
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host4"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host5"), ONLINE);
    TableRebalancer.SingleSegmentAssignment assignment =
        getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2", "host3")));

    // With 2 common instances, first assignment should be the same as target assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));

    // With 1 common instance, first assignment should have 2 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5", "host6", "host7"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host5", "host6")));

    // Without common instance, first assignment should have 2 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7", "host8"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));

    // With increasing number of replicas, first assignment should have 1 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7", "host8", "host9"), ONLINE);
    // [host1, host2, host5, host6, host7]
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6", "host7"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6", "host7")));

    // With decreasing number of replicas, first assignment should have 2 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should have 2 common instances with first assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host5")));
    // Third assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));

    // With increasing from 1 replica, first assignment should have 1 common instances with current assignment
    // NOTE: This is the best we can do because we don't have 2 replicas available
    currentInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Collections.singletonList("host1"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, false);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host2", "host3")));
  }

  @Test
  public void testTwoMinAvailableReplicasWithLowDiskMode() {
    // With 3 common instances, first assignment should keep the common instances and remove the not common instance
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host4"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host5"), ONLINE);
    TableRebalancer.SingleSegmentAssignment assignment =
        getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2", "host3")));

    // With 2 common instances, first assignment should keep the common instances and remove the not common instances
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should be the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));

    // With 1 common instance, fist assignment should keep the common instance, and 1 more instance from current
    // assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5", "host6", "host7"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should add 2 instances from target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Third assignment should remove the old instance from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host5", "host6")));
    // Fourth assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host5", "host6")));

    // Without common instance, fist assignment should keep 2 instances from current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7", "host8"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should add 2 instances from target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Third assignment should remove the old instances from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));
    // Fourth assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));

    // With increasing number of replicas, fist assignment should keep 2 instances from current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7", "host8", "host9"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should add 3 instances from target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6", "host7"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Third assignment should remove the old instances from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6", "host7")));
    // Fourth assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6", "host7")));

    // With decreasing number of replicas, fist assignment should keep 2 instances from current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Second assignment should add 1 instance from target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host2")));
    // Third assignment should remove 1 old instance from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host5")));
    // Forth assignment should add 1 more instance from target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host1", "host5")));
    // Fifth assignment should remove the other old instance from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));
    // Sixth assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host5", "host6")));

    // With increasing from 1 replica, fist assignment should keep the instance from current assignment, and add 2
    // instances from target assignment
    // NOTE: This is the best we can do because we don't have 2 replicas available
    currentInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Collections.singletonList("host1"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE);
    assignment = getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    assertEquals(assignment._availableInstances, Collections.singleton("host1"));
    // Second assignment should remove the old instance from current assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap,
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3"), ONLINE));
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host2", "host3")));
    // Third assignment should make the assignment the same as target assignment
    assignment = getNextSingleSegmentAssignment(assignment._instanceStateMap, targetInstanceStateMap, 2, true);
    assertEquals(assignment._instanceStateMap, targetInstanceStateMap);
    assertEquals(assignment._availableInstances, new TreeSet<>(Arrays.asList("host2", "host3")));
  }

  private TableRebalancer.SingleSegmentAssignment getNextSingleSegmentAssignment(
      Map<String, String> currentInstanceStateMap, Map<String, String> targetInstanceStateMap, int minAvailableReplicas,
      boolean lowDiskMode) {
    Map<String, Integer> numSegmentsToOffloadMap = new HashMap<>();
    for (String currentInstance : currentInstanceStateMap.keySet()) {
      numSegmentsToOffloadMap.put(currentInstance, 1);
    }
    for (String targetInstance : targetInstanceStateMap.keySet()) {
      numSegmentsToOffloadMap.merge(targetInstance, -1, Integer::sum);
    }
    Map<Pair<Set<String>, Set<String>>, Set<String>> assignmentMap = new HashMap<>();
    return TableRebalancer.getNextSingleSegmentAssignment(currentInstanceStateMap, targetInstanceStateMap,
        minAvailableReplicas, lowDiskMode, numSegmentsToOffloadMap, assignmentMap);
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
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
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
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
      assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
      assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
      assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
      assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host2", "host4", "host5")));

      nextAssignment =
          TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
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
        TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, false, false,
            RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
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
    nextAssignment = TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, true, false,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
    assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host3", "host4")));
    assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
    assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host3", "host4")));
    assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
    nextAssignment = TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, true, false,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
    assertEquals(nextAssignment, targetAssignment);
  }

  @Test
  public void testAssignmentWithLowDiskMode() {
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

    // Next assignment with 2 minimum available replicas with or without strict replica-group should finish in 2 steps:
    //
    // The first assignment will remove "segment1" and "segment3" from "host2", and remove "segment2" and "segment4"
    // from "host3":
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment2": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment4": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    //
    // The second assignment should reach the target assignment
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Map<String, Map<String, String>> nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, true,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
      assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host3")));
      assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host2", "host4")));
      assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host3")));
      assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host2", "host4")));

      nextAssignment =
          TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, true,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
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

    // Next assignment with 2 minimum available replicas with or without strict replica-group should finish in 4 steps:
    //
    // The first assignment will remove "segment1" and "segment3" from "host3" (with the most segments to offload), and
    // remove "segment2" and "segment4" from "host3 (with the most segments to offload)":
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE"
    //   },
    //   "segment2": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE"
    //   },
    //   "segment4": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    //
    // The second assignment will add "segment1" and "segment3" to "host4" (with the least segments to offload), and add
    // "segment2" and "segment4" to "host5" (with the least segments to offload):
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
    // The third assignment will remove "segment1" and "segment3" from "host1", and remove "segment2" and "segment4"
    // from "host2":
    // {
    //   "segment1": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment2": {
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment3": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment4": {
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    //
    // The fourth assignment should reach the target assignment
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Map<String, Map<String, String>> nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, true,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
      assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host2")));
      assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host2", "host4")));
      assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host2")));
      assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host2", "host4")));

      nextAssignment =
          TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, true,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
      assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
      assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
      assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
      assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host2", "host4", "host5")));

      nextAssignment =
          TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, true,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
      assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host2", "host4")));
      assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host4", "host5")));
      assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host2", "host4")));
      assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host4", "host5")));

      nextAssignment =
          TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, true,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
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

    // Next assignment with 2 minimum available replicas without strict replica-group should finish in 2 steps:
    //
    // The first assignment will remove "segment1" and "segment3" from "host2", and remove "segment2" and "segment4"
    // from "host2":
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment2": {
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment4": {
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    //
    // The second assignment should reach the target assignment
    Map<String, Map<String, String>> nextAssignment =
        TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, false, true,
            RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
    assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host3")));
    assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host3", "host4")));
    assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host3")));
    assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host3", "host4")));

    nextAssignment = TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, false, true,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
    assertEquals(nextAssignment, targetAssignment);

    // Next assignment with 2 minimum available replicas with strict replica-group should finish in 3 steps:
    //
    // The first assignment will remove "segment1" and "segment3" from "host2", and remove "segment2" and "segment4"
    // from "host2":
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment2": {
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment4": {
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    //
    // The second assignment will bring "segment1" and "segment3" to the target state. It cannot bring "segment2" and
    // "segment4" to the target state because "host1" and "host4" might be unavailable for strict replica-group routing,
    // which breaks the minimum available replicas requirement:
    // {
    //   "segment1": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment2": {
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment3": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment4": {
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    //
    // The third assignment should reach the target assignment
    nextAssignment = TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, true, true,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
    assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host3")));
    assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host3", "host4")));
    assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host3")));
    assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host3", "host4")));

    nextAssignment = TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, true, true,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
    assertEquals(nextAssignment.get("segment1").keySet(), new TreeSet<>(Arrays.asList("host1", "host3", "host4")));
    assertEquals(nextAssignment.get("segment2").keySet(), new TreeSet<>(Arrays.asList("host3", "host4")));
    assertEquals(nextAssignment.get("segment3").keySet(), new TreeSet<>(Arrays.asList("host1", "host3", "host4")));
    assertEquals(nextAssignment.get("segment4").keySet(), new TreeSet<>(Arrays.asList("host3", "host4")));

    nextAssignment = TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, true, true,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER);
    assertEquals(nextAssignment, targetAssignment);
  }

  @Test
  public void testIsExternalViewConverged() {
    String offlineTableName = "testTable_OFFLINE";
    Map<String, Map<String, String>> externalViewSegmentStates = new TreeMap<>();
    Map<String, Map<String, String>> idealStateSegmentStates = new TreeMap<>();
    boolean[] falseAndTrue = new boolean[]{false, true};

    // Empty segment states should match
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertTrue(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
      }
    }

    // Do not check segment that does not exist in IdealState
    Map<String, String> instanceStateMap = new TreeMap<>();
    instanceStateMap.put("instance1", ONLINE);
    externalViewSegmentStates.put("segment1", instanceStateMap);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertTrue(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
      }
    }

    // Do not check segment that is OFFLINE in IdealState
    instanceStateMap = new TreeMap<>();
    instanceStateMap.put("instance1", OFFLINE);
    idealStateSegmentStates.put("segment2", instanceStateMap);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertTrue(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
      }
    }

    // Should fail when a segment has CONSUMING instance in IdealState but does not exist in ExternalView
    instanceStateMap.put("instance2", CONSUMING);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertFalse(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
        assertEquals(
            TableRebalancer.getNumRemainingSegmentReplicasToProcess(externalViewSegmentStates, idealStateSegmentStates,
                lowDiskMode, bestEfforts, null), 1);
      }
    }

    instanceStateMap.put("instance3", CONSUMING);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertFalse(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
        assertEquals(
            TableRebalancer.getNumRemainingSegmentReplicasToProcess(externalViewSegmentStates, idealStateSegmentStates,
                lowDiskMode, bestEfforts, null), 2);
      }
    }

    // Should fail when instance state does not exist
    instanceStateMap = new TreeMap<>();
    externalViewSegmentStates.put("segment2", instanceStateMap);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertFalse(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
        assertEquals(
            TableRebalancer.getNumRemainingSegmentReplicasToProcess(externalViewSegmentStates, idealStateSegmentStates,
                lowDiskMode, bestEfforts, null), 2);
      }
    }

    // Should fail when instance state does not match
    instanceStateMap.put("instance2", OFFLINE);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertFalse(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
        assertEquals(
            TableRebalancer.getNumRemainingSegmentReplicasToProcess(externalViewSegmentStates, idealStateSegmentStates,
                lowDiskMode, bestEfforts, null), 2);
      }
    }

    // Should fail when instance state does not match
    instanceStateMap.put("instance2", OFFLINE);
    instanceStateMap.put("instance3", OFFLINE);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertFalse(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
        assertEquals(
            TableRebalancer.getNumRemainingSegmentReplicasToProcess(externalViewSegmentStates, idealStateSegmentStates,
                lowDiskMode, bestEfforts, null), 2);
      }
    }

    // Should fail when instance state does not match
    instanceStateMap.put("instance2", CONSUMING);
    instanceStateMap.put("instance3", OFFLINE);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertFalse(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
        assertEquals(
            TableRebalancer.getNumRemainingSegmentReplicasToProcess(externalViewSegmentStates, idealStateSegmentStates,
                lowDiskMode, bestEfforts, null), 1);
      }
    }

    // Should pass when instance state matches
    instanceStateMap.put("instance2", CONSUMING);
    instanceStateMap.put("instance3", CONSUMING);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        assertTrue(
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                bestEfforts, null));
        assertEquals(
            TableRebalancer.getNumRemainingSegmentReplicasToProcess(externalViewSegmentStates, idealStateSegmentStates,
                lowDiskMode, bestEfforts, null), 0);
      }
    }

    // When there are extra instances in ExternalView, should pass in regular mode but fail in low disk mode
    instanceStateMap.put("instance4", CONSUMING);
    instanceStateMap.put("instance5", CONSUMING);
    instanceStateMap.put("instance6", CONSUMING);
    for (boolean bestEfforts : falseAndTrue) {
      assertTrue(TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, false,
          bestEfforts, null));
      assertEquals(
          TableRebalancer.getNumRemainingSegmentReplicasToProcess(externalViewSegmentStates, idealStateSegmentStates,
              false, bestEfforts, null), 0);
      assertFalse(
          TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, true, bestEfforts,
              null));
      assertEquals(
          TableRebalancer.getNumRemainingSegmentReplicasToProcess(externalViewSegmentStates, idealStateSegmentStates,
              true, bestEfforts, null), 3);
    }

    // When instance state is ERROR in ExternalView, should fail in regular mode but pass in best-efforts mode
    instanceStateMap.put("instance2", ERROR);
    instanceStateMap.remove("instance4");
    instanceStateMap.remove("instance5");
    instanceStateMap.remove("instance6");
    for (boolean lowDiskMode : falseAndTrue) {
      try {
        TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode, false,
            null);
        fail();
      } catch (Exception e) {
        // Expected
      }
      assertTrue(
          TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode, true,
              null));
    }

    // When the extra instance is in ERROR state, should throw exception in low disk mode when best-efforts is disabled
    instanceStateMap.put("instance2", CONSUMING);
    instanceStateMap.put("instance3", CONSUMING);
    instanceStateMap.put("instance4", ERROR);
    instanceStateMap.put("instance5", ERROR);
    instanceStateMap.put("instance6", ERROR);
    for (boolean lowDiskMode : falseAndTrue) {
      for (boolean bestEfforts : falseAndTrue) {
        if (lowDiskMode && !bestEfforts) {
          try {
            TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, true, false,
                null);
            fail();
          } catch (Exception e) {
            // Expected
          }
        } else {
          assertTrue(
              TableRebalancer.isExternalViewConverged(externalViewSegmentStates, idealStateSegmentStates, lowDiskMode,
                  bestEfforts, null));
        }
      }
    }
  }

  @Test
  public void testAssignmentWithServerBatching() {
    // Using LLC segment naming so that batching can be tested for both non-strict replica group and strict replica
    // group based assignment. Otherwise, the partitionId might have to be fetched from ZK SegmentMetadata which will
    // not exist since these aren't actual tables
    // Current assignment:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__4__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    currentAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));
    currentAssignment.put("segment__3__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__4__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));

    // Target assignment 1:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__4__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   }
    // }
    Map<String, Map<String, String>> targetAssignment = new TreeMap<>();
    targetAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__3__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__4__0__98347869999L",
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
    // assignment after two steps. Batch size = 1, unique partitionIds
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Object2IntOpenHashMap<String> segmentToPartitionIdMap = new Object2IntOpenHashMap<>();
      Map<String, Map<String, String>> nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      assertNotEquals(nextAssignment, targetAssignment);
      assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
          new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
      assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
          new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
      assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
          new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
      assertEquals(nextAssignment.get("segment__4__0__98347869999L").keySet(),
          new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
      nextAssignment =
          TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      assertEquals(nextAssignment, targetAssignment);
    }

    // Next assignment with 2 minimum available replicas with our without strict replica-group should reach the target
    // assignment after one steps. Batch size = 2, unique partitionIds
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Object2IntOpenHashMap<String> segmentToPartitionIdMap = new Object2IntOpenHashMap<>();
      Map<String, Map<String, String>> nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              2, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      assertEquals(nextAssignment, targetAssignment);
    }

    // Target assignment 2:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__4__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    targetAssignment = new TreeMap<>();
    targetAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE));
    targetAssignment.put("segment__3__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__4__0__98347869999L",
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

    // Next assignment with 2 minimum available replicas with or without strict replica-group should finish in 3 steps
    // with batching with batchSizePerServer = 1:
    //
    // The first assignment will move "segment1" and "segment2" by one host at a time, and since the other segments
    // if added will go beyond batchSizePerServer, they'll be picked up on the next two-three assignments
    // (host4 and host2):
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__4__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    // Second Assignment (host6, host1, host4, and host5 get 1 segment each) - non-strictReplicaGroup:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__4__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    // Second Assignment (host1, host6, and host5 get 1 segment each) - strictReplicaGroup:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__4__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    //
    // The third assignment should reach the target assignment for non-strictReplicaGroup, and the fourth for
    // strictReplicaGroup
    // Third Assignment (host1 and host4 gets 1 segment each) - strictReplicaGroup:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__4__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Map<String, Map<String, String>> nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              1, new Object2IntOpenHashMap<>(), SIMPLE_PARTITION_FETCHER);
      assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
          new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
      assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
          new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
      assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
          new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
      assertEquals(nextAssignment.get("segment__4__0__98347869999L").keySet(),
          new TreeSet<>(Arrays.asList("host2", "host3", "host4")));

      nextAssignment =
          TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              1, new Object2IntOpenHashMap<>(), SIMPLE_PARTITION_FETCHER);
      if (!enableStrictReplicaGroup) {
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host4", "host5")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
        assertEquals(nextAssignment.get("segment__4__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, new Object2IntOpenHashMap<>(), SIMPLE_PARTITION_FETCHER);
      } else {
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host4", "host5")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host6")));
        assertEquals(nextAssignment.get("segment__4__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, new Object2IntOpenHashMap<>(), SIMPLE_PARTITION_FETCHER);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host4", "host5")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__4__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host4", "host5")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, new Object2IntOpenHashMap<>(), SIMPLE_PARTITION_FETCHER);
      }
      assertEquals(nextAssignment, targetAssignment);
    }

    // Target assignment 3:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__4__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    targetAssignment = new TreeMap<>();
    targetAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4"), ONLINE));
    targetAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4"), ONLINE));
    targetAssignment.put("segment__3__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host4"), ONLINE));
    targetAssignment.put("segment__4__0__98347869999L",
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
    // in 1 step using batchSizePerServer = 2
    Map<String, Map<String, String>> nextAssignment =
        TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, false, false,
            2, new Object2IntOpenHashMap<>(), SIMPLE_PARTITION_FETCHER);
    assertEquals(nextAssignment, targetAssignment);

    // Next assignment with 2 minimum available replicas with strict replica-group should finish in 2 steps even with
    // batchSizePerServer = 2:
    //
    // The first assignment will bring "segment2" and "segment4" to the target state. It cannot bring "segment1" and
    // "segment3" to the target state because "host1" and "host4" might be unavailable for strict replica-group routing,
    // which breaks the minimum available replicas requirement:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__4__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    //
    // The second assignment should reach the target assignment
    nextAssignment = TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, true, false,
        2, new Object2IntOpenHashMap<>(), SIMPLE_PARTITION_FETCHER);
    assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
        new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
    assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
        new TreeSet<>(Arrays.asList("host1", "host3", "host4")));
    assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
        new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
    assertEquals(nextAssignment.get("segment__4__0__98347869999L").keySet(),
        new TreeSet<>(Arrays.asList("host1", "host3", "host4")));
    nextAssignment = TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, true, false,
        2, new Object2IntOpenHashMap<>(), SIMPLE_PARTITION_FETCHER);
    assertEquals(nextAssignment, targetAssignment);

    // Try assignment with overlapping partitions across segments, especially for strict replica group based.
    // Non-strict replica group based should not matter
    // Current assignment:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__1__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__2__1__98347869999L": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   }
    // }
    currentAssignment = new TreeMap<>();
    currentAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__1__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));
    currentAssignment.put("segment__2__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));

    // Target assignment 1 with just 2 overall partitions instead of 4 unique ones:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__1__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__2__1__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   }
    // }
    targetAssignment = new TreeMap<>();
    targetAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__1__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__2__1__98347869999L",
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
    numSegmentsToOffloadMap =
        TableRebalancer.getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    assertEquals(numSegmentsToOffloadMap.size(), 6);
    assertEquals((int) numSegmentsToOffloadMap.get("host1"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host2"), 2);
    assertEquals((int) numSegmentsToOffloadMap.get("host3"), 2);
    assertEquals((int) numSegmentsToOffloadMap.get("host4"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host5"), -2);
    assertEquals((int) numSegmentsToOffloadMap.get("host6"), -2);

    // Next assignment with 2 minimum available replicas without strict replica-group should reach the target
    // assignment after two steps. With strict replica groups it should reach the target assignment immediately since
    // the full partition must be selected for movement for a given Pair(currentInstance, targetInstances).
    // Batch size = 1, unique partitionIds
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Object2IntOpenHashMap<String> segmentToPartitionIdMap = new Object2IntOpenHashMap<>();
      nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      if (!enableStrictReplicaGroup) {
        // Nothing should change, since we don't select based on partitions for non-strict replica groups
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      }
      assertEquals(nextAssignment, targetAssignment);
    }

    // Target assignment 2 with just 2 unique partitions:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__1__1__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__2__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host4": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    targetAssignment = new TreeMap<>();
    targetAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__1__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE));
    targetAssignment.put("segment__2__1__98347869999L",
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
    numSegmentsToOffloadMap =
        TableRebalancer.getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    assertEquals(numSegmentsToOffloadMap.size(), 6);
    assertEquals((int) numSegmentsToOffloadMap.get("host1"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host2"), 2);
    assertEquals((int) numSegmentsToOffloadMap.get("host3"), 4);
    assertEquals((int) numSegmentsToOffloadMap.get("host4"), -2);
    assertEquals((int) numSegmentsToOffloadMap.get("host5"), -2);
    assertEquals((int) numSegmentsToOffloadMap.get("host6"), -2);

    // Next assignment with 2 minimum available replicas without strict replica-group should reach the target
    // assignment after three steps. With strict replica groups it should reach the target assignment in two steps since
    // the full partition must be selected for movement for a given Pair(currentInstance, targetInstances).
    // Batch size = 1, unique partitionIds
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Object2IntOpenHashMap<String> segmentToPartitionIdMap = new Object2IntOpenHashMap<>();
      nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      if (!enableStrictReplicaGroup) {
        // Nothing should change, since we don't select based on partitions for non-strict replica groups
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host4", "host5")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      } else {
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host4")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host5")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      }
      assertEquals(nextAssignment, targetAssignment);
    }

    // Try an assignment with 3 unique partitions and batchSizePerServer = 1 to force strict replica group to go
    // through 2 loops
    // Non-strict replica group based should not matter
    // Current assignment:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__1__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__2__1__98347869999L": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__3__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   }
    // }
    currentAssignment = new TreeMap<>();
    currentAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__1__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));
    currentAssignment.put("segment__2__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));
    currentAssignment.put("segment__3__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__3__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));

    // Target assignment 4:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__1__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__2__1__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__3__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    targetAssignment = new TreeMap<>();
    targetAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__1__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__2__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__3__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__3__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));

    // Number of segments to offload:
    // {
    //   "host1": 0,
    //   "host2": 4,
    //   "host3": 2,
    //   "host4": 0,
    //   "host5": -4,
    //   "host6": -2
    // }
    numSegmentsToOffloadMap =
        TableRebalancer.getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    assertEquals(numSegmentsToOffloadMap.size(), 6);
    assertEquals((int) numSegmentsToOffloadMap.get("host1"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host2"), 4);
    assertEquals((int) numSegmentsToOffloadMap.get("host3"), 2);
    assertEquals((int) numSegmentsToOffloadMap.get("host4"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host5"), -4);
    assertEquals((int) numSegmentsToOffloadMap.get("host6"), -2);

    // Next assignment with 2 minimum available replicas without strict replica-group should reach the target
    // assignment after four steps. With strict replica groups it should reach the target assignment in two steps since
    // the full partition must be selected for movement for a given Pair(currentInstance, targetInstances).
    // Batch size = 1, unique partitionIds
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Object2IntOpenHashMap<String> segmentToPartitionIdMap = new Object2IntOpenHashMap<>();
      nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      if (!enableStrictReplicaGroup) {
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      } else {
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                1, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      }
      assertEquals(nextAssignment, targetAssignment);
    }

    // Try an assignment with 3 unique partitions but with different assignments and batchSizePerServer = 1 to force
    // strict replica group routing only to go through 2 loops
    // Non-strict replica group based should not matter
    // Current assignment:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__1__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__1__2__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__2__1__98347869999L": {
    //     "host2": "ONLINE",
    //     "host3": "ONLINE",
    //     "host4": "ONLINE"
    //   },
    //   "segment__2__2__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host7": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__3__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host2": "ONLINE",
    //     "host3": "ONLINE"
    //   },
    //   "segment__3__2__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host6": "ONLINE"
    //   }
    // }
    currentAssignment = new TreeMap<>();
    currentAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__1__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__1__2__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host6"), ONLINE));
    currentAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));
    currentAssignment.put("segment__2__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host3", "host4"), ONLINE));
    currentAssignment.put("segment__2__2__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host7"), ONLINE));
    currentAssignment.put("segment__3__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__3__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE));
    currentAssignment.put("segment__3__2__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host6"), ONLINE));

    // Target assignment 5:
    // {
    //   "segment__1__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__1__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__1__2__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__2__0__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__2__1__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__2__2__98347869999L": {
    //     "host2": "ONLINE",
    //     "host4": "ONLINE",
    //     "host6": "ONLINE"
    //   },
    //   "segment__3__0__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__3__1__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   },
    //   "segment__3__2__98347869999L": {
    //     "host1": "ONLINE",
    //     "host3": "ONLINE",
    //     "host5": "ONLINE"
    //   }
    // }
    targetAssignment = new TreeMap<>();
    targetAssignment.put("segment__1__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__1__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__1__2__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__2__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__2__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__2__2__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host2", "host4", "host6"), ONLINE));
    targetAssignment.put("segment__3__0__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__3__1__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));
    targetAssignment.put("segment__3__2__98347869999L",
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3", "host5"), ONLINE));

    // Number of segments to offload:
    // {
    //   "host1": 0,
    //   "host2": 4,
    //   "host3": 2,
    //   "host4": 0,
    //   "host5": -6,
    //   "host6": -1,
    //   "host7": 1
    // }
    numSegmentsToOffloadMap =
        TableRebalancer.getNumSegmentsToOffloadMap(currentAssignment, targetAssignment);
    assertEquals(numSegmentsToOffloadMap.size(), 7);
    assertEquals((int) numSegmentsToOffloadMap.get("host1"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host2"), 4);
    assertEquals((int) numSegmentsToOffloadMap.get("host3"), 2);
    assertEquals((int) numSegmentsToOffloadMap.get("host4"), 0);
    assertEquals((int) numSegmentsToOffloadMap.get("host5"), -6);
    assertEquals((int) numSegmentsToOffloadMap.get("host6"), -1);
    assertEquals((int) numSegmentsToOffloadMap.get("host7"), 1);

    // Next assignment with 2 minimum available replicas without strict replica-group should reach the target
    // assignment after three steps if strict replica group is enabled or disabled. Even for strictReplicaGroup,
    // though the full Pair(currentAssignment, targetAssignment) + partitionId moves as a block, it can take longer
    // to move since this can be a smaller granularity. Batch size = 2, unique partitionIds
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Object2IntOpenHashMap<String> segmentToPartitionIdMap = new Object2IntOpenHashMap<>();
      nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              2, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      if (!enableStrictReplicaGroup) {
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host6")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host7")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host6")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                2, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host6")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                2, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      } else {
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__1__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host3", "host4")));
        assertEquals(nextAssignment.get("segment__2__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                2, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                2, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      }
      assertEquals(nextAssignment, targetAssignment);
    }

    // Next assignment with 2 minimum available replicas without strict replica-group should reach the target
    // assignment after two steps if strict replica group is disabled or enabled. Batch size = 5, unique partitionIds
    for (boolean enableStrictReplicaGroup : Arrays.asList(false, true)) {
      Object2IntOpenHashMap<String> segmentToPartitionIdMap = new Object2IntOpenHashMap<>();
      nextAssignment =
          TableRebalancer.getNextAssignment(currentAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
              5, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      if (!enableStrictReplicaGroup) {
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        // host5 has reached batchSizePerServer due to which this segment didn't move in the first step
        assertEquals(nextAssignment.get("segment__3__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host6")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                2, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      } else {
        // This would have completed in a single step for batchSizePerServer = 5 if we did not have an additional check
        // to only add segments that exceed the batchSizePerServer if no prior segments were already assigned to a
        // given server
        assertNotEquals(nextAssignment, targetAssignment);
        assertEquals(nextAssignment.get("segment__1__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__1__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        assertEquals(nextAssignment.get("segment__2__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__2__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host2", "host4", "host6")));
        assertEquals(nextAssignment.get("segment__3__0__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__1__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host2", "host3")));
        assertEquals(nextAssignment.get("segment__3__2__98347869999L").keySet(),
            new TreeSet<>(Arrays.asList("host1", "host3", "host5")));
        nextAssignment =
            TableRebalancer.getNextAssignment(nextAssignment, targetAssignment, 2, enableStrictReplicaGroup, false,
                2, segmentToPartitionIdMap, SIMPLE_PARTITION_FETCHER);
      }
      assertEquals(nextAssignment, targetAssignment);
    }
  }
}
