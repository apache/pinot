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
import java.util.Map;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TableRebalancerTest {
  private static final String ONLINE = CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE;

  @Test
  public void testDowntimeMode() {
    // With common instance, next assignment should be the same as target assignment
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host3"), ONLINE);
    Map<String, String> nextInstanceStateMap =
        TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 0);
    assertEquals(nextInstanceStateMap, targetInstanceStateMap);

    // Without common instance, next assignment should be the same as target assignment
    targetInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host3", "host4"), ONLINE);
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 0);
    assertEquals(nextInstanceStateMap, targetInstanceStateMap);

    // With increasing number of replicas, next assignment should be the same as target assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host3", "host4", "host5"), ONLINE);
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 0);
    assertEquals(nextInstanceStateMap, targetInstanceStateMap);

    // With decreasing number of replicas, next assignment should be the same as target assignment
    currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE);
    targetInstanceStateMap = SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5"), ONLINE);
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 0);
    assertEquals(nextInstanceStateMap, targetInstanceStateMap);
  }

  @Test
  public void testOneMinAvailableReplicas() {
    // With 2 common instances, next assignment should be the same as target assignment
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host4"), ONLINE);
    Map<String, String> nextInstanceStateMap =
        TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(nextInstanceStateMap, targetInstanceStateMap);

    // With 1 common instance, next assignment should be the same as target assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host4", "host5"), ONLINE);
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(nextInstanceStateMap, targetInstanceStateMap);

    // Without common instance, next assignment should have 1 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5", "host6"), ONLINE);
    // [host1, host4, host5]
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(getNumCommonInstances(currentInstanceStateMap, nextInstanceStateMap), 1);
    // Next round should make the assignment the same as target assignment
    assertEquals(TableRebalancer.getNextInstanceStateMap(nextInstanceStateMap, targetInstanceStateMap, 1),
        targetInstanceStateMap);

    // With increasing number of replicas, next assignment should have 1 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host4", "host5", "host6", "host7"), ONLINE);
    // [host1, host4, host5, host6]
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(getNumCommonInstances(currentInstanceStateMap, nextInstanceStateMap), 1);
    // Next round should make the assignment the same as target assignment
    assertEquals(TableRebalancer.getNextInstanceStateMap(nextInstanceStateMap, targetInstanceStateMap, 1),
        targetInstanceStateMap);

    // With decreasing number of replicas, next assignment should have 1 common instances with current assignment
    currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host4"), ONLINE);
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7"), ONLINE);
    // [host1, host5, host6]
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 1);
    assertEquals(getNumCommonInstances(currentInstanceStateMap, nextInstanceStateMap), 1);
    // Next round should make the assignment the same as target assignment
    assertEquals(TableRebalancer.getNextInstanceStateMap(nextInstanceStateMap, targetInstanceStateMap, 1),
        targetInstanceStateMap);
  }

  @Test
  public void testTwoMinAvailableReplicas() {
    // With 3 common instances, next assignment should be the same as target assignment
    Map<String, String> currentInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host4"), ONLINE);
    Map<String, String> targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host3", "host5"), ONLINE);
    Map<String, String> nextInstanceStateMap =
        TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(nextInstanceStateMap, targetInstanceStateMap);

    // With 2 common instances, next assignment should be the same as target assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host2", "host5", "host6"), ONLINE);
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(nextInstanceStateMap, targetInstanceStateMap);

    // With 1 common instance, next assignment should have 2 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host1", "host5", "host6", "host7"), ONLINE);
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 2);
    // [host1, host2, host5, host6]
    assertEquals(getNumCommonInstances(currentInstanceStateMap, nextInstanceStateMap), 2);
    // Next round should make the assignment the same as target assignment
    assertEquals(TableRebalancer.getNextInstanceStateMap(nextInstanceStateMap, targetInstanceStateMap, 2),
        targetInstanceStateMap);

    // Without common instance, next assignment should have 2 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7", "host8"), ONLINE);
    // [host1, host2, host5, host6]
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(getNumCommonInstances(currentInstanceStateMap, nextInstanceStateMap), 2);
    // Next round should make the assignment the same as target assignment
    assertEquals(TableRebalancer.getNextInstanceStateMap(nextInstanceStateMap, targetInstanceStateMap, 2),
        targetInstanceStateMap);

    // With increasing number of replicas, next assignment should have 1 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7", "host8", "host9"), ONLINE);
    // [host1, host2, host5, host6, host7]
    nextInstanceStateMap = TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(getNumCommonInstances(currentInstanceStateMap, nextInstanceStateMap), 2);
    // Next round should make the assignment the same as target assignment
    assertEquals(TableRebalancer.getNextInstanceStateMap(nextInstanceStateMap, targetInstanceStateMap, 2),
        targetInstanceStateMap);

    // With decreasing number of replicas, next assignment should have 2 common instances with current assignment
    targetInstanceStateMap =
        SegmentAssignmentUtils.getInstanceStateMap(Arrays.asList("host5", "host6", "host7"), ONLINE);
    // [host1, host2, host5]
    Map<String, String> firstRoundInstanceStateMap =
        TableRebalancer.getNextInstanceStateMap(currentInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(getNumCommonInstances(currentInstanceStateMap, firstRoundInstanceStateMap), 2);
    // Next round should have 2 common instances with first round assignment
    // [host1, host5, host6]
    Map<String, String> secondRoundInstanceStateMap =
        TableRebalancer.getNextInstanceStateMap(firstRoundInstanceStateMap, targetInstanceStateMap, 2);
    assertEquals(getNumCommonInstances(firstRoundInstanceStateMap, secondRoundInstanceStateMap), 2);
    // Next round should make the assignment the same as target assignment
    assertEquals(TableRebalancer.getNextInstanceStateMap(secondRoundInstanceStateMap, targetInstanceStateMap, 2),
        targetInstanceStateMap);
  }

  private int getNumCommonInstances(Map<String, String> currentInstanceStateMap,
      Map<String, String> nextInstanceStateMap) {
    int numCommonInstances = 0;
    for (String instanceId : currentInstanceStateMap.keySet()) {
      if (nextInstanceStateMap.containsKey(instanceId)) {
        numCommonInstances++;
      }
    }
    return numCommonInstances;
  }
}
