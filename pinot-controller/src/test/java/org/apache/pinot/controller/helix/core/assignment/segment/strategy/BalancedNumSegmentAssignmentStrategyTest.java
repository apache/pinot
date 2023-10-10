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
package org.apache.pinot.controller.helix.core.assignment.segment.strategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.segment.OfflineSegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentTestUtils;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class BalancedNumSegmentAssignmentStrategyTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final int NUM_SEGMENTS = 100;
  private static final List<String> SEGMENTS =
      SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, NUM_SEGMENTS);
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 10;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String RAW_TABLE_NAME = "assignmentTable";
  private static final String INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME);
  private SegmentAssignment _segmentAssignment;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMap;

  @BeforeClass
  public void setUp() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    _segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(null, tableConfig, null);

    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7,
    //   instance_8, instance_9]
    // }
    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    instancePartitions.setInstances(0, 0, INSTANCES);
    _instancePartitionsMap = Collections.singletonMap(InstancePartitionsType.OFFLINE, instancePartitions);
  }

  @Test
  public void testFactory() {
    assertTrue(_segmentAssignment instanceof OfflineSegmentAssignment);
  }

  @Test
  public void testAssignSegment() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // Segment 0 should be assigned to instance 0, 1, 2
    // Segment 1 should be assigned to instance 3, 4, 5
    // Segment 2 should be assigned to instance 6, 7, 8
    // Segment 3 should be assigned to instance 9, 0, 1
    // Segment 4 should be assigned to instance 2, 3, 4
    // ...
    int expectedAssignedInstanceId = 0;
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, _instancePartitionsMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        assertEquals(instancesAssigned.get(replicaId), INSTANCES.get(expectedAssignedInstanceId));
        expectedAssignedInstanceId = (expectedAssignedInstanceId + 1) % NUM_INSTANCES;
      }
      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
  }

  @Test
  public void testTableBalanced() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, _instancePartitionsMap);
      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    // There should be 100 segments assigned
    assertEquals(currentAssignment.size(), NUM_SEGMENTS);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, INSTANCES);
    int[] expectedNumSegmentsAssignedPerInstance = new int[NUM_INSTANCES];
    int numSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Current assignment should already be balanced
    assertEquals(
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, null, null, new RebalanceConfig()),
        currentAssignment);
  }

  @Test
  public void testBootstrapTable() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, _instancePartitionsMap);
      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    // Bootstrap table should reassign all segments based on their alphabetical order
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setBootstrap(true);
    Map<String, Map<String, String>> newAssignment =
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, null, null, rebalanceConfig);
    assertEquals(newAssignment.size(), NUM_SEGMENTS);
    List<String> sortedSegments = new ArrayList<>(SEGMENTS);
    sortedSegments.sort(null);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      assertEquals(newAssignment.get(sortedSegments.get(i)), currentAssignment.get(SEGMENTS.get(i)));
    }
  }
}
