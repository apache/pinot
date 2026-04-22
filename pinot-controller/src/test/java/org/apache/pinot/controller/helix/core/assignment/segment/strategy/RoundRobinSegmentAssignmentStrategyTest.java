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
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RoundRobinSegmentAssignmentStrategyTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final int NUM_SEGMENTS = 100;
  private static final List<String> SEGMENTS =
      SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, NUM_SEGMENTS);
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 10;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  // Use a unique table name to avoid counter interference with other test classes
  private static final String RAW_TABLE_NAME = "roundRobinAssignmentTable";
  private static final String INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME);

  private SegmentAssignment _segmentAssignment;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMap;

  @BeforeClass
  public void setUp() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setSegmentAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new SegmentAssignmentConfig(AssignmentStrategy.ROUND_ROBIN_SEGMENT_ASSIGNMENT_STRATEGY))).build();
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

    // The counter initializes to a random starting value, so derive the start from the first observed result.
    List<String> firstInstances =
        _segmentAssignment.assignSegment(SEGMENTS.get(0), currentAssignment, _instancePartitionsMap);
    assertEquals(firstInstances.size(), NUM_REPLICAS);
    // Counter advances once per replica, so replicas within each segment occupy consecutive instance slots.
    int startIdx = INSTANCES.indexOf(firstInstances.get(0));
    for (int r = 0; r < NUM_REPLICAS; r++) {
      assertEquals(firstInstances.get(r), INSTANCES.get((startIdx - r + NUM_INSTANCES) % NUM_INSTANCES));
    }
    currentAssignment.put(SEGMENTS.get(0),
        SegmentAssignmentUtils.getInstanceStateMap(firstInstances, SegmentStateModel.ONLINE));

    // Each subsequent segment begins exactly where the previous segment's last replica left off.
    int nextIdx = (startIdx + NUM_REPLICAS) % NUM_INSTANCES;
    for (int segId = 1; segId < NUM_SEGMENTS; segId++) {
      List<String> instances =
          _segmentAssignment.assignSegment(SEGMENTS.get(segId), currentAssignment, _instancePartitionsMap);
      assertEquals(instances.size(), NUM_REPLICAS);
      for (int r = 0; r < NUM_REPLICAS; r++) {
        assertEquals(instances.get(r), INSTANCES.get((nextIdx - r + NUM_INSTANCES) % NUM_INSTANCES));
      }
      nextIdx = (nextIdx + NUM_REPLICAS) % NUM_INSTANCES;
      currentAssignment.put(SEGMENTS.get(segId),
          SegmentAssignmentUtils.getInstanceStateMap(instances, SegmentStateModel.ONLINE));
    }
  }

  @Test
  public void testAssignSegmentWithChangedInstanceCount() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // Assign one segment to observe the current counter position
    List<String> firstInstances =
        _segmentAssignment.assignSegment("changeTest_0", currentAssignment, _instancePartitionsMap);
    int prevInstanceId = INSTANCES.indexOf(firstInstances.get(0));

    // Switch to a new instance list with a different count (7 instead of 10)
    int newNumInstances = 7;
    List<String> newInstances = SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, newNumInstances);
    InstancePartitions newInstancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    newInstancePartitions.setInstances(0, 0, newInstances);
    Map<InstancePartitionsType, InstancePartitions> newInstancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.OFFLINE, newInstancePartitions);

    // Counter after the first assignment is prevInstanceId. Next compute advances by NUM_REPLICAS modulo the new size.
    int expectedStartIdx = (prevInstanceId + NUM_REPLICAS) % newNumInstances;
    List<String> secondInstances =
        _segmentAssignment.assignSegment("changeTest_1", currentAssignment, newInstancePartitionsMap);
    assertEquals(secondInstances.size(), NUM_REPLICAS);
    for (int r = 0; r < NUM_REPLICAS; r++) {
      assertEquals(secondInstances.get(r),
          newInstances.get((expectedStartIdx - r + newNumInstances) % newNumInstances));
    }
  }
}
