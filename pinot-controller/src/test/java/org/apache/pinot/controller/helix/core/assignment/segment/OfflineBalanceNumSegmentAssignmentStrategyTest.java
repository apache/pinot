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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class OfflineBalanceNumSegmentAssignmentStrategyTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final int NUM_SEGMENTS = 100;
  private static final List<String> SEGMENTS =
      SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, NUM_SEGMENTS);
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 10;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME);

  private SegmentAssignmentStrategy _strategy;

  @BeforeClass
  public void setUp() {
    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7, instance_8, instance_9]
    // }
    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    instancePartitions.setInstances(0, 0, INSTANCES);

    // Mock HelixManager
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore
        .get(eq(ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(INSTANCE_PARTITIONS_NAME)), any(),
            anyInt())).thenReturn(instancePartitions.toZNRecord());
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    TableConfig tableConfig =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    _strategy = SegmentAssignmentStrategyFactory.getSegmentAssignmentStrategy(helixManager, tableConfig);
  }

  @Test
  public void testFactory() {
    assertTrue(_strategy instanceof OfflineBalanceNumSegmentAssignmentStrategy);
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
      List<String> instancesAssigned = _strategy.assignSegment(segmentName, currentAssignment);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      for (int replicaId = 0; replicaId < NUM_REPLICAS; replicaId++) {
        assertEquals(instancesAssigned.get(replicaId), INSTANCES.get(expectedAssignedInstanceId));
        expectedAssignedInstanceId = (expectedAssignedInstanceId + 1) % NUM_INSTANCES;
      }
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }
  }

  @Test
  public void testTableBalanced() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned = _strategy.assignSegment(segmentName, currentAssignment);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }

    // There should be 100 segments assigned
    assertEquals(currentAssignment.size(), NUM_SEGMENTS);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 30 segments assigned
    int[] numSegmentsAssigned = SegmentAssignmentUtils.getNumSegmentsAssigned(currentAssignment, INSTANCES);
    int[] expectedNumSegmentsAssigned = new int[NUM_INSTANCES];
    int numSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    Arrays.fill(expectedNumSegmentsAssigned, numSegmentsPerInstance);
    assertEquals(numSegmentsAssigned, expectedNumSegmentsAssigned);
    // Current assignment should already be balanced
    assertEquals(_strategy.rebalanceTable(currentAssignment, null), currentAssignment);
  }
}
