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
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.tier.PinotServerTierStorage;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests the {@link RealtimeSegmentAssignment#rebalanceTable} method for table with tiers
 */
public class RealtimeNonReplicaGroupTieredSegmentAssignmentTest {
  private static final int NUM_REPLICAS = 3;
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

  private static final String TIER_A_NAME = "tierA";
  private static final String TIER_B_NAME = "tierB";
  private static final String TIER_C_NAME = "tierC";
  private static final String TAG_A_NAME = "tagA_OFFLINE";
  private static final String TAG_B_NAME = "tagB_OFFLINE";
  private static final String TAG_C_NAME = "tagC_OFFLINE";

  private static final String TIER_A_INSTANCE_NAME_PREFIX = "tierA_instance_";
  private static final int NUM_INSTANCES_TIER_A = 6;
  private static final List<String> INSTANCES_TIER_A =
      SegmentAssignmentTestUtils.getNameList(TIER_A_INSTANCE_NAME_PREFIX, NUM_INSTANCES_TIER_A);
  private static final String TIER_A_INSTANCE_PARTITIONS_NAME =
      InstancePartitionsUtils.getInstancePartitionsName(RAW_TABLE_NAME, TIER_A_NAME);

  private static final String TIER_B_INSTANCE_NAME_PREFIX = "tierB_instance_";
  private static final int NUM_INSTANCES_TIER_B = 4;
  private static final List<String> INSTANCES_TIER_B =
      SegmentAssignmentTestUtils.getNameList(TIER_B_INSTANCE_NAME_PREFIX, NUM_INSTANCES_TIER_B);
  private static final String TIER_B_INSTANCE_PARTITIONS_NAME =
      InstancePartitionsUtils.getInstancePartitionsName(RAW_TABLE_NAME, TIER_B_NAME);

  private static final String TIER_C_INSTANCE_NAME_PREFIX = "tierC_instance_";
  private static final int NUM_INSTANCES_TIER_C = 3;
  private static final List<String> INSTANCES_TIER_C =
      SegmentAssignmentTestUtils.getNameList(TIER_C_INSTANCE_NAME_PREFIX, NUM_INSTANCES_TIER_C);
  private static final String TIER_C_INSTANCE_PARTITIONS_NAME =
      InstancePartitionsUtils.getInstancePartitionsName(RAW_TABLE_NAME, TIER_C_NAME);

  private List<String> _segments;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMap;
  private Map<String, InstancePartitions> _tierInstancePartitionsMap;
  private List<Tier> _sortedTiers;
  private SegmentAssignment _segmentAssignment;

  @BeforeClass
  public void setUp() {
    _segments = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      _segments.add(new LLCSegmentName(RAW_TABLE_NAME, segmentId % NUM_PARTITIONS, segmentId / NUM_PARTITIONS,
          System.currentTimeMillis()).getSegmentName());
    }

    List<TierConfig> tierConfigList = Lists.newArrayList(
        new TierConfig(TIER_A_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, TAG_A_NAME, null, null),
        new TierConfig(TIER_B_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "20d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, TAG_B_NAME, null, null),
        new TierConfig(TIER_C_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, TAG_C_NAME, null, null));

    Map<String, String> streamConfigs = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setTierConfigList(tierConfigList).setStreamConfigs(streamConfigs).build();
    _segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(null, tableConfig);

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

    InstancePartitions instancePartitionsTierA = new InstancePartitions(TIER_A_INSTANCE_PARTITIONS_NAME);
    instancePartitionsTierA.setInstances(0, 0, INSTANCES_TIER_A);
    InstancePartitions instancePartitionsTierB = new InstancePartitions(TIER_B_INSTANCE_PARTITIONS_NAME);
    instancePartitionsTierB.setInstances(0, 0, INSTANCES_TIER_B);
    InstancePartitions instancePartitionsTierC = new InstancePartitions(TIER_C_INSTANCE_PARTITIONS_NAME);
    instancePartitionsTierC.setInstances(0, 0, INSTANCES_TIER_C);
    _tierInstancePartitionsMap = new HashMap<>();
    _tierInstancePartitionsMap.put(TIER_A_NAME, instancePartitionsTierA);
    _tierInstancePartitionsMap.put(TIER_B_NAME, instancePartitionsTierB);
    _tierInstancePartitionsMap.put(TIER_C_NAME, instancePartitionsTierC);

    _sortedTiers = Lists.newArrayList(
        new Tier(TIER_C_NAME, new TestSegmentSelectorC(), new PinotServerTierStorage(TAG_C_NAME, null, null)),
        new Tier(TIER_B_NAME, new TestSegmentSelectorB(), new PinotServerTierStorage(TAG_B_NAME, null, null)),
        new Tier(TIER_A_NAME, new TestSegmentSelectorA(), new PinotServerTierStorage(TAG_A_NAME, null, null)));
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

    // Rebalance without tier instancePartitions moves all instances to COMPLETED
    Map<String, Map<String, String>> newAssignment =
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, null, null,
            new BaseConfiguration());
    assertEquals(newAssignment.size(), NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      if (segmentId < NUM_SEGMENTS - NUM_PARTITIONS) { // ONLINE segments
        Map<String, String> instanceStateMap = newAssignment.get(_segments.get(segmentId));
        assertTrue(COMPLETED_INSTANCES.containsAll(instanceStateMap.keySet()));
        for (String value : instanceStateMap.values()) {
          assertEquals(value, SegmentStateModel.ONLINE);
        }
      } else { // CONSUMING segments
        Map<String, String> instanceStateMap = newAssignment.get(_segments.get(segmentId));
        for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
          assertTrue(entry.getKey().startsWith(CONSUMING_INSTANCE_NAME_PREFIX));
          assertEquals(entry.getValue(), SegmentStateModel.CONSUMING);
        }
      }
    }

    // Rebalance with tierInstanceAssignment should relocate ONLINE segments to tiers or to the COMPLETED instances
    int expectedOnTierA = 40;
    int expectedOnTierB = 20;
    int expectedOnCompleted = NUM_SEGMENTS - NUM_PARTITIONS - expectedOnTierA - expectedOnTierB;
    newAssignment = _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, _sortedTiers,
        _tierInstancePartitionsMap, new BaseConfiguration());
    assertEquals(newAssignment.size(), NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      if (segmentId < NUM_SEGMENTS - NUM_PARTITIONS) {
        // ONLINE segments
        Map<String, String> instanceStateMap = newAssignment.get(_segments.get(segmentId));
        if (segmentId < 20) { // tierB
          assertTrue(INSTANCES_TIER_B.containsAll(instanceStateMap.keySet()));
        } else if (segmentId < 60) { // tierA
          assertTrue(INSTANCES_TIER_A.containsAll(instanceStateMap.keySet()));
        } else { // COMPLETED
          assertTrue(COMPLETED_INSTANCES.containsAll(instanceStateMap.keySet()));
        }
        for (String value : instanceStateMap.values()) {
          assertEquals(value, SegmentStateModel.ONLINE);
        }
      } else {
        // CONSUMING segments
        Map<String, String> instanceStateMap = newAssignment.get(_segments.get(segmentId));
        for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
          assertTrue(entry.getKey().startsWith(CONSUMING_INSTANCE_NAME_PREFIX));
          assertEquals(entry.getValue(), SegmentStateModel.CONSUMING);
        }
      }
    }
    // Relocated segments should be balanced
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, COMPLETED_INSTANCES);
    assertEquals(numSegmentsAssignedPerInstance.length, NUM_COMPLETED_INSTANCES);
    int expectedMinNumSegmentsPerInstance = expectedOnCompleted / NUM_COMPLETED_INSTANCES;
    for (int i = 0; i < NUM_COMPLETED_INSTANCES; i++) {
      assertTrue(numSegmentsAssignedPerInstance[i] >= expectedMinNumSegmentsPerInstance);
    }
    // TierA segments should be balanced
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, INSTANCES_TIER_A);
    assertEquals(numSegmentsAssignedPerInstance.length, NUM_INSTANCES_TIER_A);
    expectedMinNumSegmentsPerInstance = expectedOnTierA / NUM_INSTANCES_TIER_A;
    for (int i = 0; i < NUM_INSTANCES_TIER_A; i++) {
      assertTrue(numSegmentsAssignedPerInstance[i] >= expectedMinNumSegmentsPerInstance);
    }
    // TierB segments should be balanced
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, INSTANCES_TIER_B);
    assertEquals(numSegmentsAssignedPerInstance.length, NUM_INSTANCES_TIER_B);
    expectedMinNumSegmentsPerInstance = expectedOnCompleted / NUM_INSTANCES_TIER_B;
    for (int i = 0; i < NUM_INSTANCES_TIER_B; i++) {
      assertTrue(numSegmentsAssignedPerInstance[i] >= expectedMinNumSegmentsPerInstance);
    }

    // Rebalance with including CONSUMING should give the same assignment
    BaseConfiguration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.INCLUDE_CONSUMING, true);
    assertEquals(_segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, _sortedTiers,
        _tierInstancePartitionsMap, rebalanceConfig), newAssignment);

    // Rebalance without COMPLETED instance partitions and without tierInstancePartitions again should change the
    // segment assignment back
    Map<InstancePartitionsType, InstancePartitions> noRelocationInstancePartitionsMap = new TreeMap<>();
    noRelocationInstancePartitionsMap.put(InstancePartitionsType.CONSUMING,
        _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    assertEquals(_segmentAssignment.rebalanceTable(newAssignment, noRelocationInstancePartitionsMap, null, null,
        new BaseConfiguration()), currentAssignment);

    // Rebalance without COMPLETED instance partitions and with tierInstancePartitions should move ONLINE segments to
    // Tiers and CONSUMING segments to CONSUMING tenant.
    newAssignment =
        _segmentAssignment.rebalanceTable(currentAssignment, noRelocationInstancePartitionsMap, _sortedTiers,
            _tierInstancePartitionsMap, new BaseConfiguration());

    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, INSTANCES_TIER_A);
    assertEquals(numSegmentsAssignedPerInstance.length, NUM_INSTANCES_TIER_A);
    expectedMinNumSegmentsPerInstance = expectedOnTierA / NUM_INSTANCES_TIER_A;
    for (int i = 0; i < NUM_INSTANCES_TIER_A; i++) {
      assertTrue(numSegmentsAssignedPerInstance[i] >= expectedMinNumSegmentsPerInstance);
    }

    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, INSTANCES_TIER_B);
    assertEquals(numSegmentsAssignedPerInstance.length, NUM_INSTANCES_TIER_B);
    expectedMinNumSegmentsPerInstance = expectedOnTierB / NUM_INSTANCES_TIER_B;
    for (int i = 0; i < NUM_INSTANCES_TIER_B; i++) {
      assertTrue(numSegmentsAssignedPerInstance[i] >= expectedMinNumSegmentsPerInstance);
    }

    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, CONSUMING_INSTANCES);
    assertEquals(numSegmentsAssignedPerInstance.length, NUM_CONSUMING_INSTANCES);

    // Bootstrap
    rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.BOOTSTRAP, true);
    newAssignment = _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, _sortedTiers,
        _tierInstancePartitionsMap, rebalanceConfig);
    int index = 0;
    int indexA = 0;
    int indexB = 0;
    for (Map.Entry<String, Map<String, String>> entry : newAssignment.entrySet()) {
      String segmentName = entry.getKey();
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      Map<String, String> instanceStateMap = entry.getValue();
      if (instanceStateMap.containsValue(SegmentStateModel.ONLINE)) {
        if (llcSegmentName.getSequenceNumber() < 5) {
          for (int i = 0; i < NUM_REPLICAS; i++) {
            String expectedInstance = INSTANCES_TIER_B.get(indexB++ % NUM_INSTANCES_TIER_B);
            assertEquals(instanceStateMap.get(expectedInstance), SegmentStateModel.ONLINE);
          }
        } else if (llcSegmentName.getSequenceNumber() < 15) {
          for (int i = 0; i < NUM_REPLICAS; i++) {
            String expectedInstance = INSTANCES_TIER_A.get(indexA++ % NUM_INSTANCES_TIER_A);
            assertEquals(instanceStateMap.get(expectedInstance), SegmentStateModel.ONLINE);
          }
        } else {
          for (int i = 0; i < NUM_REPLICAS; i++) {
            String expectedInstance = COMPLETED_INSTANCES.get(index++ % NUM_COMPLETED_INSTANCES);
            assertEquals(instanceStateMap.get(expectedInstance), SegmentStateModel.ONLINE);
          }
        }
      } else {
        // CONSUMING and OFFLINE segments should not be reassigned
        assertEquals(instanceStateMap, currentAssignment.get(segmentName));
      }
    }
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

  /**
   * Selects segments with sequence number 5-14 i.e. 10 segments per partition (40 segments)
   */
  private static class TestSegmentSelectorA implements TierSegmentSelector {
    @Override
    public String getType() {
      return "TestSegmentSelectorA";
    }

    @Override
    public boolean selectSegment(String tableNameWithType, String segmentName) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      return llcSegmentName.getSequenceNumber() >= 5 && llcSegmentName.getSequenceNumber() < 15;
    }
  }

  /**
   * Selects segments with sequence number 0-4 i.e. 5 segments per partition (20 segments)
   */
  private static class TestSegmentSelectorB implements TierSegmentSelector {
    @Override
    public String getType() {
      return "TestSegmentSelectorB";
    }

    @Override
    public boolean selectSegment(String tableNameWithType, String segmentName) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      return llcSegmentName.getSequenceNumber() >= 0 && llcSegmentName.getSequenceNumber() < 5;
    }
  }

  /**
   * Selects no segments
   */
  private static class TestSegmentSelectorC implements TierSegmentSelector {
    @Override
    public String getType() {
      return "TestSegmentSelectorC";
    }

    @Override
    public boolean selectSegment(String tableNameWithType, String segmentName) {
      return false;
    }
  }
}
