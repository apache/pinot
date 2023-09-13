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

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.tier.PinotServerTierStorage;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests the {@link OfflineSegmentAssignment#rebalanceTable} method for table with tiers
 */
public class OfflineNonReplicaGroupTieredSegmentAssignmentTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final int NUM_SEGMENTS = 100;
  private static final List<String> SEGMENTS =
      SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, NUM_SEGMENTS);
  private static final String RAW_TABLE_NAME = "testTable";

  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 10;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME);

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

  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMap;
  private Map<String, InstancePartitions> _tierInstancePartitionsMap;
  private List<Tier> _sortedTiers;
  private SegmentAssignment _segmentAssignment;

  @BeforeClass
  public void setUp() {
    List<TierConfig> tierConfigList = Lists.newArrayList(
        new TierConfig(TIER_A_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "50d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, TAG_A_NAME, null, null),
        new TierConfig(TIER_B_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "70d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, TAG_B_NAME, null, null),
        new TierConfig(TIER_C_NAME, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "120d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, TAG_C_NAME, null, null));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS)
            .setTierConfigList(tierConfigList).build();

    _segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(null, tableConfig);

    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5, instance_6, instance_7,
    //   instance_8, instance_9]
    // }
    InstancePartitions instancePartitionsOffline = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    instancePartitionsOffline.setInstances(0, 0, INSTANCES);
    _instancePartitionsMap = Collections.singletonMap(InstancePartitionsType.OFFLINE, instancePartitionsOffline);
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
  public void testTableBalanced() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, _instancePartitionsMap);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
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

    // On rebalancing, segments move to tiers
    Map<String, Map<String, String>> newAssignment =
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, _sortedTiers,
            _tierInstancePartitionsMap, new BaseConfiguration());
    assertEquals(newAssignment.size(), NUM_SEGMENTS);

    // segments 0-49 remain unchanged
    for (int i = 0; i < 49; i++) {
      String segmentName = SEGMENT_NAME_PREFIX + i;
      assertEquals(newAssignment.get(segmentName), currentAssignment.get(segmentName));
    }
    // segments 50-69 go to tierA
    // segments 70-99 go to tierB
    int expectedOnTierA = 20;
    int expectedOnTierB = 30;
    for (int i = 50; i < 100; i++) {
      String segmentName = SEGMENT_NAME_PREFIX + i;
      if (i < 70) {
        Assert.assertTrue(INSTANCES_TIER_A.containsAll(newAssignment.get(segmentName).keySet()));
      } else {
        Assert.assertTrue(INSTANCES_TIER_B.containsAll(newAssignment.get(segmentName).keySet()));
      }
    }

    // TierA segments should be balanced
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, INSTANCES_TIER_A);
    assertEquals(numSegmentsAssignedPerInstance.length, NUM_INSTANCES_TIER_A);
    int expectedMinNumSegmentsPerInstance = expectedOnTierA / NUM_INSTANCES_TIER_A;
    for (int i = 0; i < NUM_INSTANCES_TIER_A; i++) {
      assertTrue(numSegmentsAssignedPerInstance[i] >= expectedMinNumSegmentsPerInstance);
    }
    // TierB segments should be balanced
    numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(newAssignment, INSTANCES_TIER_B);
    assertEquals(numSegmentsAssignedPerInstance.length, NUM_INSTANCES_TIER_B);
    expectedMinNumSegmentsPerInstance = expectedOnTierB / NUM_INSTANCES_TIER_B;
    for (int i = 0; i < NUM_INSTANCES_TIER_B; i++) {
      assertTrue(numSegmentsAssignedPerInstance[i] >= expectedMinNumSegmentsPerInstance);
    }

    // rebalance without tierInstancePartitions resets the assignment
    Map<String, Map<String, String>> resetAssignment =
        _segmentAssignment.rebalanceTable(newAssignment, _instancePartitionsMap, null, null, new BaseConfiguration());
    for (String segment : SEGMENTS) {
      Assert.assertTrue(INSTANCES.containsAll(resetAssignment.get(segment).keySet()));
    }
  }

  @Test
  public void testBootstrapTable() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned =
          _segmentAssignment.assignSegment(segmentName, currentAssignment, _instancePartitionsMap);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    // Bootstrap table should reassign all segments
    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.BOOTSTRAP, true);
    Map<String, Map<String, String>> newAssignment =
        _segmentAssignment.rebalanceTable(currentAssignment, _instancePartitionsMap, _sortedTiers,
            _tierInstancePartitionsMap, new BaseConfiguration());
    assertEquals(newAssignment.size(), NUM_SEGMENTS);

    // segments 0-49 remain unchanged
    for (int i = 0; i < 49; i++) {
      String segmentName = SEGMENT_NAME_PREFIX + i;
      assertEquals(newAssignment.get(segmentName), currentAssignment.get(segmentName));
    }
    // segments 50-69 go to tierA
    // segments 70-99 go to tierB
    for (int i = 50; i < 100; i++) {
      String segmentName = SEGMENT_NAME_PREFIX + i;
      for (String instance : newAssignment.get(segmentName).keySet()) {
        if (i < 70) {
          Assert.assertTrue(instance.startsWith(TIER_A_INSTANCE_NAME_PREFIX));
        } else {
          Assert.assertTrue(instance.startsWith(TIER_B_INSTANCE_NAME_PREFIX));
        }
      }
    }
  }

  /**
   * Selects segment_50 to segment_69 i.e. 20 segments
   */
  private static class TestSegmentSelectorA implements TierSegmentSelector {
    @Override
    public String getType() {
      return "TestSegmentSelectorA";
    }

    @Override
    public boolean selectSegment(String tableNameWithType, String segmentName) {
      int segId = Integer.parseInt(segmentName.split("_")[1]);
      return segId >= 50 && segId < 70;
    }
  }

  /**
   * Selects segment_70 to segment_99 i.e. 30 segments
   */
  private static class TestSegmentSelectorB implements TierSegmentSelector {
    @Override
    public String getType() {
      return "TestSegmentSelectorB";
    }

    @Override
    public boolean selectSegment(String tableNameWithType, String segmentName) {
      int segId = Integer.parseInt(segmentName.split("_")[1]);
      return segId >= 70 && segId < 100;
    }
  }

  /**
   * Selects segments >= segment_120 i.e. 0 segments
   */
  private static class TestSegmentSelectorC implements TierSegmentSelector {
    @Override
    public String getType() {
      return "TestSegmentSelectorC";
    }

    @Override
    public boolean selectSegment(String tableNameWithType, String segmentName) {
      int segId = Integer.parseInt(segmentName.split("_")[1]);
      return segId >= 120;
    }
  }
}
