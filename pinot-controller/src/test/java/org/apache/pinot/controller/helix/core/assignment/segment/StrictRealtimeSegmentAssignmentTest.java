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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.tier.PinotServerTierStorage;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("unchecked")
public class StrictRealtimeSegmentAssignmentTest {
  private static final int NUM_REPLICAS = 3;
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_SEGMENTS = 24;
  private static final String CONSUMING_INSTANCE_NAME_PREFIX = "consumingInstance_";
  private static final int NUM_CONSUMING_INSTANCES = 9;
  private static final List<String> CONSUMING_INSTANCES =
      SegmentAssignmentTestUtils.getNameList(CONSUMING_INSTANCE_NAME_PREFIX, NUM_CONSUMING_INSTANCES);
  private static final List<String> NEW_CONSUMING_INSTANCES =
      SegmentAssignmentTestUtils.getNameList("new_" + CONSUMING_INSTANCE_NAME_PREFIX, NUM_CONSUMING_INSTANCES);
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String CONSUMING_INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.CONSUMING.getInstancePartitionsName(RAW_TABLE_NAME);
  private static final String COLD_TIER_NAME = "coldTier";
  // Cold tier may have a different replication factor than the hot tier. We test both:
  //   - Fewer cold servers than NUM_REPLICAS (2 vs 3) — cold tier with reduced replication
  //   - Same as NUM_REPLICAS (3 vs 3) — cold tier matching hot tier replication
  private static final List<String> COLD_TIER_SERVERS = List.of("coldTier_server_0", "coldTier_server_1");
  private static final List<String> COLD_TIER_SERVERS_MATCHING_REPLICATION =
      List.of("coldTier_server_0", "coldTier_server_1", "coldTier_server_2");

  private List<String> _segments;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMap;
  private InstancePartitions _newConsumingInstancePartitions;

  @BeforeClass
  public void setUp() {
    _segments = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      _segments.add(new LLCSegmentName(RAW_TABLE_NAME, segmentId % NUM_PARTITIONS, segmentId / NUM_PARTITIONS,
          System.currentTimeMillis()).getSegmentName());
    }
    _instancePartitionsMap = new TreeMap<>();
    // CONSUMING instances:
    // {
    //   0_0=[instance_0, instance_1, instance_2],
    //   0_1=[instance_3, instance_4, instance_5],
    //   0_2=[instance_6, instance_7, instance_8]
    // }
    //        p0          p1          p2
    //        p3
    InstancePartitions consumingInstancePartitions = new InstancePartitions(CONSUMING_INSTANCE_PARTITIONS_NAME);
    int numConsumingInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    int consumingInstanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> consumingInstancesForReplicaGroup = new ArrayList<>(numConsumingInstancesPerReplicaGroup);
      for (int i = 0; i < numConsumingInstancesPerReplicaGroup; i++) {
        consumingInstancesForReplicaGroup.add(CONSUMING_INSTANCES.get(consumingInstanceIdToAdd++));
      }
      consumingInstancePartitions.setInstances(0, replicaGroupId, consumingInstancesForReplicaGroup);
    }
    _instancePartitionsMap.put(InstancePartitionsType.CONSUMING, consumingInstancePartitions);

    // new CONSUMING instances:
    // {
    //   0_0=[new_instance_0, new_instance_1, new_instance_2],
    //   0_1=[new_instance_3, new_instance_4, new_instance_5],
    //   0_2=[new_instance_6, new_instance_7, new_instance_8]
    // }
    consumingInstanceIdToAdd = 0;
    _newConsumingInstancePartitions = new InstancePartitions(CONSUMING_INSTANCE_PARTITIONS_NAME);
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> consumingInstancesForReplicaGroup = new ArrayList<>(numConsumingInstancesPerReplicaGroup);
      for (int i = 0; i < numConsumingInstancesPerReplicaGroup; i++) {
        consumingInstancesForReplicaGroup.add(NEW_CONSUMING_INSTANCES.get(consumingInstanceIdToAdd++));
      }
      _newConsumingInstancePartitions.setInstances(0, replicaGroupId, consumingInstancesForReplicaGroup);
    }
  }

  @DataProvider(name = "tableTypes")
  public Object[] getTableTypes() {
    return new Object[]{"upsert", "dedup"};
  }

  // Cold tier may have a different replication factor than hot tier. Tests using this provider run twice:
  // once with cold replication < NUM_REPLICAS, once with cold replication == NUM_REPLICAS.
  @DataProvider(name = "coldTierReplications")
  public Object[][] getColdTierReplications() {
    return new Object[][]{
        {COLD_TIER_SERVERS},
        {COLD_TIER_SERVERS_MATCHING_REPLICATION}
    };
  }

  private static SegmentAssignment createSegmentAssignment(String tableType) {
    TableConfigBuilder builder = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setNumReplicas(NUM_REPLICAS)
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap())
        .setSegmentAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.COMPLETED.toString(),
            new SegmentAssignmentConfig(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY)))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(PARTITION_COLUMN, 1));
    TableConfig tableConfig;
    if ("upsert".equalsIgnoreCase(tableType)) {
      tableConfig = builder.setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).build();
    } else {
      tableConfig = builder.setDedupConfig(new DedupConfig()).build();
    }
    SegmentAssignment segmentAssignment =
        SegmentAssignmentFactory.getSegmentAssignment(createHelixManager(), tableConfig, null);
    assertSegmentAssignmentType(segmentAssignment, tableType);
    return segmentAssignment;
  }

  private static SegmentAssignment createSegmentAssignmentWithTiers(String tableType) {
    return createSegmentAssignmentWithTiers(tableType, COLD_TIER_SERVERS);
  }

  private static SegmentAssignment createSegmentAssignmentWithTiers(String tableType, List<String> coldTierServers) {
    TableConfigBuilder builder = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setNumReplicas(NUM_REPLICAS)
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap())
        .setSegmentAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.COMPLETED.toString(),
            new SegmentAssignmentConfig(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY)))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(PARTITION_COLUMN, 1))
        .setTierConfigList(List.of(
            new TierConfig(COLD_TIER_NAME, "time", "6h", null, "pinot_server", "cold_REALTIME", null, null)));
    TableConfig tableConfig;
    if ("upsert".equalsIgnoreCase(tableType)) {
      tableConfig = builder.setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).build();
    } else {
      tableConfig = builder.setDedupConfig(new DedupConfig()).build();
    }
    SegmentAssignment segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(
        createHelixManagerWithTiers(COLD_TIER_NAME, coldTierServers), tableConfig, null);
    assertSegmentAssignmentType(segmentAssignment, tableType);
    return segmentAssignment;
  }

  private static void assertSegmentAssignmentType(SegmentAssignment segmentAssignment, String tableType) {
    if ("upsert".equalsIgnoreCase(tableType)) {
      assertTrue(segmentAssignment instanceof SingleTierStrictRealtimeSegmentAssignment);
    } else {
      assertTrue(segmentAssignment instanceof MultiTierStrictRealtimeSegmentAssignment);
    }
  }

  @Test(dataProvider = "tableTypes")
  public void testAssignSegment(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    int numInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    // Add segments for partition 0/1/2, but add no segment for partition 3.
    List<String> instancesAssigned;
    for (int segmentId = 0; segmentId < 3; segmentId++) {
      String segmentName = _segments.get(segmentId);
      instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      // Segment 0 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 1 (partition 1) should be assigned to instance 1, 4, 7
      // Segment 2 (partition 2) should be assigned to instance 2, 5, 8
      // Following segments are assigned to those instances if continue to use the same instancePartition
      // Segment 3 (partition 3) should be assigned to instance 0, 3, 6
      // Segment 4 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 5 (partition 1) should be assigned to instance 1, 4, 7
      // ...
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int partitionId = segmentId % NUM_PARTITIONS;
        int expectedAssignedInstanceId =
            partitionId % numInstancesPerReplicaGroup + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
      }
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }
    // Use new instancePartition to assign the new segments below.
    Map<InstancePartitionsType, InstancePartitions> newConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _newConsumingInstancePartitions);

    // No existing segments for partition 3, so use the assignment decided by new instancePartition.
    // So segment 3 (partition 3) should be assigned to instance new_0, new_3, new_6
    int segmentId = 3;
    String segmentName = _segments.get(segmentId);
    instancesAssigned =
        segmentAssignment.assignSegment(segmentName, currentAssignment, newConsumingInstancePartitionMap);
    assertEquals(instancesAssigned,
        Arrays.asList("new_consumingInstance_0", "new_consumingInstance_3", "new_consumingInstance_6"));
    addToAssignment(currentAssignment, segmentId, instancesAssigned);

    // Use existing assignment for partition 0/1/2, instead of the one decided by new instancePartition.
    for (segmentId = 4; segmentId < 7; segmentId++) {
      segmentName = _segments.get(segmentId);
      instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, newConsumingInstancePartitionMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);

      // Those segments are assigned according to the assignment from idealState, instead of using new_xxx instances
      // Segment 4 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 5 (partition 1) should be assigned to instance 1, 4, 7
      // Segment 6 (partition 2) should be assigned to instance 2, 5, 8
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int partitionId = segmentId % NUM_PARTITIONS;
        int expectedAssignedInstanceId =
            partitionId % numInstancesPerReplicaGroup + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
      }
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }
  }

  @Test(dataProvider = "tableTypes")
  public void testAssignSegmentWithOfflineSegment(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    int numInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    // Add segments for partition 0/1/2, but add no segment for partition 3.
    List<String> instancesAssigned;
    for (int segmentId = 0; segmentId < 3; segmentId++) {
      String segmentName = _segments.get(segmentId);
      instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      // Segment 0 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 1 (partition 1) should be assigned to instance 1, 4, 7
      // Segment 2 (partition 2) should be assigned to instance 2, 5, 8
      // Following segments are assigned to those instances if continue to use the same instancePartition
      // Segment 3 (partition 3) should be assigned to instance 0, 3, 6
      // Segment 4 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 5 (partition 1) should be assigned to instance 1, 4, 7
      // ...
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int partitionId = segmentId % NUM_PARTITIONS;
        int expectedAssignedInstanceId =
            partitionId % numInstancesPerReplicaGroup + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
      }
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.OFFLINE));
    }
    // Use new instancePartition to assign the new segments below.
    Map<InstancePartitionsType, InstancePartitions> newConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _newConsumingInstancePartitions);

    // No existing segments for partition 3, so use the assignment decided by new instancePartition. All existing
    // segments for partition 0/1/2 are offline, thus skipped, so use the assignment decided by new instancePartition.
    for (int segmentId = 3; segmentId < 7; segmentId++) {
      String segmentName = _segments.get(segmentId);
      instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, newConsumingInstancePartitionMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);

      // Those segments are assigned according to the assignment from idealState, instead of using new_xxx instances
      // Segment 4 (partition 0) should be assigned to instance 0, 3, 6
      // Segment 5 (partition 1) should be assigned to instance 1, 4, 7
      // Segment 6 (partition 2) should be assigned to instance 2, 5, 8
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int partitionId = segmentId % NUM_PARTITIONS;
        int expectedAssignedInstanceId =
            partitionId % numInstancesPerReplicaGroup + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), NEW_CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
      }
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }
  }

  @Test
  public void testRebalanceDedupTableWithTiers() {
    SegmentAssignment segmentAssignment = createSegmentAssignment("dedup");
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    Set<String> segmentsOnTier = new HashSet<>();
    for (int segmentId = 0; segmentId < 6; segmentId++) {
      String segmentName = _segments.get(segmentId);
      if (segmentId < 3) {
        segmentsOnTier.add(segmentName);
      }
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, _instancePartitionsMap);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
    String tierName = "coldTier";
    List<Tier> sortedTiers = createSortedTiers(tierName, segmentsOnTier);
    Map<String, InstancePartitions> tierInstancePartitionsMap = createTierInstancePartitionsMap(tierName, 3);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setIncludeConsuming(true);
    Map<String, Map<String, String>> newAssignment =
        segmentAssignment.rebalanceTable(currentAssignment, onlyConsumingInstancePartitionMap, sortedTiers,
            tierInstancePartitionsMap, rebalanceConfig);
    assertEquals(newAssignment.size(), currentAssignment.size());
    for (String segName : currentAssignment.keySet()) {
      if (segmentsOnTier.contains(segName)) {
        assertTrue(newAssignment.get(segName).keySet().stream().allMatch(s -> s.startsWith(tierName)));
      } else {
        assertTrue(
            newAssignment.get(segName).keySet().stream().allMatch(s -> s.startsWith(CONSUMING_INSTANCE_NAME_PREFIX)));
      }
    }
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Tiers must not be "
      + "specified for table.*")
  public void testRebalanceUpsertTableWithTiers() {
    SegmentAssignment segmentAssignment = createSegmentAssignment("upsert");
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    Set<String> segmentsOnTier = new HashSet<>();
    for (int segmentId = 0; segmentId < 6; segmentId++) {
      String segmentName = _segments.get(segmentId);
      if (segmentId < 3) {
        segmentsOnTier.add(segmentName);
      }
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, _instancePartitionsMap);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    String tierName = "coldTier";
    List<Tier> sortedTiers = createSortedTiers(tierName, segmentsOnTier);
    Map<String, InstancePartitions> tierInstancePartitionsMap = createTierInstancePartitionsMap(tierName, 3);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setIncludeConsuming(true);
    segmentAssignment.rebalanceTable(currentAssignment, onlyConsumingInstancePartitionMap, sortedTiers,
        tierInstancePartitionsMap, rebalanceConfig);
  }

  @Test
  public void testOnlyColdTierSegmentForPartitionFallsBackToComputed() {
    // When the only segment for a partition is on cold tier, getExistingAssignment() returns null after the
    // tier filter skips it, and the assignment falls back to the computed assignment from instance partitions.
    // This covers the case where the bug previously placed a single existing segment on cold tier (or the
    // legitimate case where a partition's only completed segment has aged out to cold).
    // Only dedup tables support multi-tier assignment (MultiTierStrictRealtimeSegmentAssignment).
    SegmentAssignment segmentAssignment = createSegmentAssignmentWithTiers("dedup");
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // Assign segments 0-3 (one per partition) to CONSUMING instances normally
    for (int segmentId = 0; segmentId < NUM_PARTITIONS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }

    // Replace partition 0's only segment with a cold-tier placement
    currentAssignment.put(_segments.get(0), buildColdTierStateMap());

    // Assign a new segment for partition 0 using NEW instance partitions. With the cold-tier segment skipped
    // by the tier filter, getExistingAssignment returns null and the assignment falls back to the computed
    // (new) instance partitions.
    Map<InstancePartitionsType, InstancePartitions> newConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _newConsumingInstancePartitions);
    int newSegmentId = 4;
    String newSegmentName = _segments.get(newSegmentId);
    List<String> instancesAssigned =
        segmentAssignment.assignSegment(newSegmentName, currentAssignment, newConsumingInstancePartitionMap);
    assertEquals(instancesAssigned.size(), NUM_REPLICAS);
    assertEquals(instancesAssigned,
        Arrays.asList("new_consumingInstance_0", "new_consumingInstance_3", "new_consumingInstance_6"));
  }

  @Test
  public void testAssignSegmentPrefersSamePartitionOnConsumingTier() {
    // Verifies that when a mix of cold-tier and consuming-tier segments exist for the same partition,
    // the consuming-tier segment's assignment is used (not the cold-tier one).
    // Only dedup tables support multi-tier assignment (MultiTierStrictRealtimeSegmentAssignment).
    SegmentAssignment segmentAssignment = createSegmentAssignmentWithTiers("dedup");
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    int numInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // Assign segments 0-7 (2 rounds of 4 partitions)
    for (int segmentId = 0; segmentId < 8; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }

    // Move the older segment for partition 0 (segment 0) to cold tier. Segment 4 (also partition 0) stays on
    // consuming tier. The cold-tier segment sorts before the consuming-tier segment in TreeMap.
    currentAssignment.put(_segments.get(0), buildColdTierStateMap());

    // Assign new segment for partition 0 using new instance partitions. The existing consuming-tier
    // assignment (from segment 4) should be used, not the cold-tier one (from segment 0).
    Map<InstancePartitionsType, InstancePartitions> newConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _newConsumingInstancePartitions);
    int newSegmentId = 8;
    String newSegmentName = _segments.get(newSegmentId);
    List<String> instancesAssigned =
        segmentAssignment.assignSegment(newSegmentName, currentAssignment, newConsumingInstancePartitionMap);
    assertEquals(instancesAssigned.size(), NUM_REPLICAS);
    // Should use the existing consuming-tier assignment (original instances), not new or cold-tier
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      int expectedAssignedInstanceId = replicaGroupId * numInstancesPerReplicaGroup;
      assertEquals(instancesAssigned.get(replicaGroupId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
    }
  }

  @Test(dataProvider = "coldTierReplications")
  public void testCascadeColdTierCorruptionSelfHeals(List<String> coldTierServers) {
    // When the bug has already fired, previous segments were assigned to cold-tier servers. Both seg_0 (seq=0)
    // and seg_4 (seq=1) for partition 0 are on cold tier. The fix must break the cascade by returning null
    // (all cold-tier segments skipped) and falling back to the computed assignment.
    // Parameterized over cold-tier replication: covers both fewer-than-NUM_REPLICAS and matching-NUM_REPLICAS.
    SegmentAssignment segmentAssignment = createSegmentAssignmentWithTiers("dedup", coldTierServers);
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // Assign 2 rounds of segments (0-7)
    for (int segmentId = 0; segmentId < 2 * NUM_PARTITIONS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }

    // Simulate cascade corruption: BOTH segments for partition 0 are on cold-tier servers.
    // seg_0 was moved by SegmentRelocator, seg_4 was assigned there by the bug.
    currentAssignment.put(_segments.get(0), buildColdTierStateMap(coldTierServers));
    currentAssignment.put(_segments.get(4), buildColdTierStateMap(coldTierServers));

    // Assign seg_8 (partition 0, seq=2). Both existing p0 segments are cold-tier → all skipped → null →
    // falls back to computed assignment. This breaks the cascade.
    List<String> instancesAssigned = segmentAssignment.assignSegment(
        _segments.get(8), currentAssignment, onlyConsumingInstancePartitionMap);
    assertEquals(instancesAssigned.size(), NUM_REPLICAS);
    int numInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      int expectedAssignedInstanceId = replicaGroupId * numInstancesPerReplicaGroup;
      assertEquals(instancesAssigned.get(replicaGroupId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
    }
  }

  @Test
  public void testIPChangeWithTierConfigPreservesDedupInvariant() {
    // Instance partitions change (new servers) but no cold-tier segments exist. The tier filter must NOT
    // interfere — old consuming servers should still be returned to preserve the dedup invariant.
    SegmentAssignment segmentAssignment = createSegmentAssignmentWithTiers("dedup");
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    int numInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // Assign segments 0-3 (one per partition) with original IPs
    for (int segmentId = 0; segmentId < NUM_PARTITIONS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }

    // No cold-tier moves. Assign seg_4 (partition 0) with NEW instance partitions.
    // Even though getTierInstances() returns the cold-tier server set from ZK,
    // no existing segment is on those servers, so nothing gets filtered.
    // Old consuming assignment from seg_0 should override new IPs.
    Map<InstancePartitionsType, InstancePartitions> newConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _newConsumingInstancePartitions);
    List<String> instancesAssigned =
        segmentAssignment.assignSegment(_segments.get(4), currentAssignment, newConsumingInstancePartitionMap);
    assertEquals(instancesAssigned.size(), NUM_REPLICAS);
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      int expectedAssignedInstanceId = replicaGroupId * numInstancesPerReplicaGroup;
      assertEquals(instancesAssigned.get(replicaGroupId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
    }
  }

  @Test(dataProvider = "coldTierReplications")
  public void testAssignSegmentAfterCommitWithColdTier(List<String> coldTierServers) {
    // Realistic production flow: PinotLLCRealtimeSegmentManager transitions the committing segment to ONLINE
    // before calling assignSegment. This test explicitly simulates that transition.
    // Parameterized over cold-tier replication: covers both fewer-than-NUM_REPLICAS and matching-NUM_REPLICAS.
    SegmentAssignment segmentAssignment = createSegmentAssignmentWithTiers("dedup", coldTierServers);
    Map<InstancePartitionsType, InstancePartitions> onlyConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _instancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    int numInstancesPerReplicaGroup = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // Assign 2 rounds (segments 0-7)
    for (int segmentId = 0; segmentId < 2 * NUM_PARTITIONS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, onlyConsumingInstancePartitionMap);
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }

    // Move seg_0 (p0, seq=0) to cold tier
    currentAssignment.put(_segments.get(0), buildColdTierStateMap(coldTierServers));

    // Simulate PinotLLCRealtimeSegmentManager committing seg_4 (p0, seq=1):
    // it transitions the committing segment from CONSUMING to ONLINE before calling assignSegment.
    String committingSegment = _segments.get(4);
    Map<String, String> committingMap = currentAssignment.get(committingSegment);
    currentAssignment.put(committingSegment,
        SegmentAssignmentUtils.getInstanceStateMap(new ArrayList<>(committingMap.keySet()), SegmentStateModel.ONLINE));

    // Now assign seg_8 (p0, seq=2) with new IPs. State:
    //   seg_0 (seq=0): ONLINE on cold-tier → skipped by tier filter
    //   seg_4 (seq=1): ONLINE on hot-tier  → returned as existing assignment
    // Old hot-tier assignment should override new IPs.
    Map<InstancePartitionsType, InstancePartitions> newConsumingInstancePartitionMap =
        Map.of(InstancePartitionsType.CONSUMING, _newConsumingInstancePartitions);
    List<String> instancesAssigned = segmentAssignment.assignSegment(
        _segments.get(8), currentAssignment, newConsumingInstancePartitionMap);
    assertEquals(instancesAssigned.size(), NUM_REPLICAS);
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      int expectedAssignedInstanceId = replicaGroupId * numInstancesPerReplicaGroup;
      assertEquals(instancesAssigned.get(replicaGroupId), CONSUMING_INSTANCES.get(expectedAssignedInstanceId));
    }
  }

  @Test(dataProvider = "tableTypes")
  public void testAssignSegmentToCompletedServers(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();
    instancePartitionsMap.put(InstancePartitionsType.COMPLETED, new InstancePartitions("completed"));
    try {
      segmentAssignment.assignSegment("seg01", new TreeMap<>(), instancePartitionsMap);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to find CONSUMING instance partitions"), e.getMessage());
    }
    instancePartitionsMap.put(InstancePartitionsType.CONSUMING, new InstancePartitions("consuming"));
    try {
      segmentAssignment.assignSegment("seg01", new TreeMap<>(), instancePartitionsMap);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("One instance partition type should be provided"), e.getMessage());
    }
  }

  @Test(dataProvider = "tableTypes")
  public void testRebalanceTableToCompletedServers(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    String tierName = "coldTier";
    List<Tier> sortedTiers = createSortedTiers(tierName, Collections.emptySet());
    Map<String, InstancePartitions> tierInstancePartitionsMap = createTierInstancePartitionsMap(tierName, 3);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();
    instancePartitionsMap.put(InstancePartitionsType.COMPLETED, new InstancePartitions("completed"));
    try {
      segmentAssignment.rebalanceTable(new TreeMap<>(), instancePartitionsMap, sortedTiers, tierInstancePartitionsMap,
          rebalanceConfig);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to find CONSUMING instance partitions"), e.getMessage());
    }
    instancePartitionsMap.put(InstancePartitionsType.CONSUMING, new InstancePartitions("consuming"));
    try {
      segmentAssignment.rebalanceTable(new TreeMap<>(), instancePartitionsMap, sortedTiers, tierInstancePartitionsMap,
          rebalanceConfig);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("One instance partition type should be provided"), e.getMessage());
    }
  }

  private static Map<String, String> buildColdTierStateMap() {
    return buildColdTierStateMap(COLD_TIER_SERVERS);
  }

  private static Map<String, String> buildColdTierStateMap(List<String> coldTierServers) {
    Map<String, String> map = new HashMap<>();
    for (String server : coldTierServers) {
      map.put(server, SegmentStateModel.ONLINE);
    }
    return map;
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

  private static HelixManager createHelixManager() {
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.get(anyString(), eq(null), eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
      String path = invocation.getArgument(0, String.class);
      String segmentName = path.substring(path.lastIndexOf('/') + 1);
      return new ZNRecord(segmentName);
    });
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    return helixManager;
  }

  private static HelixManager createHelixManagerWithTiers(String tierName, List<String> tierServers) {
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    // Build the tier instance partitions ZK record so getTierInstances() can fetch it
    String tierInstancePartitionsName =
        InstancePartitionsUtils.getInstancePartitionsNameForTier(RAW_TABLE_NAME, tierName);
    String tierInstancePartitionsPath =
        ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(tierInstancePartitionsName);
    InstancePartitions tierInstancePartitions = new InstancePartitions(tierInstancePartitionsName);
    tierInstancePartitions.setInstances(0, 0, tierServers);
    ZNRecord tierZNRecord = tierInstancePartitions.toZNRecord();
    // Single stub that branches by path — order-independent, avoids relying on Mockito's "last stub wins"
    // behavior when multiple matchers could match the same call.
    doAnswer(invocation -> {
      String path = invocation.getArgument(0, String.class);
      if (path.equals(tierInstancePartitionsPath)) {
        return tierZNRecord;
      }
      String lastComponent = path.substring(path.lastIndexOf('/') + 1);
      return new ZNRecord(lastComponent);
    }).when(propertyStore).get(anyString(), eq(null), eq(AccessOption.PERSISTENT));
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    return helixManager;
  }

  private static Map<String, InstancePartitions> createTierInstancePartitionsMap(String tierName, int serverCnt) {
    Map<String, InstancePartitions> instancePartitionsMap = new HashMap<>();
    InstancePartitions instancePartitionsColdTier =
        new InstancePartitions(InstancePartitionsUtils.getInstancePartitionsName(RAW_TABLE_NAME, tierName));
    List<String> serverList = new ArrayList<>();
    for (int i = 0; i < serverCnt; i++) {
      serverList.add(tierName + "_server_" + i);
    }
    instancePartitionsColdTier.setInstances(0, 0, serverList);
    instancePartitionsMap.put(tierName, instancePartitionsColdTier);
    return instancePartitionsMap;
  }

  private static List<Tier> createSortedTiers(String tierName, Set<String> segmentsOnTier) {
    return List.of(new Tier(tierName, new TierSegmentSelector() {
      @Override
      public String getType() {
        return "dummy";
      }

      @Override
      public boolean selectSegment(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
        return segmentsOnTier.contains(segmentZKMetadata.getSegmentName());
      }
    }, new PinotServerTierStorage(tierName, null, null)));
  }
}
