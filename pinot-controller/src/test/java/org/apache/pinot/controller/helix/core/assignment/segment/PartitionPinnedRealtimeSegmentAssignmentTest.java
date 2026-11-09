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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.tier.PinotServerTierStorage;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


/// Tests the partition-pinned assignment of [RealtimeSegmentAssignment]: the segments of a partition follow the
/// instances recorded for the partition in the partition assignment of the ideal state.
@SuppressWarnings("unchecked")
public class PartitionPinnedRealtimeSegmentAssignmentTest {
  private static final int NUM_REPLICAS = 3;
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_SEGMENTS = 24;
  private static final String CONSUMING_INSTANCE_NAME_PREFIX = "consumingInstance_";
  private static final int NUM_CONSUMING_INSTANCES = 9;
  private static final int NUM_INSTANCES_PER_REPLICA_GROUP = NUM_CONSUMING_INSTANCES / NUM_REPLICAS;
  private static final List<String> CONSUMING_INSTANCES =
      SegmentAssignmentTestUtils.getNameList(CONSUMING_INSTANCE_NAME_PREFIX, NUM_CONSUMING_INSTANCES);
  private static final List<String> NEW_CONSUMING_INSTANCES =
      SegmentAssignmentTestUtils.getNameList("new_" + CONSUMING_INSTANCE_NAME_PREFIX, NUM_CONSUMING_INSTANCES);
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = RAW_TABLE_NAME + "_REALTIME";
  private static final String CONSUMING_INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.CONSUMING.getInstancePartitionsName(RAW_TABLE_NAME);
  private static final String TIER_NAME = "coldTier";

  private static List<String> _segments;
  private Map<InstancePartitionsType, InstancePartitions> _consumingInstancePartitionsMap;
  private Map<InstancePartitionsType, InstancePartitions> _newConsumingInstancePartitionsMap;

  @BeforeClass
  public void setUp() {
    _segments = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      _segments.add(new LLCSegmentName(RAW_TABLE_NAME, segmentId % NUM_PARTITIONS, segmentId / NUM_PARTITIONS,
          System.currentTimeMillis()).getSegmentName());
    }
    // CONSUMING instances:
    // {
    //   0_0=[consumingInstance_0, consumingInstance_1, consumingInstance_2],
    //   0_1=[consumingInstance_3, consumingInstance_4, consumingInstance_5],
    //   0_2=[consumingInstance_6, consumingInstance_7, consumingInstance_8]
    // }
    //        p0                   p1                   p2
    //        p3
    _consumingInstancePartitionsMap =
        Map.of(InstancePartitionsType.CONSUMING, createConsumingInstancePartitions(CONSUMING_INSTANCES));
    // New CONSUMING instances:
    // {
    //   0_0=[new_consumingInstance_0, new_consumingInstance_1, new_consumingInstance_2],
    //   0_1=[new_consumingInstance_3, new_consumingInstance_4, new_consumingInstance_5],
    //   0_2=[new_consumingInstance_6, new_consumingInstance_7, new_consumingInstance_8]
    // }
    _newConsumingInstancePartitionsMap =
        Map.of(InstancePartitionsType.CONSUMING, createConsumingInstancePartitions(NEW_CONSUMING_INSTANCES));
  }

  private static InstancePartitions createConsumingInstancePartitions(List<String> instances) {
    InstancePartitions instancePartitions = new InstancePartitions(CONSUMING_INSTANCE_PARTITIONS_NAME);
    int instanceId = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> instancesForReplicaGroup = new ArrayList<>(NUM_INSTANCES_PER_REPLICA_GROUP);
      for (int i = 0; i < NUM_INSTANCES_PER_REPLICA_GROUP; i++) {
        instancesForReplicaGroup.add(instances.get(instanceId++));
      }
      instancePartitions.setInstances(0, replicaGroupId, instancesForReplicaGroup);
    }
    return instancePartitions;
  }

  @DataProvider(name = "tableTypes")
  public Object[] getTableTypes() {
    return new Object[]{"upsert", "dedup", "strictReplicaGroup"};
  }

  private static SegmentAssignment createSegmentAssignment(String tableType) {
    return createSegmentAssignment(createTableConfigBuilder(tableType).build(), createHelixManager());
  }

  private static SegmentAssignment createSegmentAssignment(TableConfig tableConfig, HelixManager helixManager) {
    return SegmentAssignmentFactory.getSegmentAssignment(helixManager, tableConfig);
  }

  private static TableConfigBuilder createTableConfigBuilder(String tableType) {
    TableConfigBuilder builder = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setNumReplicas(NUM_REPLICAS)
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap())
        .setSegmentAssignmentConfigMap(Map.of(InstancePartitionsType.COMPLETED.toString(),
            new SegmentAssignmentConfig(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY,
                PARTITION_COLUMN)))
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, null));
    switch (tableType) {
      case "upsert":
        builder.setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL));
        break;
      case "dedup":
        builder.setDedupConfig(new DedupConfig());
        break;
      default:
        break;
    }
    return builder;
  }

  /// Returns the instances the given partition maps to in the given instance partitions, as a partition assignment
  /// entry (sorted).
  private static List<String> getExpectedInstances(List<String> instances, int partitionId) {
    List<String> expectedInstances = new ArrayList<>(NUM_REPLICAS);
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      int instanceId = partitionId % NUM_INSTANCES_PER_REPLICA_GROUP + replicaGroupId * NUM_INSTANCES_PER_REPLICA_GROUP;
      expectedInstances.add(instances.get(instanceId));
    }
    return PartitionAssignmentUtils.toInstanceList(expectedInstances);
  }

  @Test(dataProvider = "tableTypes")
  public void testAssignSegment(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();

    // Add segments for partition 0/1/2, but add no segment for partition 3. Each partition is added to the partition
    // assignment with the instances from the instance partitions.
    for (int segmentId = 0; segmentId < 3; segmentId++) {
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(_segments.get(segmentId), idealState, _consumingInstancePartitionsMap);
      assertEquals(instancesAssigned, getExpectedInstances(CONSUMING_INSTANCES, segmentId));
      assertEquals(PartitionAssignmentUtils.getInstances(idealState, segmentId), instancesAssigned);
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState).keySet(), Set.of(0, 1, 2));

    // Partition 3 has no entry, so it is placed by the new instance partitions
    List<String> instancesAssigned =
        segmentAssignment.assignSegment(_segments.get(3), idealState, _newConsumingInstancePartitionsMap);
    assertEquals(instancesAssigned, getExpectedInstances(NEW_CONSUMING_INSTANCES, 3));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 3), instancesAssigned);
    addToAssignment(currentAssignment, 3, instancesAssigned);

    // Partition 0/1/2 keep their instances although the instance partitions changed
    for (int segmentId = 4; segmentId < 7; segmentId++) {
      instancesAssigned =
          segmentAssignment.assignSegment(_segments.get(segmentId), idealState, _newConsumingInstancePartitionsMap);
      assertEquals(instancesAssigned, getExpectedInstances(CONSUMING_INSTANCES, segmentId % NUM_PARTITIONS));
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState).keySet(), Set.of(0, 1, 2, 3));
  }

  /// A table created before the partition assignment was introduced has segments but no entry: the entry of a
  /// partition is created from its CONSUMING segment, else from its latest LLC segment, skipping the OFFLINE segments.
  @Test(dataProvider = "tableTypes")
  public void testAssignSegmentWithExistingSegments(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    // Partition 0: ONLINE segment (sequence 0) on the old instances, CONSUMING segment (sequence 1) on the new ones
    currentAssignment.put(_segments.get(0),
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(CONSUMING_INSTANCES, 0),
            SegmentStateModel.ONLINE));
    currentAssignment.put(_segments.get(4),
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(NEW_CONSUMING_INSTANCES, 0),
            SegmentStateModel.CONSUMING));
    // Partition 1: ONLINE segments only, the latest one (sequence 1) on the new instances
    currentAssignment.put(_segments.get(1),
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(CONSUMING_INSTANCES, 1),
            SegmentStateModel.ONLINE));
    currentAssignment.put(_segments.get(5),
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(NEW_CONSUMING_INSTANCES, 1),
            SegmentStateModel.ONLINE));
    // Partition 2: OFFLINE segment only, which is skipped
    currentAssignment.put(_segments.get(2),
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(CONSUMING_INSTANCES, 2),
            SegmentStateModel.OFFLINE));

    for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(_segments.get(8 + partitionId), idealState, _consumingInstancePartitionsMap);
      List<String> expectedInstances =
          getExpectedInstances(partitionId < 2 ? NEW_CONSUMING_INSTANCES : CONSUMING_INSTANCES, partitionId);
      assertEquals(instancesAssigned, expectedInstances);
      assertEquals(PartitionAssignmentUtils.getInstances(idealState, partitionId), instancesAssigned);
    }
  }

  /// The partition id of an uploaded segment with a custom name is read from its ZK metadata, both to assign it and
  /// to create the entry of its partition from it.
  @Test(dataProvider = "tableTypes")
  public void testAssignUploadedSegment(String tableType) {
    String uploadedSegment = "uploadedSegment";
    SegmentAssignment segmentAssignment = createSegmentAssignment(createTableConfigBuilder(tableType).build(),
        createHelixManager(Set.of(), Map.of(uploadedSegment, 1)));
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();

    List<String> instancesAssigned =
        segmentAssignment.assignSegment(uploadedSegment, idealState, _newConsumingInstancePartitionsMap);
    assertEquals(instancesAssigned, getExpectedInstances(NEW_CONSUMING_INSTANCES, 1));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 1), instancesAssigned);

    // Without entry, the uploaded segment is found from its ZK metadata, and an uploaded segment without ZK metadata
    // is skipped
    PartitionAssignmentUtils.setPartitionAssignment(idealState, Map.of());
    currentAssignment.put(uploadedSegment,
        SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    currentAssignment.put("orphanSegment",
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(CONSUMING_INSTANCES, 2),
            SegmentStateModel.ONLINE));
    instancesAssigned = segmentAssignment.assignSegment(_segments.get(1), idealState, _consumingInstancePartitionsMap);
    assertEquals(instancesAssigned, getExpectedInstances(NEW_CONSUMING_INSTANCES, 1));

    // A segment without partition id is rejected for an upsert/dedup table, and assigned with a partition id derived
    // from its name otherwise, without entry
    String segmentWithoutPartition = "segmentWithoutPartition";
    if (tableType.equals("strictReplicaGroup")) {
      instancesAssigned =
          segmentAssignment.assignSegment(segmentWithoutPartition, idealState, _consumingInstancePartitionsMap);
      assertEquals(instancesAssigned,
          getExpectedInstances(CONSUMING_INSTANCES, SegmentUtils.getDefaultPartitionId(segmentWithoutPartition)));
      assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState).keySet(), Set.of(1));
    } else {
      assertThrows(IllegalStateException.class,
          () -> segmentAssignment.assignSegment(segmentWithoutPartition, idealState,
              _consumingInstancePartitionsMap));
    }
  }

  /// A failed read of the ZK metadata of the uploaded segments of a partition without entry fails the assignment,
  /// instead of taking them as segments without partition id and creating the entry on other instances than theirs.
  /// The failure is not cached, so the next assignment reads the ZK metadata again.
  @Test
  public void testAssignSegmentWithFailedMetadataRead() {
    String uploadedSegment = "uploadedSegment";
    Map<String, Integer> segmentPartitionIds = Map.of(uploadedSegment, 1);
    HelixManager helixManager = createHelixManager(Set.of(), segmentPartitionIds);
    when(helixManager.getHelixPropertyStore().get(anyList(), any(), eq(AccessOption.PERSISTENT), eq(true)))
        .thenThrow(new HelixMetaDataAccessException("Failed to read segment ZK metadata"))
        .thenAnswer(invocation -> createSegmentZNRecords(invocation.getArgument(0), Set.of(), segmentPartitionIds));
    SegmentAssignment segmentAssignment =
        createSegmentAssignment(createTableConfigBuilder("upsert").build(), helixManager);
    // The uploaded segment of partition 1 is on the current instances of the partition, which has no entry
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.getRecord().getMapFields().put(uploadedSegment,
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(CONSUMING_INSTANCES, 1),
            SegmentStateModel.ONLINE));

    expectThrows(HelixMetaDataAccessException.class,
        () -> segmentAssignment.assignSegment(_segments.get(1), idealState, _newConsumingInstancePartitionsMap));
    assertNull(PartitionAssignmentUtils.getInstances(idealState, 1));

    List<String> instancesAssigned =
        segmentAssignment.assignSegment(_segments.get(1), idealState, _newConsumingInstancePartitionsMap);
    assertEquals(instancesAssigned, getExpectedInstances(CONSUMING_INSTANCES, 1));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 1), instancesAssigned);
  }

  /// The latest LLC segment of a partition without CONSUMING segment can have been moved to a tier, in which case its
  /// instances are not the CONSUMING instances of the partition. The tier is read from the segment ZK metadata even
  /// when the table has no tier config (anymore), because removing the tier config does not move the segment back.
  @Test
  public void testAssignSegmentWithLatestSegmentOnTier() {
    String tieredSegment = _segments.get(4);
    SegmentAssignment segmentAssignment = createSegmentAssignment(createTableConfigBuilder("dedup").build(),
        createHelixManager(Set.of(tieredSegment), Map.of()));
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    currentAssignment.put(_segments.get(0),
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(CONSUMING_INSTANCES, 0),
            SegmentStateModel.ONLINE));
    currentAssignment.put(tieredSegment, SegmentAssignmentUtils.getInstanceStateMap(
        List.of("coldTier_server_0", "coldTier_server_1", "coldTier_server_2"), SegmentStateModel.ONLINE));

    List<String> instancesAssigned =
        segmentAssignment.assignSegment(_segments.get(8), idealState, _newConsumingInstancePartitionsMap);
    assertEquals(instancesAssigned, getExpectedInstances(NEW_CONSUMING_INSTANCES, 0));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 0), instancesAssigned);
  }

  /// With completed segments relocated to the COMPLETED instances, the completed segments of a partition without
  /// CONSUMING segment are not on the CONSUMING instances of the partition, so its entry is created from the instance
  /// partitions.
  @Test
  public void testAssignSegmentWithRelocatedCompletedSegments() {
    TableConfig tableConfig = createTableConfigBuilder("strictReplicaGroup")
        .setTagOverrideConfig(new TagOverrideConfig(null, "completed_REALTIME"))
        .build();
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableConfig, createHelixManager());
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    // Partition 0: completed segment relocated to the COMPLETED instances
    currentAssignment.put(_segments.get(0), SegmentAssignmentUtils.getInstanceStateMap(
        List.of("completedInstance_0", "completedInstance_1", "completedInstance_2"), SegmentStateModel.ONLINE));
    // Partition 1: CONSUMING segment on the old instances
    currentAssignment.put(_segments.get(1),
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(CONSUMING_INSTANCES, 1),
            SegmentStateModel.CONSUMING));

    List<String> instancesAssigned =
        segmentAssignment.assignSegment(_segments.get(4), idealState, _newConsumingInstancePartitionsMap);
    assertEquals(instancesAssigned, getExpectedInstances(NEW_CONSUMING_INSTANCES, 0));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 0), instancesAssigned);
    instancesAssigned =
        segmentAssignment.assignSegment(_segments.get(5), idealState, _newConsumingInstancePartitionsMap);
    assertEquals(instancesAssigned, getExpectedInstances(CONSUMING_INSTANCES, 1));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 1), instancesAssigned);
  }

  /// A rebalance in low disk mode without minimum available replicas empties the entry of a partition while it drops
  /// the partition from its current instances. A segment assigned meanwhile is placed as if the partition had no entry:
  /// from the instance partitions, since the segments of the partition are not hosted anywhere.
  @Test(dataProvider = "tableTypes")
  public void testAssignSegmentWithEmptyEntry(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    idealState.getRecord().getMapFields().put(_segments.get(0), Map.of());
    PartitionAssignmentUtils.setInstances(idealState, 0, List.of());

    List<String> instancesAssigned =
        segmentAssignment.assignSegment(_segments.get(4), idealState, _newConsumingInstancePartitionsMap);
    assertEquals(instancesAssigned, getExpectedInstances(NEW_CONSUMING_INSTANCES, 0));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 0), instancesAssigned);
  }

  @Test(dataProvider = "tableTypes")
  public void testRebalanceTable(String tableType) {
    HelixManager helixManager = createHelixManager();
    SegmentAssignment segmentAssignment =
        createSegmentAssignment(createTableConfigBuilder(tableType).build(), helixManager);
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    for (int segmentId = 0; segmentId < 8; segmentId++) {
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(_segments.get(segmentId), idealState, _consumingInstancePartitionsMap);
      addToAssignment(currentAssignment, segmentId, instancesAssigned);
    }
    // Partition 3 has an entry but all its segments are OFFLINE
    for (int segmentId = 3; segmentId < 8; segmentId += NUM_PARTITIONS) {
      String segmentName = _segments.get(segmentId);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(currentAssignment.get(segmentName).keySet(),
              SegmentStateModel.OFFLINE));
    }
    // Partition 4 has an entry but no segment
    PartitionAssignmentUtils.setInstances(idealState, 4, getExpectedInstances(CONSUMING_INSTANCES, 4));

    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setIncludeConsuming(true);
    TableAssignment tableAssignment =
        segmentAssignment.rebalanceTable(idealState, _newConsumingInstancePartitionsMap, null, null, rebalanceConfig);

    // All partitions with segments are moved to the new instances, including partition 3 whose segments are OFFLINE.
    // The entry of partition 4, which has no segment, is dropped.
    Map<Integer, List<String>> partitionAssignment = tableAssignment.getPartitionAssignment();
    assertEquals(partitionAssignment.keySet(), Set.of(0, 1, 2, 3));
    for (Map.Entry<Integer, List<String>> entry : partitionAssignment.entrySet()) {
      assertEquals(entry.getValue(), getExpectedInstances(NEW_CONSUMING_INSTANCES, entry.getKey()));
    }
    // COMPLETED and CONSUMING segments follow their partition, OFFLINE segments are not moved
    Map<String, Map<String, String>> newAssignment = tableAssignment.getSegmentAssignment();
    assertEquals(newAssignment.size(), 8);
    for (int segmentId = 0; segmentId < 8; segmentId++) {
      String segmentName = _segments.get(segmentId);
      int partitionId = segmentId % NUM_PARTITIONS;
      Map<String, String> instanceStateMap = newAssignment.get(segmentName);
      if (partitionId == 3) {
        assertEquals(instanceStateMap, currentAssignment.get(segmentName));
      } else {
        assertEquals(instanceStateMap, SegmentAssignmentUtils.getInstanceStateMap(partitionAssignment.get(partitionId),
            segmentId < NUM_PARTITIONS ? SegmentStateModel.ONLINE : SegmentStateModel.CONSUMING));
      }
    }

    // Rebalancing again with the same instance partitions does not change anything
    PartitionAssignmentUtils.setPartitionAssignment(idealState, partitionAssignment);
    idealState.getRecord().setMapFields(newAssignment);
    tableAssignment =
        segmentAssignment.rebalanceTable(idealState, _newConsumingInstancePartitionsMap, null, null, rebalanceConfig);
    assertEquals(tableAssignment.getSegmentAssignment(), newAssignment);
    assertEquals(tableAssignment.getPartitionAssignment(), partitionAssignment);

    // The partition ids are all in the segment names, so no segment ZK metadata is read for them
    verify(helixManager.getHelixPropertyStore(), never()).get(anyList(), any(), anyInt(), anyBoolean());
  }

  /// The partition ids of the segments without partition id in their name are read from their ZK metadata, and kept
  /// when the target assignment is recalculated with the same segment assignment, which reads only the segments added
  /// since: a segment uploaded after the first read, and a segment whose ZK metadata was left by a failed upload and
  /// replaced by the corrected upload with another partition.
  @Test
  public void testRebalanceTableWithSegmentsAddedAfterFirstRead() {
    String uploadedSegment0 = "uploadedSegment0";
    String uploadedSegment1 = "uploadedSegment1";
    String reuploadedSegment = "reuploadedSegment";
    // The ZK metadata of the re-uploaded segment is left in partition 0 by a failed upload
    Map<String, Integer> segmentPartitionIds = new HashMap<>(Map.of(uploadedSegment0, 0, reuploadedSegment, 0));
    SegmentAssignment segmentAssignment = createSegmentAssignment(createTableConfigBuilder("upsert").build(),
        createHelixManager(Set.of(), segmentPartitionIds));
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    currentAssignment.put(uploadedSegment0,
        SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(CONSUMING_INSTANCES, 0),
            SegmentStateModel.ONLINE));
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setIncludeConsuming(true);
    TableAssignment tableAssignment =
        segmentAssignment.rebalanceTable(idealState, _newConsumingInstancePartitionsMap, null, null, rebalanceConfig);
    assertEquals(tableAssignment.getSegmentPartitionIds(), Map.of(uploadedSegment0, 0));

    // The segment uploaded after the bulk read, and the corrected upload of the other segment, are placed with their
    // partition
    segmentPartitionIds.put(uploadedSegment1, 1);
    segmentPartitionIds.put(reuploadedSegment, 1);
    for (String segment : List.of(uploadedSegment1, reuploadedSegment)) {
      currentAssignment.put(segment,
          SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(CONSUMING_INSTANCES, 1),
              SegmentStateModel.ONLINE));
    }
    tableAssignment =
        segmentAssignment.rebalanceTable(idealState, _newConsumingInstancePartitionsMap, null, null, rebalanceConfig);
    assertEquals(tableAssignment.getSegmentPartitionIds(),
        Map.of(uploadedSegment0, 0, uploadedSegment1, 1, reuploadedSegment, 1));
    for (String segment : List.of(uploadedSegment1, reuploadedSegment)) {
      assertEquals(tableAssignment.getSegmentAssignment().get(segment),
          SegmentAssignmentUtils.getInstanceStateMap(getExpectedInstances(NEW_CONSUMING_INSTANCES, 1),
              SegmentStateModel.ONLINE));
    }
  }

  @Test
  public void testRebalanceDedupTableWithTiers() {
    SegmentAssignment segmentAssignment = createSegmentAssignment("dedup");
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    Set<String> segmentsOnTier = new HashSet<>();
    for (int segmentId = 0; segmentId < 6; segmentId++) {
      String segmentName = _segments.get(segmentId);
      if (segmentId < 3) {
        segmentsOnTier.add(segmentName);
      }
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, idealState, _consumingInstancePartitionsMap);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
    List<Tier> sortedTiers = createSortedTiers(segmentsOnTier);
    Map<String, InstancePartitions> tierInstancePartitionsMap = createTierInstancePartitionsMap(3);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setIncludeConsuming(true);
    TableAssignment tableAssignment =
        segmentAssignment.rebalanceTable(idealState, _consumingInstancePartitionsMap, sortedTiers,
            tierInstancePartitionsMap, rebalanceConfig);
    Map<String, Map<String, String>> newAssignment = tableAssignment.getSegmentAssignment();
    assertEquals(newAssignment.size(), currentAssignment.size());
    for (String segName : currentAssignment.keySet()) {
      if (segmentsOnTier.contains(segName)) {
        assertTrue(newAssignment.get(segName).keySet().stream().allMatch(s -> s.startsWith(TIER_NAME)));
      } else {
        assertTrue(
            newAssignment.get(segName).keySet().stream().allMatch(s -> s.startsWith(CONSUMING_INSTANCE_NAME_PREFIX)));
      }
    }
    // The partitions with non-tier segments keep their CONSUMING instances, and the entry of partition 2, whose only
    // segment is on the tier, is dropped
    assertEquals(tableAssignment.getPartitionAssignment(), Map.of(
        0, getExpectedInstances(CONSUMING_INSTANCES, 0),
        1, getExpectedInstances(CONSUMING_INSTANCES, 1),
        3, getExpectedInstances(CONSUMING_INSTANCES, 3)
    ));
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Tiers must not be "
      + "specified for upsert table.*")
  public void testRebalanceUpsertTableWithTiers() {
    SegmentAssignment segmentAssignment = createSegmentAssignment("upsert");
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    Set<String> segmentsOnTier = new HashSet<>();
    for (int segmentId = 0; segmentId < 6; segmentId++) {
      String segmentName = _segments.get(segmentId);
      if (segmentId < 3) {
        segmentsOnTier.add(segmentName);
      }
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, idealState, _consumingInstancePartitionsMap);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
    List<Tier> sortedTiers = createSortedTiers(segmentsOnTier);
    Map<String, InstancePartitions> tierInstancePartitionsMap = createTierInstancePartitionsMap(3);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setIncludeConsuming(true);
    segmentAssignment.rebalanceTable(idealState, _consumingInstancePartitionsMap, sortedTiers,
        tierInstancePartitionsMap, rebalanceConfig);
  }

  @DataProvider(name = "upsertDedupTableTypes")
  public Object[] getUpsertDedupTableTypes() {
    return new Object[]{"upsert", "dedup"};
  }

  /// Upsert and dedup tables must assign and rebalance their segments with the CONSUMING instance partitions only.
  @Test(dataProvider = "upsertDedupTableTypes")
  public void testCompletedInstancePartitionsRejected(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();
    instancePartitionsMap.put(InstancePartitionsType.COMPLETED, new InstancePartitions("completed"));
    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> segmentAssignment.assignSegment("seg01", idealState, instancePartitionsMap));
    assertTrue(e.getMessage().contains("Failed to find CONSUMING instance partitions"), e.getMessage());
    e = expectThrows(IllegalStateException.class,
        () -> segmentAssignment.rebalanceTable(idealState, instancePartitionsMap, null, null,
            new RebalanceConfig()));
    assertTrue(e.getMessage().contains("Failed to find CONSUMING instance partitions"), e.getMessage());

    instancePartitionsMap.put(InstancePartitionsType.CONSUMING, new InstancePartitions("consuming"));
    e = expectThrows(IllegalStateException.class,
        () -> segmentAssignment.assignSegment("seg01", idealState, instancePartitionsMap));
    assertTrue(e.getMessage().contains("One instance partition type should be provided"), e.getMessage());
    e = expectThrows(IllegalStateException.class,
        () -> segmentAssignment.rebalanceTable(idealState, instancePartitionsMap, null, null,
            new RebalanceConfig()));
    assertTrue(e.getMessage().contains("COMPLETED instance partitions must not be provided"), e.getMessage());
  }

  /// Other tables can relocate uploaded segments to the COMPLETED instance partitions, which does not pin them to
  /// their partition.
  @Test
  public void testAssignUploadedSegmentToCompletedServers() {
    SegmentAssignment segmentAssignment = createSegmentAssignment("strictReplicaGroup");
    IdealState idealState = new IdealState(REALTIME_TABLE_NAME);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = Map.of(InstancePartitionsType.COMPLETED,
        _consumingInstancePartitionsMap.get(InstancePartitionsType.CONSUMING));
    List<String> instancesAssigned = segmentAssignment.assignSegment("seg01", idealState, instancePartitionsMap);
    assertEquals(instancesAssigned.size(), NUM_REPLICAS);
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState), Map.of());
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
    return createHelixManager(Set.of(), Map.of());
  }

  /// Creates a Helix manager whose property store returns, for each segment, a ZK metadata on the tier for the given
  /// tiered segments, and with the given partition id for the segments in the given map.
  private static HelixManager createHelixManager(Set<String> tieredSegments, Map<String, Integer> segmentPartitionIds) {
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.get(anyString(), eq(null), eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
      String path = invocation.getArgument(0, String.class);
      String segmentName = path.substring(path.lastIndexOf('/') + 1);
      return createSegmentZNRecord(segmentName, tieredSegments, segmentPartitionIds);
    });
    // Batched read of the ZK metadata of the given segments, used to resolve the partition id of the segments without
    // partition id in their name
    when(propertyStore.get(anyList(), any(), eq(AccessOption.PERSISTENT), eq(true))).thenAnswer(
        invocation -> createSegmentZNRecords(invocation.getArgument(0), tieredSegments, segmentPartitionIds));
    // Bulk read of all segment ZK metadata, used to resolve the eligible tier of each segment once per rebalance
    List<ZNRecord> segmentZNRecords = new ArrayList<>(_segments.size() + segmentPartitionIds.size());
    for (String segmentName : _segments) {
      segmentZNRecords.add(createSegmentZNRecord(segmentName, tieredSegments, segmentPartitionIds));
    }
    for (String segmentName : segmentPartitionIds.keySet()) {
      segmentZNRecords.add(createSegmentZNRecord(segmentName, tieredSegments, segmentPartitionIds));
    }
    when(propertyStore.getChildren(anyString(), eq(null), eq(AccessOption.PERSISTENT), anyInt(), anyInt())).thenReturn(
        segmentZNRecords);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    return helixManager;
  }

  private static List<ZNRecord> createSegmentZNRecords(List<String> paths, Set<String> tieredSegments,
      Map<String, Integer> segmentPartitionIds) {
    List<ZNRecord> records = new ArrayList<>(paths.size());
    for (String path : paths) {
      records.add(
          createSegmentZNRecord(path.substring(path.lastIndexOf('/') + 1), tieredSegments, segmentPartitionIds));
    }
    return records;
  }

  private static ZNRecord createSegmentZNRecord(String segmentName, Set<String> tieredSegments,
      Map<String, Integer> segmentPartitionIds) {
    ZNRecord record = new ZNRecord(segmentName);
    if (tieredSegments.contains(segmentName)) {
      record.setSimpleField(Segment.TIER, TIER_NAME);
    }
    Integer partitionId = segmentPartitionIds.get(segmentName);
    if (partitionId != null) {
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(record);
      segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Map.of(PARTITION_COLUMN,
          new ColumnPartitionMetadata("murmur", NUM_PARTITIONS, Set.of(partitionId), null))));
    }
    return record;
  }

  private static Map<String, InstancePartitions> createTierInstancePartitionsMap(int serverCnt) {
    Map<String, InstancePartitions> instancePartitionsMap = new HashMap<>();
    InstancePartitions instancePartitionsColdTier =
        new InstancePartitions(InstancePartitionsUtils.getInstancePartitionsName(RAW_TABLE_NAME, TIER_NAME));
    List<String> serverList = new ArrayList<>();
    for (int i = 0; i < serverCnt; i++) {
      serverList.add(TIER_NAME + "_server_" + i);
    }
    instancePartitionsColdTier.setInstances(0, 0, serverList);
    instancePartitionsMap.put(TIER_NAME, instancePartitionsColdTier);
    return instancePartitionsMap;
  }

  private static List<Tier> createSortedTiers(Set<String> segmentsOnTier) {
    return List.of(new Tier(TIER_NAME, new TierSegmentSelector() {
      @Override
      public String getType() {
        return "dummy";
      }

      @Override
      public boolean selectSegment(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
        return segmentsOnTier.contains(segmentZKMetadata.getSegmentName());
      }
    }, new PinotServerTierStorage(TIER_NAME, null, null)));
  }
}
