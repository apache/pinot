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
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests the partition-pinned assignment of [OfflineSegmentAssignment]: with one instance per partition within each
/// replica-group, the segments of a partition follow the instances recorded for the partition in the partition
/// assignment of the ideal state.
@SuppressWarnings("unchecked")
public class PartitionPinnedOfflineSegmentAssignmentTest {
  private static final int NUM_REPLICAS = 2;
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final int NUM_PARTITIONS = 4;
  private static final int NUM_SEGMENTS = 8;
  private static final String SEGMENT_WITHOUT_PARTITION = "segmentWithoutPartition";
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = NUM_REPLICAS * NUM_PARTITIONS;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final List<String> NEW_INSTANCES =
      SegmentAssignmentTestUtils.getNameList("new_" + INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = RAW_TABLE_NAME + "_OFFLINE";
  private static final String INSTANCE_PARTITIONS_NAME =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME);

  private List<String> _segments;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMap;
  private Map<InstancePartitionsType, InstancePartitions> _newInstancePartitionsMap;

  @BeforeClass
  public void setUp() {
    _segments = SegmentAssignmentTestUtils.getNameList("segment_", NUM_SEGMENTS);
    // Instances, one per partition within each replica-group:
    // {
    //   0_0=[instance_0], 1_0=[instance_1], 2_0=[instance_2], 3_0=[instance_3],
    //   0_1=[instance_4], 1_1=[instance_5], 2_1=[instance_6], 3_1=[instance_7]
    // }
    _instancePartitionsMap = Map.of(InstancePartitionsType.OFFLINE, createInstancePartitions(INSTANCES, 1));
    _newInstancePartitionsMap = Map.of(InstancePartitionsType.OFFLINE, createInstancePartitions(NEW_INSTANCES, 1));
  }

  private static InstancePartitions createInstancePartitions(List<String> instances, int numInstancesPerPartition) {
    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    int numPartitions = NUM_PARTITIONS / numInstancesPerPartition;
    int instanceId = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
        List<String> instancesForPartition = new ArrayList<>(numInstancesPerPartition);
        for (int i = 0; i < numInstancesPerPartition; i++) {
          instancesForPartition.add(instances.get(instanceId++));
        }
        instancePartitions.setInstances(partitionId, replicaGroupId, instancesForPartition);
      }
    }
    return instancePartitions;
  }

  @DataProvider(name = "tableTypes")
  public Object[] getTableTypes() {
    return new Object[]{"upsert", "strictReplicaGroup"};
  }

  private SegmentAssignment createSegmentAssignment(String tableType) {
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNumReplicas(NUM_REPLICAS)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Map.of(PARTITION_COLUMN, new ColumnPartitionConfig("murmur", NUM_PARTITIONS))))
        .setSegmentAssignmentConfigMap(Map.of(InstancePartitionsType.OFFLINE.toString(),
            new SegmentAssignmentConfig(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY,
                PARTITION_COLUMN)))
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, null));
    if (tableType.equals("upsert")) {
      builder.setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL));
    }
    return SegmentAssignmentFactory.getSegmentAssignment(createHelixManager(), builder.build());
  }

  /// Returns the instances the given partition maps to in the given instances, one per replica-group.
  private static List<String> getExpectedInstances(List<String> instances, int partitionId) {
    List<String> expectedInstances = new ArrayList<>(NUM_REPLICAS);
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      expectedInstances.add(instances.get(partitionId + replicaGroupId * NUM_PARTITIONS));
    }
    return expectedInstances;
  }

  @Test(dataProvider = "tableTypes")
  public void testAssignSegment(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();

    // Each partition is added to the partition assignment with the instances from the instance partitions
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      int partitionId = segmentId % NUM_PARTITIONS;
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, idealState, _instancePartitionsMap);
      assertEquals(instancesAssigned, getExpectedInstances(INSTANCES, partitionId));
      assertEquals(PartitionAssignmentUtils.getInstances(idealState, partitionId), instancesAssigned);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState).keySet(), Set.of(0, 1, 2, 3));

    // The partitions keep their instances although the instance partitions changed
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(_segments.get(segmentId), idealState, _newInstancePartitionsMap);
      assertEquals(instancesAssigned, getExpectedInstances(INSTANCES, segmentId % NUM_PARTITIONS));
    }

    // A segment without partition id is assigned with the segment assignment strategy and does not add a partition
    List<String> instancesAssigned =
        segmentAssignment.assignSegment(SEGMENT_WITHOUT_PARTITION, idealState, _newInstancePartitionsMap);
    assertEquals(instancesAssigned.size(), NUM_REPLICAS);
    assertTrue(instancesAssigned.stream().allMatch(NEW_INSTANCES::contains));
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState).keySet(), Set.of(0, 1, 2, 3));
  }

  /// With more than one instance per partition within a replica-group, the segments of a partition can be spread
  /// over the instances of the partition, so they are not pinned.
  @Test(dataProvider = "tableTypes")
  public void testAssignSegmentWithMultipleInstancesPerPartition(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Map.of(InstancePartitionsType.OFFLINE, createInstancePartitions(INSTANCES, 2));
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(_segments.get(segmentId), idealState, instancePartitionsMap);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);
      idealState.getRecord().getMapFields().put(_segments.get(segmentId),
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState), Map.of());
    TableAssignment tableAssignment =
        segmentAssignment.rebalanceTable(idealState, instancePartitionsMap, null, null, new RebalanceConfig());
    assertEquals(tableAssignment.getPartitionAssignment(), Map.of());
    assertEquals(tableAssignment.getSegmentAssignment(), idealState.getRecord().getMapFields());
  }

  @Test(dataProvider = "tableTypes")
  public void testRebalanceTable(String tableType) {
    SegmentAssignment segmentAssignment = createSegmentAssignment(tableType);
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = _segments.get(segmentId);
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, idealState, _instancePartitionsMap);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
    List<String> instancesAssigned =
        segmentAssignment.assignSegment(SEGMENT_WITHOUT_PARTITION, idealState, _instancePartitionsMap);
    currentAssignment.put(SEGMENT_WITHOUT_PARTITION,
        SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    // Partition 4 has an entry but no segment
    PartitionAssignmentUtils.setInstances(idealState, 4, getExpectedInstances(INSTANCES, 0));

    // All partitions with segments are moved to the new instances, and the segments follow their partition. The entry
    // of partition 4, which has no segment, is dropped.
    TableAssignment tableAssignment =
        segmentAssignment.rebalanceTable(idealState, _newInstancePartitionsMap, null, null, new RebalanceConfig());
    // The partition ids of the segments are resolved from their ZK metadata for the batching of the rebalance
    Map<String, Integer> expectedSegmentPartitionIds = new HashMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      expectedSegmentPartitionIds.put(_segments.get(segmentId), segmentId % NUM_PARTITIONS);
    }
    expectedSegmentPartitionIds.put(SEGMENT_WITHOUT_PARTITION, BaseSegmentAssignment.NO_PARTITION_ID);
    assertEquals(tableAssignment.getSegmentPartitionIds(), expectedSegmentPartitionIds);
    Map<Integer, List<String>> partitionAssignment = tableAssignment.getPartitionAssignment();
    assertEquals(partitionAssignment.keySet(), Set.of(0, 1, 2, 3));
    for (Map.Entry<Integer, List<String>> entry : partitionAssignment.entrySet()) {
      assertEquals(entry.getValue(), getExpectedInstances(NEW_INSTANCES, entry.getKey() % NUM_PARTITIONS));
    }
    Map<String, Map<String, String>> newAssignment = tableAssignment.getSegmentAssignment();
    assertEquals(newAssignment.size(), NUM_SEGMENTS + 1);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      assertEquals(newAssignment.get(_segments.get(segmentId)), SegmentAssignmentUtils.getInstanceStateMap(
          partitionAssignment.get(segmentId % NUM_PARTITIONS), SegmentStateModel.ONLINE));
    }
    // The segment without partition id is reassigned with the segment assignment strategy
    Map<String, String> instanceStateMap = newAssignment.get(SEGMENT_WITHOUT_PARTITION);
    assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    assertTrue(instanceStateMap.keySet().stream().allMatch(NEW_INSTANCES::contains));

    // Rebalancing again with the same instance partitions does not change anything
    PartitionAssignmentUtils.setPartitionAssignment(idealState, partitionAssignment);
    idealState.getRecord().setMapFields(newAssignment);
    tableAssignment =
        segmentAssignment.rebalanceTable(idealState, _newInstancePartitionsMap, null, null, new RebalanceConfig());
    assertEquals(tableAssignment.getSegmentAssignment(), newAssignment);
    assertEquals(tableAssignment.getPartitionAssignment(), partitionAssignment);
  }

  @Test
  public void testNotPinnedWithoutStrictReplicaGroupRouting() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNumReplicas(NUM_REPLICAS)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Map.of(PARTITION_COLUMN, new ColumnPartitionConfig("murmur", NUM_PARTITIONS))))
        .setSegmentAssignmentConfigMap(Map.of(InstancePartitionsType.OFFLINE.toString(),
            new SegmentAssignmentConfig(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY,
                PARTITION_COLUMN)))
        .build();
    SegmentAssignment segmentAssignment =
        SegmentAssignmentFactory.getSegmentAssignment(createHelixManager(), tableConfig);
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    List<String> instancesAssigned =
        segmentAssignment.assignSegment(_segments.get(1), idealState, _instancePartitionsMap);
    assertEquals(instancesAssigned, getExpectedInstances(INSTANCES, 1));
    assertNull(PartitionAssignmentUtils.getInstances(idealState, 1));
  }

  /// The segments of a dimension table are assigned to all servers of the tenant, so they are not pinned although
  /// the OFFLINE instance partitions hold one instance per partition.
  @Test
  public void testNotPinnedForDimensionTable() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNumReplicas(1)
        .setIsDimTable(true)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Map.of(PARTITION_COLUMN, new ColumnPartitionConfig("murmur", NUM_PARTITIONS))))
        .setSegmentAssignmentConfigMap(Map.of(InstancePartitionsType.OFFLINE.toString(),
            new SegmentAssignmentConfig(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY,
                PARTITION_COLUMN)))
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, null))
        .build();
    // The tenant has an OFFLINE server, which the instance partitions hold, and a REALTIME server
    String offlineServer = "offlineServer";
    String realtimeServer = "realtimeServer";
    HelixManager helixManager = createHelixManager();
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("testCluster"));
    when(dataAccessor.<InstanceConfig>getChildValues(any(), eq(true))).thenReturn(
        List.of(createInstanceConfig(offlineServer, TableType.OFFLINE),
            createInstanceConfig(realtimeServer, TableType.REALTIME)));
    when(helixManager.getHelixDataAccessor()).thenReturn(dataAccessor);
    SegmentAssignment segmentAssignment = SegmentAssignmentFactory.getSegmentAssignment(helixManager, tableConfig);
    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME);
    instancePartitions.setInstances(0, 0, List.of(offlineServer));
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    List<String> instancesAssigned = segmentAssignment.assignSegment(_segments.get(1), idealState,
        Map.of(InstancePartitionsType.OFFLINE, instancePartitions));
    assertEquals(new HashSet<>(instancesAssigned), Set.of(offlineServer, realtimeServer));
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState), Map.of());
  }

  private static InstanceConfig createInstanceConfig(String instanceName, TableType tableType) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.addTag(TagNameUtils.getServerTagForTenant(null, tableType));
    return instanceConfig;
  }

  /// Creates a Helix manager whose property store returns the ZK metadata of the segments, with the partition id of
  /// the segment partition column for all segments but the one without partition.
  private HelixManager createHelixManager() {
    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.get(anyString(), eq(null), eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
      String path = invocation.getArgument(0, String.class);
      return createSegmentZNRecord(path.substring(path.lastIndexOf('/') + 1));
    });
    when(propertyStore.get(anyList(), any(), eq(AccessOption.PERSISTENT), eq(true))).thenAnswer(invocation -> {
      List<String> paths = invocation.getArgument(0);
      List<ZNRecord> records = new ArrayList<>(paths.size());
      for (String path : paths) {
        records.add(createSegmentZNRecord(path.substring(path.lastIndexOf('/') + 1)));
      }
      return records;
    });
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    return helixManager;
  }

  private ZNRecord createSegmentZNRecord(String segmentName) {
    ZNRecord record = new ZNRecord(segmentName);
    int segmentId = _segments.indexOf(segmentName);
    if (segmentId >= 0) {
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(record);
      segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Map.of(PARTITION_COLUMN,
          new ColumnPartitionMetadata("murmur", NUM_PARTITIONS, Set.of(segmentId % NUM_PARTITIONS), null))));
    }
    return record;
  }
}
