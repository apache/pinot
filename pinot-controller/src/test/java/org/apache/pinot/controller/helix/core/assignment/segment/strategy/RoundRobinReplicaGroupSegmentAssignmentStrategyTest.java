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
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.assignment.segment.OfflineSegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentTestUtils;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.SegmentAssignmentConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("unchecked")
public class RoundRobinReplicaGroupSegmentAssignmentStrategyTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final int NUM_SEGMENTS = 12;
  private static final List<String> SEGMENTS =
      SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, NUM_SEGMENTS);
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 18;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  // Use unique table names to avoid counter interference with other test classes
  private static final String RAW_TABLE_NAME_WITHOUT_PARTITION = "rrReplicaGroupTableWithoutPartition";
  private static final String INSTANCE_PARTITIONS_NAME_WITHOUT_PARTITION =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME_WITHOUT_PARTITION);
  private static final String RAW_TABLE_NAME_WITH_PARTITION = "rrReplicaGroupTableWithPartition";
  private static final String OFFLINE_TABLE_NAME_WITH_PARTITION =
      TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_WITH_PARTITION);
  private static final String INSTANCE_PARTITIONS_NAME_WITH_PARTITION =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME_WITH_PARTITION);
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final int NUM_PARTITIONS = 3;

  private SegmentAssignment _segmentAssignmentWithoutPartition;
  private InstancePartitions _instancePartitionsWithoutPartition;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMapWithoutPartition;
  private SegmentAssignment _segmentAssignmentWithPartition;
  private InstancePartitions _instancePartitionsWithPartition;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMapWithPartition;

  @BeforeClass
  public void setUp() {
    TableConfig tableConfigWithoutPartitions =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME_WITHOUT_PARTITION)
            .setNumReplicas(NUM_REPLICAS)
            .setSegmentAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new SegmentAssignmentConfig(
                    AssignmentStrategy.ROUND_ROBIN_REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY))).build();
    _segmentAssignmentWithoutPartition =
        SegmentAssignmentFactory.getSegmentAssignment(null, tableConfigWithoutPartitions, null);

    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5],
    //   0_1=[instance_6, instance_7, instance_8, instance_9, instance_10, instance_11],
    //   0_2=[instance_12, instance_13, instance_14, instance_15, instance_16, instance_17]
    // }
    _instancePartitionsWithoutPartition = new InstancePartitions(INSTANCE_PARTITIONS_NAME_WITHOUT_PARTITION);
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS;
    int instanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> instancesForReplicaGroup = new ArrayList<>(numInstancesPerReplicaGroup);
      for (int i = 0; i < numInstancesPerReplicaGroup; i++) {
        instancesForReplicaGroup.add(INSTANCES.get(instanceIdToAdd++));
      }
      _instancePartitionsWithoutPartition.setInstances(0, replicaGroupId, instancesForReplicaGroup);
    }
    _instancePartitionsMapWithoutPartition =
        Collections.singletonMap(InstancePartitionsType.OFFLINE, _instancePartitionsWithoutPartition);

    // Mock HelixManager for partition metadata
    ZkHelixPropertyStore<ZNRecord> propertyStoreWithPartitions = mock(ZkHelixPropertyStore.class);
    List<ZNRecord> segmentZKMetadataZNRecords = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      int partitionId = segmentId % NUM_PARTITIONS;
      segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Collections.singletonMap(PARTITION_COLUMN,
          new ColumnPartitionMetadata(null, NUM_PARTITIONS, Collections.singleton(partitionId), null))));
      ZNRecord segmentZKMetadataZNRecord = segmentZKMetadata.toZNRecord();
      when(propertyStoreWithPartitions.get(
          eq(ZKMetadataProvider.constructPropertyStorePathForSegment(OFFLINE_TABLE_NAME_WITH_PARTITION, segmentName)),
          any(), anyInt())).thenReturn(segmentZKMetadataZNRecord);
      segmentZKMetadataZNRecords.add(segmentZKMetadataZNRecord);
    }
    when(propertyStoreWithPartitions
        .getChildren(eq(ZKMetadataProvider.constructPropertyStorePathForResource(OFFLINE_TABLE_NAME_WITH_PARTITION)),
            any(), anyInt(), anyInt(), anyInt())).thenReturn(segmentZKMetadataZNRecords);
    HelixManager helixManagerWithPartitions = mock(HelixManager.class);
    when(helixManagerWithPartitions.getHelixPropertyStore()).thenReturn(propertyStoreWithPartitions);

    int numInstancesPerPartition = numInstancesPerReplicaGroup / NUM_PARTITIONS;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        new ReplicaGroupStrategyConfig(PARTITION_COLUMN, numInstancesPerPartition);
    TableConfig tableConfigWithPartitions =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME_WITH_PARTITION)
            .setNumReplicas(NUM_REPLICAS)
            .setReplicaGroupStrategyConfig(replicaGroupStrategyConfig)
            .setSegmentAssignmentConfigMap(Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(),
                new SegmentAssignmentConfig(
                    AssignmentStrategy.ROUND_ROBIN_REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY))).build();
    _segmentAssignmentWithPartition =
        SegmentAssignmentFactory.getSegmentAssignment(helixManagerWithPartitions, tableConfigWithPartitions, null);

    // {
    //   0_0=[instance_0, instance_1], 1_0=[instance_2, instance_3], 2_0=[instance_4, instance_5],
    //   0_1=[instance_6, instance_7], 1_1=[instance_8, instance_9], 2_1=[instance_10, instance_11],
    //   0_2=[instance_12, instance_13], 1_2=[instance_14, instance_15], 2_2=[instance_16, instance_17]
    // }
    _instancePartitionsWithPartition = new InstancePartitions(INSTANCE_PARTITIONS_NAME_WITH_PARTITION);
    instanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
        List<String> instancesForPartition = new ArrayList<>(numInstancesPerPartition);
        for (int i = 0; i < numInstancesPerPartition; i++) {
          instancesForPartition.add(INSTANCES.get(instanceIdToAdd++));
        }
        _instancePartitionsWithPartition.setInstances(partitionId, replicaGroupId, instancesForPartition);
      }
    }
    _instancePartitionsMapWithPartition =
        Collections.singletonMap(InstancePartitionsType.OFFLINE, _instancePartitionsWithPartition);
  }

  @Test
  public void testFactory() {
    assertTrue(_segmentAssignmentWithoutPartition instanceof OfflineSegmentAssignment);
    assertTrue(_segmentAssignmentWithPartition instanceof OfflineSegmentAssignment);
  }

  @Test
  public void testAssignSegmentWithoutPartition() {
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS; // 6
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // The counter initializes to a random value; derive the starting instanceId from the first result.
    List<String> firstInstances = _segmentAssignmentWithoutPartition
        .assignSegment(SEGMENTS.get(0), currentAssignment, _instancePartitionsMapWithoutPartition);
    assertEquals(firstInstances.size(), NUM_REPLICAS);
    // The counter advances once per segment. Each replica group contributes the instance at the same instanceId.
    int startIdx = _instancePartitionsWithoutPartition.getInstances(0, 0).indexOf(firstInstances.get(0));
    for (int rgId = 0; rgId < NUM_REPLICAS; rgId++) {
      assertEquals(firstInstances.get(rgId),
          _instancePartitionsWithoutPartition.getInstances(0, rgId).get(startIdx));
    }
    currentAssignment.put(SEGMENTS.get(0),
        SegmentAssignmentUtils.getInstanceStateMap(firstInstances, SegmentStateModel.ONLINE));

    // Each subsequent segment advances the instanceId by 1 within the replica group.
    for (int segId = 1; segId < NUM_SEGMENTS; segId++) {
      int expectedIdx = (startIdx + segId) % numInstancesPerReplicaGroup;
      List<String> instances = _segmentAssignmentWithoutPartition
          .assignSegment(SEGMENTS.get(segId), currentAssignment, _instancePartitionsMapWithoutPartition);
      assertEquals(instances.size(), NUM_REPLICAS);
      for (int rgId = 0; rgId < NUM_REPLICAS; rgId++) {
        assertEquals(instances.get(rgId),
            _instancePartitionsWithoutPartition.getInstances(0, rgId).get(expectedIdx));
      }
      currentAssignment.put(SEGMENTS.get(segId),
          SegmentAssignmentUtils.getInstanceStateMap(instances, SegmentStateModel.ONLINE));
    }
  }

  @Test
  public void testAssignSegmentWithPartition() {
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS; // 6
    int numInstancesPerPartition = numInstancesPerReplicaGroup / NUM_PARTITIONS; // 2
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();

    // Each partition has its own independent counter, so track start and assignment count per partition.
    int[] partitionStartIdx = new int[NUM_PARTITIONS];
    Arrays.fill(partitionStartIdx, -1);
    int[] partitionAssignmentCount = new int[NUM_PARTITIONS];

    for (int segId = 0; segId < NUM_SEGMENTS; segId++) {
      int partitionId = segId % NUM_PARTITIONS;
      List<String> instances = _segmentAssignmentWithPartition
          .assignSegment(SEGMENTS.get(segId), currentAssignment, _instancePartitionsMapWithPartition);
      assertEquals(instances.size(), NUM_REPLICAS);

      if (partitionStartIdx[partitionId] == -1) {
        // First segment for this partition — observe the random starting index.
        partitionStartIdx[partitionId] =
            _instancePartitionsWithPartition.getInstances(partitionId, 0).indexOf(instances.get(0));
      }
      int expectedIdx =
          (partitionStartIdx[partitionId] + partitionAssignmentCount[partitionId]) % numInstancesPerPartition;
      partitionAssignmentCount[partitionId]++;

      for (int rgId = 0; rgId < NUM_REPLICAS; rgId++) {
        assertEquals(instances.get(rgId),
            _instancePartitionsWithPartition.getInstances(partitionId, rgId).get(expectedIdx));
      }
      currentAssignment.put(SEGMENTS.get(segId),
          SegmentAssignmentUtils.getInstanceStateMap(instances, SegmentStateModel.ONLINE));
    }
  }
}
