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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
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
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.spi.utils.RebalanceConfigConstants;
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
public class ReplicaGroupSegmentAssignmentStrategyTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final int NUM_SEGMENTS = 12;
  private static final List<String> SEGMENTS =
      SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, NUM_SEGMENTS);
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 18;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String RAW_TABLE_NAME_WITHOUT_PARTITION = "testTableWithoutPartition";
  private static final String INSTANCE_PARTITIONS_NAME_WITHOUT_PARTITION =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME_WITHOUT_PARTITION);
  private static final String RAW_TABLE_NAME_WITH_PARTITION = "testTableWithPartition";
  private static final String OFFLINE_TABLE_NAME_WITH_PARTITION =
      TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_WITH_PARTITION);
  private static final String INSTANCE_PARTITIONS_NAME_WITH_PARTITION =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME_WITH_PARTITION);
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final int NUM_PARTITIONS = 3;

  private SegmentAssignment _segmentAssignmentWithoutPartition;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMapWithoutPartition;
  private SegmentAssignment _segmentAssignmentWithPartition;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMapWithPartition;

  @BeforeClass
  public void setUp() {
    TableConfig tableConfigWithoutPartitions =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME_WITHOUT_PARTITION)
            .setNumReplicas(NUM_REPLICAS)
            .setSegmentAssignmentStrategy(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY).build();
    _segmentAssignmentWithoutPartition =
        SegmentAssignmentFactory.getSegmentAssignment(null, tableConfigWithoutPartitions, null);

    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5],
    //   0_1=[instance_6, instance_7, instance_8, instance_9, instance_10, instance_11],
    //   0_2=[instance_12, instance_13, instance_14, instance_15, instance_16, instance_17]
    // }
    InstancePartitions instancePartitionsWithoutPartition =
        new InstancePartitions(INSTANCE_PARTITIONS_NAME_WITHOUT_PARTITION);
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS;
    int instanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> instancesForReplicaGroup = new ArrayList<>(numInstancesPerReplicaGroup);
      for (int i = 0; i < numInstancesPerReplicaGroup; i++) {
        instancesForReplicaGroup.add(INSTANCES.get(instanceIdToAdd++));
      }
      instancePartitionsWithoutPartition.setInstances(0, replicaGroupId, instancesForReplicaGroup);
    }
    _instancePartitionsMapWithoutPartition =
        Collections.singletonMap(InstancePartitionsType.OFFLINE, instancePartitionsWithoutPartition);

    // Mock HelixManager
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

    int numInstancesPerPartition = numInstancesPerReplicaGroup / NUM_REPLICAS;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        new ReplicaGroupStrategyConfig(PARTITION_COLUMN, numInstancesPerPartition);
    TableConfig tableConfigWithPartitions =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME_WITH_PARTITION)
            .setNumReplicas(NUM_REPLICAS)
            .setSegmentAssignmentStrategy(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY)
            .setReplicaGroupStrategyConfig(replicaGroupStrategyConfig).build();
    _segmentAssignmentWithPartition =
        SegmentAssignmentFactory.getSegmentAssignment(helixManagerWithPartitions, tableConfigWithPartitions, null);

    // {
    //   0_0=[instance_0, instance_1], 1_0=[instance_2, instance_3], 2_0=[instance_4, instance_5],
    //   0_1=[instance_6, instance_7], 1_1=[instance_8, instance_9], 2_1=[instance_10, instance_11],
    //   0_2=[instance_12, instance_13], 1_2=[instance_14, instance_15], 2_2=[instance_16, instance_17]
    // }
    InstancePartitions instancePartitionsWithPartition =
        new InstancePartitions(INSTANCE_PARTITIONS_NAME_WITH_PARTITION);
    instanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
        List<String> instancesForPartition = new ArrayList<>(numInstancesPerPartition);
        for (int i = 0; i < numInstancesPerPartition; i++) {
          instancesForPartition.add(INSTANCES.get(instanceIdToAdd++));
        }
        instancePartitionsWithPartition.setInstances(partitionId, replicaGroupId, instancesForPartition);
      }
    }
    _instancePartitionsMapWithPartition =
        Collections.singletonMap(InstancePartitionsType.OFFLINE, instancePartitionsWithPartition);
  }

  @Test
  public void testFactory() {
    assertTrue(_segmentAssignmentWithoutPartition instanceof OfflineSegmentAssignment);
    assertTrue(_segmentAssignmentWithPartition instanceof OfflineSegmentAssignment);
  }

  @Test
  public void testAssignSegmentWithoutPartition() {
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      List<String> instancesAssigned = _segmentAssignmentWithoutPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithoutPartition);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);

      // Segment 0 should be assigned to instance 0, 6, 12
      // Segment 1 should be assigned to instance 1, 7, 13
      // Segment 2 should be assigned to instance 2, 8, 14
      // Segment 3 should be assigned to instance 3, 9, 15
      // Segment 4 should be assigned to instance 4, 10, 16
      // Segment 5 should be assigned to instance 5, 11, 17
      // Segment 6 should be assigned to instance 0, 6, 12
      // Segment 7 should be assigned to instance 1, 7, 13
      // ...
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int expectedAssignedInstanceId =
            segmentId % numInstancesPerReplicaGroup + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), INSTANCES.get(expectedAssignedInstanceId));
      }

      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
  }

  @Test
  public void testAssignSegmentWithPartition() {
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    int numInstancesPerPartition = numInstancesPerReplicaGroup / NUM_PARTITIONS;
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      List<String> instancesAssigned = _segmentAssignmentWithPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithPartition);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);

      // Segment 0 (partition 0) should be assigned to instance 0, 6, 12
      // Segment 1 (partition 1) should be assigned to instance 2, 8, 14
      // Segment 2 (partition 2) should be assigned to instance 4, 10, 16
      // Segment 3 (partition 0) should be assigned to instance 1, 7, 13
      // Segment 4 (partition 1) should be assigned to instance 3, 9, 15
      // Segment 5 (partition 2) should be assigned to instance 5, 11, 17
      // Segment 6 (partition 0) should be assigned to instance 0, 6, 12
      // Segment 7 (partition 1) should be assigned to instance 2, 8, 14
      // ...
      int partitionId = segmentId % NUM_PARTITIONS;
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int expectedAssignedInstanceId =
            (segmentId % numInstancesPerReplicaGroup) / NUM_PARTITIONS + partitionId * numInstancesPerPartition
                + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), INSTANCES.get(expectedAssignedInstanceId));
      }

      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }
  }

  @Test
  public void testTableBalancedWithoutPartition() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned = _segmentAssignmentWithoutPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithoutPartition);
      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    assertEquals(currentAssignment.size(), NUM_SEGMENTS);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, INSTANCES);
    int[] expectedNumSegmentsAssignedPerInstance = new int[NUM_INSTANCES];
    int numSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Current assignment should already be balanced
    assertEquals(_segmentAssignmentWithoutPartition
            .rebalanceTable(currentAssignment, _instancePartitionsMapWithoutPartition, null, null,
                new BaseConfiguration()),
        currentAssignment);
  }

  @Test
  public void testTableBalancedWithPartition() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned = _segmentAssignmentWithPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithPartition);
      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    assertEquals(currentAssignment.size(), NUM_SEGMENTS);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, INSTANCES);
    int[] expectedNumSegmentsAssignedPerInstance = new int[NUM_INSTANCES];
    int numSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Current assignment should already be balanced
    assertEquals(_segmentAssignmentWithPartition
            .rebalanceTable(currentAssignment, _instancePartitionsMapWithPartition, null, null,
                new BaseConfiguration()),
        currentAssignment);
  }

  @Test
  public void testBootstrapTableWithoutPartition() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned = _segmentAssignmentWithoutPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithoutPartition);
      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    // Bootstrap table should reassign all segments based on their alphabetical order
    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.BOOTSTRAP, true);
    Map<String, Map<String, String>> newAssignment = _segmentAssignmentWithoutPartition
        .rebalanceTable(currentAssignment, _instancePartitionsMapWithoutPartition, null, null, rebalanceConfig);
    assertEquals(newAssignment.size(), NUM_SEGMENTS);
    List<String> sortedSegments = new ArrayList<>(SEGMENTS);
    sortedSegments.sort(null);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      assertEquals(newAssignment.get(sortedSegments.get(i)), currentAssignment.get(SEGMENTS.get(i)));
    }
  }

  @Test
  public void testBootstrapTableWithPartition() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned = _segmentAssignmentWithPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithPartition);
      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    // Bootstrap table should reassign all segments based on their alphabetical order within the partition
    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.BOOTSTRAP, true);
    Map<String, Map<String, String>> newAssignment = _segmentAssignmentWithPartition
        .rebalanceTable(currentAssignment, _instancePartitionsMapWithPartition, null, null, rebalanceConfig);
    assertEquals(newAssignment.size(), NUM_SEGMENTS);
    int numSegmentsPerPartition = NUM_SEGMENTS / NUM_PARTITIONS;
    String[][] partitionIdToSegmentsMap = new String[NUM_PARTITIONS][numSegmentsPerPartition];
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      partitionIdToSegmentsMap[i % NUM_PARTITIONS][i / NUM_PARTITIONS] = SEGMENTS.get(i);
    }
    String[][] partitionIdToSortedSegmentsMap = new String[NUM_PARTITIONS][numSegmentsPerPartition];
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      String[] sortedSegments = new String[numSegmentsPerPartition];
      System.arraycopy(partitionIdToSegmentsMap[i], 0, sortedSegments, 0, numSegmentsPerPartition);
      Arrays.sort(sortedSegments);
      partitionIdToSortedSegmentsMap[i] = sortedSegments;
    }
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      for (int j = 0; j < numSegmentsPerPartition; j++) {
        assertEquals(newAssignment.get(partitionIdToSortedSegmentsMap[i][j]),
            currentAssignment.get(partitionIdToSegmentsMap[i][j]));
      }
    }
  }

  @Test
  public void testRebalanceTableWithPartitionColumnAndInstancePartitionsMapWithOnePartition() {
    // make an unbalanced assignment by assigning all segments to the first three instances
    String instance0 = INSTANCE_NAME_PREFIX + "0";
    String instance1 = INSTANCE_NAME_PREFIX + "1";
    String instance2 = INSTANCE_NAME_PREFIX + "2";
    Map<String, Map<String, String>> unbalancedAssignment = new TreeMap<>();
    SEGMENTS.forEach(segName -> unbalancedAssignment.put(segName, ImmutableMap
        .of(instance0, SegmentStateModel.ONLINE, instance1, SegmentStateModel.ONLINE, instance2,
            SegmentStateModel.ONLINE)));
    Map<String, Map<String, String>> balancedAssignment = _segmentAssignmentWithPartition
        .rebalanceTable(unbalancedAssignment, _instancePartitionsMapWithoutPartition, null, null,
            new BaseConfiguration());
    int[] actualNumSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(balancedAssignment, INSTANCES);
    int[] expectedNumSegmentsAssignedPerInstance = new int[NUM_INSTANCES];
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES);
    assertEquals(actualNumSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
  }

  @Test
  public void testOneReplicaWithPartition() {
    // Mock HelixManager
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    List<ZNRecord> segmentZKMetadataZNRecords = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      int partitionId = segmentId % NUM_PARTITIONS;
      segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Collections.singletonMap(PARTITION_COLUMN,
          new ColumnPartitionMetadata(null, NUM_PARTITIONS, Collections.singleton(partitionId), null))));
      ZNRecord segmentZKMetadataZNRecord = segmentZKMetadata.toZNRecord();
      when(propertyStore.get(
          eq(ZKMetadataProvider.constructPropertyStorePathForSegment(OFFLINE_TABLE_NAME_WITH_PARTITION, segmentName)),
          any(), anyInt())).thenReturn(segmentZKMetadataZNRecord);
      segmentZKMetadataZNRecords.add(segmentZKMetadataZNRecord);
    }
    when(propertyStore
        .getChildren(eq(ZKMetadataProvider.constructPropertyStorePathForResource(OFFLINE_TABLE_NAME_WITH_PARTITION)),
            any(), anyInt(), anyInt(), anyInt())).thenReturn(segmentZKMetadataZNRecords);
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    int numInstancesPerPartition = NUM_INSTANCES / NUM_PARTITIONS;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        new ReplicaGroupStrategyConfig(PARTITION_COLUMN, numInstancesPerPartition);
    TableConfig tableConfigWithPartitions =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME_WITH_PARTITION).setNumReplicas(1)
            .setSegmentAssignmentStrategy(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY).build();
    tableConfigWithPartitions.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);
    SegmentAssignment segmentAssignment =
        SegmentAssignmentFactory.getSegmentAssignment(helixManager, tableConfigWithPartitions, null);

    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5],
    //   1_0=[instance_6, instance_7, instance_8, instance_9, instance_10, instance_11],
    //   2_0=[instance_12, instance_13, instance_14, instance_15, instance_16, instance_17],
    // }
    InstancePartitions instancePartitions = new InstancePartitions(INSTANCE_PARTITIONS_NAME_WITH_PARTITION);
    int instanceIdToAdd = 0;
    for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
      List<String> instancesForPartition = new ArrayList<>(numInstancesPerPartition);
      for (int i = 0; i < numInstancesPerPartition; i++) {
        instancesForPartition.add(INSTANCES.get(instanceIdToAdd++));
      }
      instancePartitions.setInstances(partitionId, 0, instancesForPartition);
    }
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        Collections.singletonMap(InstancePartitionsType.OFFLINE, instancePartitions);

    // Test assignment
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      List<String> instancesAssigned =
          segmentAssignment.assignSegment(segmentName, currentAssignment, instancePartitionsMap);
      assertEquals(instancesAssigned.size(), 1);

      // Segment 0 (partition 0) should be assigned to instance 0
      // Segment 1 (partition 1) should be assigned to instance 6
      // Segment 2 (partition 2) should be assigned to instance 12
      // Segment 3 (partition 0) should be assigned to instance 1
      // Segment 4 (partition 1) should be assigned to instance 7
      // Segment 5 (partition 2) should be assigned to instance 13
      // Segment 6 (partition 0) should be assigned to instance 2
      // Segment 7 (partition 1) should be assigned to instance 8
      // ...
      int partitionId = segmentId % NUM_PARTITIONS;
      int expectedAssignedInstanceId =
          (segmentId % NUM_INSTANCES) / NUM_PARTITIONS + partitionId * numInstancesPerPartition;
      assertEquals(instancesAssigned.get(0), INSTANCES.get(expectedAssignedInstanceId));

      currentAssignment
          .put(segmentName, SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentStateModel.ONLINE));
    }

    // Current assignment should already be balanced
    assertEquals(
        segmentAssignment.rebalanceTable(currentAssignment, instancePartitionsMap, null, null, new BaseConfiguration()),
        currentAssignment);

    // Test bootstrap
    // Bootstrap table should reassign all segments based on their alphabetical order within the partition
    Configuration rebalanceConfig = new BaseConfiguration();
    rebalanceConfig.setProperty(RebalanceConfigConstants.BOOTSTRAP, true);
    Map<String, Map<String, String>> newAssignment =
        segmentAssignment.rebalanceTable(currentAssignment, instancePartitionsMap, null, null, rebalanceConfig);
    assertEquals(newAssignment.size(), NUM_SEGMENTS);
    int numSegmentsPerPartition = NUM_SEGMENTS / NUM_PARTITIONS;
    String[][] partitionIdToSegmentsMap = new String[NUM_PARTITIONS][numSegmentsPerPartition];
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      partitionIdToSegmentsMap[i % NUM_PARTITIONS][i / NUM_PARTITIONS] = SEGMENTS.get(i);
    }
    String[][] partitionIdToSortedSegmentsMap = new String[NUM_PARTITIONS][numSegmentsPerPartition];
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      String[] sortedSegments = new String[numSegmentsPerPartition];
      System.arraycopy(partitionIdToSegmentsMap[i], 0, sortedSegments, 0, numSegmentsPerPartition);
      Arrays.sort(sortedSegments);
      partitionIdToSortedSegmentsMap[i] = sortedSegments;
    }
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      for (int j = 0; j < numSegmentsPerPartition; j++) {
        assertEquals(newAssignment.get(partitionIdToSortedSegmentsMap[i][j]),
            currentAssignment.get(partitionIdToSegmentsMap[i][j]));
      }
    }
  }
}
