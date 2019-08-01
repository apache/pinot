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
package org.apache.pinot.controller.helix.core.sharding;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.ReplicaGroupStrategyConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.partition.ReplicaGroupPartitionAssignment;
import org.apache.pinot.common.partition.ReplicaGroupPartitionAssignmentGenerator;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.ReplicaGroupTestUtils;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SegmentAssignmentStrategyTest extends ControllerTest {
  private static final String TABLE_NAME_BALANCED = "testResourceBalanced";
  private static final String TABLE_NAME_RANDOM = "testResourceRandom";
  private static final String TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT = "testReplicaGroupPartitionAssignment";
  private static final String TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP = "testTableLevelReplicaGroup";
  private static final String TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP = "testPartitionLevelReplicaGroup";
  private static final String PARTITION_COLUMN = "memberId";
  private static final int NUM_REPLICAS = 2;
  private static final int NUM_SERVERS = 10;
  private static final int NUM_BROKERS = 1;
  private static final Random RANDOM = new Random();

  private ReplicaGroupPartitionAssignmentGenerator _partitionAssignmentGenerator;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(NUM_BROKERS, true);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_SERVERS, true);

    _partitionAssignmentGenerator = new ReplicaGroupPartitionAssignmentGenerator(_propertyStore);
  }

  @Test
  public void testRandomSegmentAssignmentStrategy()
      throws Exception {
    // Adding table
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME_RANDOM)
            .setSegmentAssignmentStrategy("RandomAssignmentStrategy").setNumReplicas(NUM_REPLICAS).build();
    _helixResourceManager.addTable(tableConfig);

    // Wait for the table addition
    while (!_helixResourceManager.hasOfflineTable(TABLE_NAME_RANDOM)) {
      Thread.sleep(100);
    }

    for (int i = 0; i < 10; ++i) {
      _helixResourceManager
          .addNewSegment(TABLE_NAME_RANDOM, SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME_RANDOM),
              "downloadUrl");

      // Wait for all segments appear in the external view
      while (!allSegmentsPushedToIdealState(TABLE_NAME_RANDOM, i + 1)) {
        Thread.sleep(100);
      }
      final Set<String> taggedInstances = _helixResourceManager.getAllInstancesForServerTenant("DefaultTenant_OFFLINE");
      final Map<String, Integer> instanceToNumSegmentsMap = new HashMap<>();
      for (final String instance : taggedInstances) {
        instanceToNumSegmentsMap.put(instance, 0);
      }
      IdealState idealState = _helixAdmin
          .getResourceIdealState(getHelixClusterName(), TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_RANDOM));
      Assert.assertEquals(idealState.getPartitionSet().size(), i + 1);
      for (final String segmentId : idealState.getPartitionSet()) {
        Assert.assertEquals(idealState.getInstanceStateMap(segmentId).size(), NUM_REPLICAS);
      }
    }
  }

  @Test
  public void testBalanceNumSegmentAssignmentStrategy()
      throws Exception {
    final int numReplicas = 3;

    // Adding table
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME_BALANCED)
            .setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy").setNumReplicas(numReplicas).build();
    _helixResourceManager.addTable(tableConfig);

    int numSegments = 20;
    for (int i = 0; i < numSegments; ++i) {
      _helixResourceManager
          .addNewSegment(TABLE_NAME_BALANCED, SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME_BALANCED),
              "downloadUrl");
    }

    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME_BALANCED, numSegments)) {
      Thread.sleep(100);
    }

    final Set<String> taggedInstances = _helixResourceManager.getAllInstancesForServerTenant("DefaultTenant_OFFLINE");
    final Map<String, Integer> instance2NumSegmentsMap = new HashMap<>();
    for (final String instance : taggedInstances) {
      instance2NumSegmentsMap.put(instance, 0);
    }
    final IdealState idealState = _helixAdmin
        .getResourceIdealState(getHelixClusterName(), TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_BALANCED));
    for (final String segmentId : idealState.getPartitionSet()) {
      for (final String instance : idealState.getInstanceStateMap(segmentId).keySet()) {
        instance2NumSegmentsMap.put(instance, instance2NumSegmentsMap.get(instance) + 1);
      }
    }
    final int totalSegments = (numSegments) * numReplicas;
    final int minNumSegmentsPerInstance = totalSegments / NUM_SERVERS;
    int maxNumSegmentsPerInstance = minNumSegmentsPerInstance;
    if ((minNumSegmentsPerInstance * NUM_SERVERS) < totalSegments) {
      maxNumSegmentsPerInstance = maxNumSegmentsPerInstance + 1;
    }
    for (final String instance : instance2NumSegmentsMap.keySet()) {
      Assert.assertTrue(instance2NumSegmentsMap.get(instance) >= minNumSegmentsPerInstance,
          "expected >=" + minNumSegmentsPerInstance + " actual:" + instance2NumSegmentsMap.get(instance));
      Assert.assertTrue(instance2NumSegmentsMap.get(instance) <= maxNumSegmentsPerInstance,
          "expected <=" + maxNumSegmentsPerInstance + " actual:" + instance2NumSegmentsMap.get(instance));
    }
    _helixAdmin.dropResource(getHelixClusterName(), TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_BALANCED));
  }

  @Test
  public void testOfflineReplicaGroupPartitionAssignment()
      throws Exception {
    String rawTableName = TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT;
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);

    // Adding a table without replica group
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE)
        .setTableName(TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT)
        .setSegmentAssignmentStrategy("RandomAssignmentStrategy").setNumReplicas(NUM_REPLICAS).build();
    _helixResourceManager.addTable(tableConfig);

    // Check that partition assignment does not exist
    ReplicaGroupPartitionAssignment partitionAssignment =
        _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);

    Assert.assertTrue(partitionAssignment == null);

    // Update table config with replica group config
    int numInstancesPerPartition = 5;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(numInstancesPerPartition);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(true);

    TableConfig replicaGroupTableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE)
        .setTableName(TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT).setNumReplicas(NUM_REPLICAS)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy").build();

    replicaGroupTableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Check that the replica group partition assignment is created
    _helixResourceManager.setExistingTableConfig(replicaGroupTableConfig);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertTrue(partitionAssignment != null);

    // After table deletion, check that the replica group partition assignment is deleted
    _helixResourceManager.deleteOfflineTable(rawTableName);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertTrue(partitionAssignment == null);

    // Create a table with replica group
    _helixResourceManager.addTable(replicaGroupTableConfig);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertTrue(partitionAssignment != null);

    // Check that the replica group partition assignment is deleted
    _helixResourceManager.deleteOfflineTable(rawTableName);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertTrue(partitionAssignment == null);
  }

  /**
   * Tests to check creation/deletion of replica group znode when table is created/updated/deleted
   */
  @Test
  public void testRealtimeReplicaGroupPartitionAssignment() {
    String rawTableName = TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT;
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);

    Schema schema = new Schema.SchemaBuilder().setSchemaName(rawTableName).build();
    _helixResourceManager.addOrUpdateSchema(schema);

    Map<String, String> streamConfigMap = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();

    // Adding a table without replica group
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME)
        .setTableName(TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT)
        .setSegmentAssignmentStrategy("RandomAssignmentStrategy").setNumReplicas(NUM_REPLICAS)
        .setStreamConfigs(streamConfigMap).setLLC(true).build();
    try {
      _helixResourceManager.addTable(tableConfig);
    } catch (Exception e) {
      // ignore
    }

    // Check that partition assignment does not exist
    ReplicaGroupPartitionAssignment partitionAssignment =
        _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);

    Assert.assertNull(partitionAssignment);

    // Update table config with replica group config
    int numInstancesPerPartition = 5;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(numInstancesPerPartition);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(true);

    TableConfig replicaGroupTableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME)
        .setTableName(TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT).setNumReplicas(NUM_REPLICAS)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy").setStreamConfigs(streamConfigMap)
        .setLLC(true).build();

    replicaGroupTableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Check that the replica group partition assignment is created
    try {
      _helixResourceManager.setExistingTableConfig(replicaGroupTableConfig);
    } catch (Exception e) {
      // ignore
    }
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertNotNull(partitionAssignment);

    // After table deletion, check that the replica group partition assignment is deleted
    _helixResourceManager.deleteRealtimeTable(rawTableName);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertNull(partitionAssignment);

    // Create a table with replica group
    try {
      _helixResourceManager.addTable(replicaGroupTableConfig);
    } catch (Exception e) {
      // ignore
    }
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertNotNull(partitionAssignment);

    // Check that the replica group partition assignment is deleted
    _helixResourceManager.deleteRealtimeTable(rawTableName);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertNull(partitionAssignment);
  }

  @Test
  public void testTableLevelAndMirroringReplicaGroupSegmentAssignmentStrategy()
      throws Exception {
    // Create the configuration for segment assignment strategy.
    int numInstancesPerPartition = 5;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(numInstancesPerPartition);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(true);

    // Create table config
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE)
        .setTableName(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP).setNumReplicas(NUM_REPLICAS)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy").build();

    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Create the table
    _helixResourceManager.addTable(tableConfig);

    // Wait for table addition
    while (!_helixResourceManager.hasOfflineTable(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP)) {
      Thread.sleep(100);
    }

    // Upload segments
    Map<Integer, Set<String>> segmentsPerPartition = ReplicaGroupTestUtils
        .uploadMultipleSegmentsWithPartitionNumber(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP, 20, null,
            _helixResourceManager, 1);

    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP, 20)) {
      Thread.sleep(100);
    }

    // Fetch the replica group mapping table
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP);
    ReplicaGroupPartitionAssignment partitionAssignment =
        _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(offlineTableName);

    IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(),
        TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP));

    // Validate the segment assignment
    Assert.assertTrue(ReplicaGroupTestUtils
        .validateReplicaGroupSegmentAssignment(tableConfig, partitionAssignment, idealState.getRecord().getMapFields(),
            segmentsPerPartition));
  }

  @Test
  public void testPartitionLevelReplicaGroupSegmentAssignmentStrategy()
      throws Exception {
    int totalPartitionNumber = RANDOM.nextInt(8) + 2;
    int numInstancesPerPartition = RANDOM.nextInt(5) + 1;
    int numSegments = RANDOM.nextInt(10) + 10;

    // Create the configuration for segment assignment strategy.
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(numInstancesPerPartition);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(false);
    // Now, set the partitioning column to trigger the partition level replica group assignment.
    replicaGroupStrategyConfig.setPartitionColumn(PARTITION_COLUMN);

    // Create the indexing config
    IndexingConfig indexingConfig = new IndexingConfig();
    Map<String, ColumnPartitionConfig> partitionConfigMap = new HashMap<>();
    partitionConfigMap.put(PARTITION_COLUMN, new ColumnPartitionConfig("modulo", totalPartitionNumber));
    indexingConfig.setSegmentPartitionConfig(new SegmentPartitionConfig(partitionConfigMap));

    // Create table config
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE)
        .setTableName(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP).setNumReplicas(NUM_REPLICAS)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy").build();

    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);
    tableConfig.setIndexingConfig(indexingConfig);

    // Add table
    _helixResourceManager.addTable(tableConfig);

    // Wait for table addition
    while (!_helixResourceManager.hasOfflineTable(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP)) {
      Thread.sleep(100);
    }

    // Upload segments
    Map<Integer, Set<String>> segmentsPerPartition = ReplicaGroupTestUtils
        .uploadMultipleSegmentsWithPartitionNumber(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP, numSegments,
            PARTITION_COLUMN, _helixResourceManager, totalPartitionNumber);

    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP, numSegments)) {
      Thread.sleep(100);
    }

    // Fetch the replica group mapping table.
    String offlineTable = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP);
    ReplicaGroupPartitionAssignment partitionAssignment =
        _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(offlineTable);

    IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(),
        TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP));

    // Validate the segment assignment
    Assert.assertTrue(ReplicaGroupTestUtils
        .validateReplicaGroupSegmentAssignment(tableConfig, partitionAssignment, idealState.getRecord().getMapFields(),
            segmentsPerPartition));
  }

  private boolean allSegmentsPushedToIdealState(String tableName, int segmentNum) {
    IdealState idealState =
        _helixAdmin.getResourceIdealState(getHelixClusterName(), TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    return idealState != null && idealState.getPartitionSet() != null
        && idealState.getPartitionSet().size() == segmentNum;
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
