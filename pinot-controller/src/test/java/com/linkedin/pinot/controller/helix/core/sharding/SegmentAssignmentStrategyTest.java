/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.common.config.ColumnPartitionConfig;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignment;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignmentGenerator;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.controller.utils.ReplicaGroupTestUtils;
import com.linkedin.pinot.controller.utils.SegmentMetadataMockUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class SegmentAssignmentStrategyTest {
  private final static String ZK_SERVER = ZkStarter.DEFAULT_ZK_STR;
  private final static String HELIX_CLUSTER_NAME = "TestSegmentAssignmentStrategyHelix";
  private final static String TABLE_NAME_BALANCED = "testResourceBalanced";
  private final static String TABLE_NAME_RANDOM = "testResourceRandom";
  private final static String TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT = "testReplicaGroupPartitionAssignment";
  private final static String TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP = "testTableLevelReplicaGroup";
  private final static String TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP = "testPartitionLevelReplicaGroup";

  private static final Random random = new Random();
  private final static String PARTITION_COLUMN = "memberId";
  private final static int NUM_REPLICA = 2;
  private PinotHelixResourceManager _pinotHelixResourceManager;
  private ZkClient _zkClient;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private final int _numServerInstance = 10;
  private final int _numBrokerInstance = 1;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private ReplicaGroupPartitionAssignmentGenerator _partitionAssignmentGenerator;

  @BeforeTest
  public void setup() throws Exception {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    _zkClient = new ZkClient(ZK_SERVER);
    final String zkPath = "/" + HELIX_CLUSTER_NAME;
    if (_zkClient.exists(zkPath)) {
      _zkClient.deleteRecursive(zkPath);
    }
    final String instanceId = "localhost_helixController";
    _pinotHelixResourceManager =
        new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null, 10000L, true, /*isUpdateStateModel=*/
            false);
    _pinotHelixResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId, /*isUpdateStateModel=*/false);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    _partitionAssignmentGenerator =
        new ReplicaGroupPartitionAssignmentGenerator(_helixZkManager.getHelixPropertyStore());

    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER,
        _numServerInstance, true);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER,
        _numBrokerInstance, true);
    Thread.sleep(100);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_OFFLINE").size(),
        _numServerInstance);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_REALTIME").size(),
        _numServerInstance);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size(),
        _numBrokerInstance);
  }

  @AfterTest
  public void tearDown() {
    _pinotHelixResourceManager.stop();
    _zkClient.close();
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  @Test
  public void testRandomSegmentAssignmentStrategy() throws Exception {
    // Adding table
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME_RANDOM)
            .setSegmentAssignmentStrategy("RandomAssignmentStrategy")
            .setNumReplicas(NUM_REPLICA)
            .build();
    _pinotHelixResourceManager.addTable(tableConfig);

    // Wait for the table addition
    while (!_pinotHelixResourceManager.hasOfflineTable(TABLE_NAME_RANDOM)) {
      Thread.sleep(100);
    }

    for (int i = 0; i < 10; ++i) {
      _pinotHelixResourceManager.addNewSegment(SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME_RANDOM),
          "downloadUrl");

      // Wait for all segments appear in the external view
      while (!allSegmentsPushedToIdealState(TABLE_NAME_RANDOM, i + 1)) {
        Thread.sleep(100);
      }
      final Set<String> taggedInstances =
          _pinotHelixResourceManager.getAllInstancesForServerTenant("DefaultTenant_OFFLINE");
      final Map<String, Integer> instanceToNumSegmentsMap = new HashMap<>();
      for (final String instance : taggedInstances) {
        instanceToNumSegmentsMap.put(instance, 0);
      }
      IdealState idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME,
          TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_RANDOM));
      Assert.assertEquals(idealState.getPartitionSet().size(), i + 1);
      for (final String segmentId : idealState.getPartitionSet()) {
        Assert.assertEquals(idealState.getInstanceStateMap(segmentId).size(), NUM_REPLICA);
      }
    }
  }

  @Test
  public void testBalanceNumSegmentAssignmentStrategy() throws Exception {
    final int numReplicas = 3;

    // Adding table
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME_BALANCED)
            .setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy")
            .setNumReplicas(numReplicas)
            .build();
    _pinotHelixResourceManager.addTable(tableConfig);

    int numSegments = 20;
    for (int i = 0; i < numSegments; ++i) {
      _pinotHelixResourceManager.addNewSegment(SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME_BALANCED),
          "downloadUrl");
    }

    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME_BALANCED, numSegments)) {
      Thread.sleep(100);
    }

    final Set<String> taggedInstances =
        _pinotHelixResourceManager.getAllInstancesForServerTenant("DefaultTenant_OFFLINE");
    final Map<String, Integer> instance2NumSegmentsMap = new HashMap<>();
    for (final String instance : taggedInstances) {
      instance2NumSegmentsMap.put(instance, 0);
    }
    final IdealState idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME,
        TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_BALANCED));
    for (final String segmentId : idealState.getPartitionSet()) {
      for (final String instance : idealState.getInstanceStateMap(segmentId).keySet()) {
        instance2NumSegmentsMap.put(instance, instance2NumSegmentsMap.get(instance) + 1);
      }
    }
    final int totalSegments = (numSegments) * numReplicas;
    final int minNumSegmentsPerInstance = totalSegments / _numServerInstance;
    int maxNumSegmentsPerInstance = minNumSegmentsPerInstance;
    if ((minNumSegmentsPerInstance * _numServerInstance) < totalSegments) {
      maxNumSegmentsPerInstance = maxNumSegmentsPerInstance + 1;
    }
    for (final String instance : instance2NumSegmentsMap.keySet()) {
      Assert.assertTrue(instance2NumSegmentsMap.get(instance) >= minNumSegmentsPerInstance,
          "expected >=" + minNumSegmentsPerInstance + " actual:" + instance2NumSegmentsMap.get(instance));
      Assert.assertTrue(instance2NumSegmentsMap.get(instance) <= maxNumSegmentsPerInstance,
          "expected <=" + maxNumSegmentsPerInstance + " actual:" + instance2NumSegmentsMap.get(instance));
    }
    _helixAdmin.dropResource(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_BALANCED));
  }

  @Test
  public void testReplicaGroupPartitionAssignment() throws Exception {
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT);

    // Adding a table without replica group
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(
        TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT)
        .setSegmentAssignmentStrategy("RandomAssignmentStrategy")
        .setNumReplicas(NUM_REPLICA)
        .build();
    _pinotHelixResourceManager.addTable(tableConfig);

    // Check that partition assignment does not exist
    ReplicaGroupPartitionAssignment partitionAssignment =
        _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);

    Assert.assertTrue(partitionAssignment == null);

    // Update table config with replica group config
    int numInstancesPerPartition = 5;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(numInstancesPerPartition);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(true);

    TableConfig replicaGroupTableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(
        TABLE_NAME_REPLICA_GROUP_PARTITION_ASSIGNMENT)
        .setNumReplicas(NUM_REPLICA)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy")
        .build();

    replicaGroupTableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Check that the replica group partition assignment is created
    _pinotHelixResourceManager.setExistingTableConfig(replicaGroupTableConfig, tableNameWithType, CommonConstants.Helix.TableType.OFFLINE);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertTrue(partitionAssignment != null);

    // After table deletion, check that the replica group partition assignment is deleted
    _pinotHelixResourceManager.deleteOfflineTable(tableNameWithType);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertTrue(partitionAssignment == null);

    // Create a table with replica group
    _pinotHelixResourceManager.addTable(replicaGroupTableConfig);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertTrue(partitionAssignment != null);

    // Check that the replica group partition assignment is deleted
    _pinotHelixResourceManager.deleteOfflineTable(tableNameWithType);
    partitionAssignment = _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(tableNameWithType);
    Assert.assertTrue(partitionAssignment == null);
  }

  @Test
  public void testTableLevelAndMirroringReplicaGroupSegmentAssignmentStrategy() throws Exception {
    // Create the configuration for segment assignment strategy.
    int numInstancesPerPartition = 5;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(numInstancesPerPartition);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(true);

    // Create table config
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(
        TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP)
        .setNumReplicas(NUM_REPLICA)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy")
        .build();

    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Create the table
    _pinotHelixResourceManager.addTable(tableConfig);

    // Wait for table addition
    while (!_pinotHelixResourceManager.hasOfflineTable(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP)) {
      Thread.sleep(100);
    }

    // Upload segments
    Map<Integer, Set<String>> segmentsPerPartition =
        ReplicaGroupTestUtils.uploadMultipleSegmentsWithPartitionNumber(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP, 20, null,
            _pinotHelixResourceManager, 1);

    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP, 20)) {
      Thread.sleep(100);
    }

    // Fetch the replica group mapping table
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP);
    ReplicaGroupPartitionAssignment partitionAssignment =
        _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(offlineTableName);

    IdealState idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME,
        TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP));

    // Validate the segment assignment
    Assert.assertTrue(
        ReplicaGroupTestUtils.validateReplicaGroupSegmentAssignment(tableConfig, partitionAssignment,
            idealState.getRecord().getMapFields(), segmentsPerPartition));
  }

  @Test
  public void testPartitionLevelReplicaGroupSegmentAssignmentStrategy() throws Exception {
    int totalPartitionNumber = random.nextInt(8) + 2;
    int numInstancesPerPartition = random.nextInt(5) + 1;
    int numSegments = random.nextInt(10) + 10;

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
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(
        TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP)
        .setNumReplicas(NUM_REPLICA)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy")
        .build();

    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);
    tableConfig.setIndexingConfig(indexingConfig);

    // Add table
    _pinotHelixResourceManager.addTable(tableConfig);

    // Wait for table addition
    while (!_pinotHelixResourceManager.hasOfflineTable(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP)) {
      Thread.sleep(100);
    }

    // Upload segments
    Map<Integer, Set<String>> segmentsPerPartition =
        ReplicaGroupTestUtils.uploadMultipleSegmentsWithPartitionNumber(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP,
            numSegments, PARTITION_COLUMN, _pinotHelixResourceManager, totalPartitionNumber);

    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP, numSegments)) {
      Thread.sleep(100);
    }

    // Fetch the replica group mapping table.
    String offlineTable = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP);
    ReplicaGroupPartitionAssignment partitionAssignment =
        _partitionAssignmentGenerator.getReplicaGroupPartitionAssignment(offlineTable);

    IdealState idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME,
        TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP));

    // Validate the segment assignment
    Assert.assertTrue(
        ReplicaGroupTestUtils.validateReplicaGroupSegmentAssignment(tableConfig, partitionAssignment,
            idealState.getRecord().getMapFields(), segmentsPerPartition));
  }

  private boolean allSegmentsPushedToIdealState(String tableName, int segmentNum) {
    IdealState idealState =
        _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    return idealState != null && idealState.getPartitionSet() != null
        && idealState.getPartitionSet().size() == segmentNum;
  }
}
