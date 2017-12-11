/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.sharding;

import com.linkedin.pinot.common.config.ColumnPartitionConfig;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.math.IntRange;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class SegmentAssignmentStrategyTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentAssignmentStrategyTest.class);

  private final static String ZK_SERVER = ZkStarter.DEFAULT_ZK_STR;
  private final static String HELIX_CLUSTER_NAME = "TestSegmentAssignmentStrategyHelix";
  private final static String TABLE_NAME_BALANCED = "testResourceBalanced";
  private final static String TABLE_NAME_RANDOM = "testResourceRandom";
  private final static String TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP= "testTableLevelReplicaGroup";
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
        new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null, 10000L, true, /*isUpdateStateModel=*/false);
    _pinotHelixResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId, /*isUpdateStateModel=*/false);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();

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
      addOneSegment(TABLE_NAME_RANDOM);

      // Wait for all segments appear in the external view
      while (!allSegmentsPushedToIdealState(TABLE_NAME_RANDOM, i + 1)) {
        Thread.sleep(100);
      }
      final Set<String> taggedInstances =
          _pinotHelixResourceManager.getAllInstancesForServerTenant("DefaultTenant_OFFLINE");
      final Map<String, Integer> instance2NumSegmentsMap = new HashMap<String, Integer>();
      for (final String instance : taggedInstances) {
        instance2NumSegmentsMap.put(instance, 0);
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
      addOneSegment(TABLE_NAME_BALANCED);
    }

    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME_BALANCED, numSegments)) {
      Thread.sleep(100);
    }

    final Set<String> taggedInstances =
        _pinotHelixResourceManager.getAllInstancesForServerTenant("DefaultTenant_OFFLINE");
    final Map<String, Integer> instance2NumSegmentsMap = new HashMap<String, Integer>();
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
  public void testTableLevelAndMirroringReplicaGroupSegmentAssignmentStrategy() throws Exception {
    // Create the configuration for segment assignment strategy.
    int numInstancesPerPartition = 5;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(numInstancesPerPartition);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(true);

    // Create table config
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE)
        .setTableName(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP)
        .setNumReplicas(NUM_REPLICA)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy")
        .build();

    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Create the table and upload segments
    _pinotHelixResourceManager.addTable(tableConfig);

    // Wait for table addition
    while (!_pinotHelixResourceManager.hasOfflineTable(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP)) {
      Thread.sleep(100);
    }

    int numSegments = 20;
    Set<String> segments = new HashSet<>();
    for (int i = 0; i < numSegments; ++i) {
      String segmentName = "segment" + i;
      addOneSegmentWithPartitionInfo(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP, segmentName, null, 0);
      segments.add(segmentName);
    }

    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP, numSegments)) {
      Thread.sleep(100);
    }

    // Create a table of a list of segments that are assigned to a server.
    Map<String, Set<String>> serverToSegments = getServersToSegmentsMapping(TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP);

    // Fetch the replica group mapping table
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixZkManager.getHelixPropertyStore();
    PartitionToReplicaGroupMappingZKMetadata partitionToReplicaGroupMaping =
        ZKMetadataProvider.getPartitionToReplicaGroupMappingZKMedata(propertyStore,
            TABLE_NAME_TABLE_LEVEL_REPLICA_GROUP);

    // Check that each replica group for contains all segments of the table.
    for (int group = 0; group < NUM_REPLICA; group++) {
      List<String> serversInReplicaGroup = partitionToReplicaGroupMaping.getInstancesfromReplicaGroup(0, group);
      Set<String> segmentsInReplicaGroup = new HashSet<>();
      for (String server : serversInReplicaGroup) {
        segmentsInReplicaGroup.addAll(serverToSegments.get(server));
      }
      Assert.assertTrue(segmentsInReplicaGroup.containsAll(segments));
    }

    // Create the expected mirroring servers.
    for (int instanceIndex = 0; instanceIndex < numInstancesPerPartition; instanceIndex++) {
      Set<Set<String>> mirroringServerSegments = new HashSet<>();
      for (int group = 0; group < NUM_REPLICA; group++) {
        List<String> serversInReplicaGroup = partitionToReplicaGroupMaping.getInstancesfromReplicaGroup(0, group);
        String server = serversInReplicaGroup.get(instanceIndex);
        mirroringServerSegments.add(serverToSegments.get(server));
      }
      Assert.assertEquals(mirroringServerSegments.size(), 1);
    }
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
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE)
        .setTableName(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP)
        .setNumReplicas(NUM_REPLICA)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy")
        .build();

    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);
    tableConfig.setIndexingConfig(indexingConfig);

    // This will trigger to build the partition to replica group mapping table.
    _pinotHelixResourceManager.addTable(tableConfig);

    // Wait for table addition
    while (!_pinotHelixResourceManager.hasOfflineTable(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP)) {
      Thread.sleep(100);
    }

    // Tracking segments that belong to a partition number.
    Map<Integer, Set<String>> partitionToSegment = new HashMap<>();

    // Upload segments
    for (int i = 0; i < numSegments; ++i) {
      int partitionNumber = i % totalPartitionNumber;
      String segmentName = "segment" + i;
      addOneSegmentWithPartitionInfo(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP, segmentName, PARTITION_COLUMN,
          partitionNumber);
      if (!partitionToSegment.containsKey(partitionNumber)) {
        partitionToSegment.put(partitionNumber, new HashSet<String>());
      }
      partitionToSegment.get(partitionNumber).add(segmentName);
    }

    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP, numSegments)) {
      Thread.sleep(100);
    }

    // Create a table of a list of segments that are assigned to a server.
    Map<String, Set<String>> serverToSegments = getServersToSegmentsMapping(TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP);

    // Fetch the replica group mapping table.
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixZkManager.getHelixPropertyStore();
    PartitionToReplicaGroupMappingZKMetadata partitionToReplicaGroupMapping =
        ZKMetadataProvider.getPartitionToReplicaGroupMappingZKMedata(propertyStore,
            TABLE_NAME_PARTITION_LEVEL_REPLICA_GROUP);

    // Check that each replica group for a partition contains all segments that belong to the partition.
    for (int partition = 0; partition < totalPartitionNumber; partition++) {
      for (int group = 0; group < NUM_REPLICA; group++) {
        List<String> serversInReplicaGroup =
            partitionToReplicaGroupMapping.getInstancesfromReplicaGroup(partition, group);
        List<String> segmentsInReplicaGroup = new ArrayList<>();
        for (String server : serversInReplicaGroup) {
          Set<String> segmentsInServer = serverToSegments.get(server);
          for (String segment : segmentsInServer) {
            if (partitionToSegment.get(partition).contains(segment)) {
              segmentsInReplicaGroup.add(segment);
            }
          }
        }
        Assert.assertEquals(segmentsInReplicaGroup.size(), partitionToSegment.get(partition).size());
        Assert.assertTrue(segmentsInReplicaGroup.containsAll(partitionToSegment.get(partition)));
      }
    }
  }

  private boolean allSegmentsPushedToIdealState(String tableName, int segmentNum) {
    IdealState idealState =
        _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    if (idealState != null && idealState.getPartitionSet() != null
        && idealState.getPartitionSet().size() == segmentNum) {
      return true;
    }
    return false;
  }

  private Map<String, Set<String>> getServersToSegmentsMapping(String tableName) {
    IdealState idealState =
        _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE.tableNameWithType(tableName));

    List<String> servers = _pinotHelixResourceManager.getServerInstancesForTable(tableName,
        CommonConstants.Helix.TableType.OFFLINE);
    Map<String, Set<String>> serverToSegments = new HashMap<>();

    for (String server : servers) {
      serverToSegments.put(server, new HashSet<String>());
    }

    for (String segment : idealState.getPartitionSet()) {
      for (String server : idealState.getInstanceStateMap(segment).keySet()) {
        serverToSegments.get(server).add(segment);
      }
    }
    return serverToSegments;
  }

  private void addOneSegment(String tableName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(tableName);
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotHelixResourceManager.addNewSegment(segmentMetadata, "downloadUrl");
  }

  private void addOneSegmentWithPartitionInfo(String tableName, String segmentName, String columnName,
      int partitionNumber) {
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    List<IntRange> partitionRanges = new ArrayList<>();
    partitionRanges.add(new IntRange(partitionNumber));
    when(columnMetadata.getPartitionRanges()).thenReturn(partitionRanges);

    SegmentMetadataImpl meta = mock(SegmentMetadataImpl.class);
    if (columnName != null) {
      when(meta.getColumnMetadataFor(columnName)).thenReturn(columnMetadata);
    }
    when(meta.getTableName()).thenReturn(tableName);
    when(meta.getName()).thenReturn(segmentName);
    when(meta.getCrc()).thenReturn("0");
    _pinotHelixResourceManager.addNewSegment(meta, "downloadUrl");
  }
}
