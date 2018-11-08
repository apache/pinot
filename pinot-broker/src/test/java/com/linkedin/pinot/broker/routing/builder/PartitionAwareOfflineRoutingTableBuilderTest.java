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
package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.broker.routing.FakePropertyStore;
import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.RoutingConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.ColumnPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignment;
import com.linkedin.pinot.common.partition.ReplicaGroupPartitionAssignmentGenerator;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.math.IntRange;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionAwareOfflineRoutingTableBuilderTest {
  private static final String OFFLINE_TABLE_NAME = "myTable_OFFLINE";
  private static final String PARTITION_FUNCTION_NAME = "modulo";
  private static final String PARTITION_COLUMN = "memberId";

  private static final Pql2Compiler COMPILER = new Pql2Compiler();

  private static final Random RANDOM = new Random();

  private int NUM_REPLICA;
  private int NUM_PARTITION;
  private int NUM_SERVERS;
  private int NUM_SEGMENTS;

  @Test
  public void testBrokerSideServerAndSegmentPruning() throws Exception {
    int numIterations = 50;

    for (int iter = 0; iter < numIterations; iter++) {
      NUM_PARTITION = RANDOM.nextInt(8) + 3;
      NUM_REPLICA = RANDOM.nextInt(3) + 3;
      NUM_SERVERS = NUM_REPLICA * (RANDOM.nextInt(10) + 3);
      NUM_SEGMENTS = RANDOM.nextInt(100) + 3;

      // Create the fake property store
      FakePropertyStore fakePropertyStore = new FakePropertyStore();

      // Create the table config, partition mapping,
      TableConfig tableConfig = buildOfflineTableConfig();

      // Create the replica group id to server mapping
      Map<Integer, List<String>> replicaToServerMapping = buildReplicaGroupMapping();

      // Update segment zk metadata.
      Map<Integer, Integer> partitionSegmentCount = new HashMap<>();
      for (int i = 0; i < NUM_PARTITION; i++) {
        partitionSegmentCount.put(i, 0);
      }

      for (int i = 0; i < NUM_SEGMENTS; i++) {
        String segmentName = "segment" + i;
        int partition = i % NUM_PARTITION;
        partitionSegmentCount.put(partition, partitionSegmentCount.get(partition) + 1);

        SegmentZKMetadata metadata = buildOfflineSegmentZKMetadata(segmentName, partition);
        fakePropertyStore.setContents(
            ZKMetadataProvider.constructPropertyStorePathForSegment(OFFLINE_TABLE_NAME, segmentName),
            metadata.toZNRecord());
      }

      // Update replica group mapping zk metadata
      updateReplicaGroupPartitionAssignment(OFFLINE_TABLE_NAME, fakePropertyStore);

      // Create the fake external view
      ExternalView externalView = buildExternalView(OFFLINE_TABLE_NAME, replicaToServerMapping);

      // Create instance Configs
      List<InstanceConfig> instanceConfigs = new ArrayList<>();
      for (int serverId = 0; serverId < NUM_SERVERS; serverId++) {
        String serverName = "Server_localhost_" + serverId;
        instanceConfigs.add(new InstanceConfig(serverName));
      }

      // Create the partition aware offline routing table builder.
      RoutingTableBuilder routingTableBuilder =
          buildPartitionAwareOfflineRoutingTableBuilder(fakePropertyStore, tableConfig, externalView, instanceConfigs);

      // Check the query that requires to scan all segment.
      String countStarQuery = "select count(*) from myTable";
      Map<String, List<String>> routingTable =
          routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(countStarQuery));

      // Check that the number of servers picked are always equal or less than the number of servers
      // from a single replica group.
      Assert.assertTrue(routingTable.keySet().size() <= (NUM_SERVERS / NUM_REPLICA));

      // Check that all segments are covered exactly for once.
      Set<String> assignedSegments = new HashSet<>();
      for (List<String> segmentsForServer : routingTable.values()) {
        for (String segmentName : segmentsForServer) {
          Assert.assertFalse(assignedSegments.contains(segmentName));
          assignedSegments.add(segmentName);
        }
      }
      Assert.assertEquals(assignedSegments.size(), NUM_SEGMENTS);

      // Check the broker side server and segment pruning.
      for (int queryPartition = 0; queryPartition < 100; queryPartition++) {
        String filterQuery = "select count(*) from myTable where " + PARTITION_COLUMN + " = " + queryPartition;
        routingTable =
            routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(filterQuery));

        // Check that the number of servers picked are always equal or less than the number of servers
        // in a single replica group.
        Assert.assertTrue(routingTable.keySet().size() <= (NUM_SERVERS / NUM_REPLICA));

        int partition = queryPartition % NUM_PARTITION;
        assignedSegments = new HashSet<>();
        for (List<String> segmentsForServer : routingTable.values()) {
          for (String segmentName : segmentsForServer) {
            Assert.assertFalse(assignedSegments.contains(segmentName));
            assignedSegments.add(segmentName);
          }
        }
        Assert.assertEquals(assignedSegments.size(), partitionSegmentCount.get(partition).intValue());
      }
    }
  }

  @Test
  public void testRoutingTableAfterRebalance() throws Exception {
    NUM_REPLICA = 1;
    NUM_PARTITION = 1;
    NUM_SERVERS = 1;
    NUM_SEGMENTS = 10;

    // Create the fake property store
    FakePropertyStore fakePropertyStore = new FakePropertyStore();

    // Create the table config, partition mapping,
    TableConfig tableConfig = buildOfflineTableConfig();

    // Create the replica group id to server mapping
    Map<Integer, List<String>> replicaToServerMapping = buildReplicaGroupMapping();

    // Update segment zk metadata.
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "segment" + i;
      int partition = i % NUM_PARTITION;
      SegmentZKMetadata metadata = buildOfflineSegmentZKMetadata(segmentName, partition);
      fakePropertyStore.setContents(
          ZKMetadataProvider.constructPropertyStorePathForSegment(OFFLINE_TABLE_NAME, segmentName),
          metadata.toZNRecord());
    }

    // Update replica group mapping zk metadata
    updateReplicaGroupPartitionAssignment(OFFLINE_TABLE_NAME, fakePropertyStore);

    // Create the fake external view
    ExternalView externalView = buildExternalView(OFFLINE_TABLE_NAME, replicaToServerMapping);

    // Create instance Configs
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    for (int serverId = 0; serverId < NUM_SERVERS; serverId++) {
      String serverName = "Server_localhost_" + serverId;
      instanceConfigs.add(new InstanceConfig(serverName));
    }

    // Create the partition aware offline routing table builder.
    RoutingTableBuilder routingTableBuilder =
        buildPartitionAwareOfflineRoutingTableBuilder(fakePropertyStore, tableConfig, externalView, instanceConfigs);

    // Simulate the case where we add 1 more server/replica
    NUM_REPLICA = 2;
    NUM_SERVERS = 2;

    // Update instance configs
    String newServerName = "Server_localhost_" + (NUM_SERVERS - 1);
    instanceConfigs.add(new InstanceConfig(newServerName));

    // Update replica group partition assignment
    updateReplicaGroupPartitionAssignment(OFFLINE_TABLE_NAME, fakePropertyStore);

    // Update external view
    Map<Integer, List<String>> newReplicaToServerMapping = buildReplicaGroupMapping();
    ExternalView newExternalView = buildExternalView(OFFLINE_TABLE_NAME, newReplicaToServerMapping);

    // Compute routing table and this should not throw null pointer exception
    routingTableBuilder.computeOnExternalViewChange(OFFLINE_TABLE_NAME, newExternalView, instanceConfigs);

    Set<String> servers = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      String countStarQuery = "select count(*) from " + OFFLINE_TABLE_NAME;
      Map<String, List<String>> routingTable =
          routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(countStarQuery));
      Assert.assertEquals(routingTable.keySet().size(), 1);
      servers.add(routingTable.keySet().iterator().next());
    }

    // Check if both servers are get picked
    Assert.assertEquals(servers.size(), 2);
  }

  private void updateReplicaGroupPartitionAssignment(String tableNameWithType, FakePropertyStore propertyStore) {
    // Create partition assignment mapping table.
    ReplicaGroupPartitionAssignment replicaGroupPartitionAssignment =
        new ReplicaGroupPartitionAssignment(tableNameWithType);

    int partitionId = 0;
    for (int serverId = 0; serverId < NUM_SERVERS; serverId++) {
      String serverName = "Server_localhost_" + serverId;
      int replicaGroupId = serverId / (NUM_SERVERS / NUM_REPLICA);
      replicaGroupPartitionAssignment.addInstanceToReplicaGroup(partitionId, replicaGroupId, serverName);
    }

    // Write partition assignment to the property store.
    ReplicaGroupPartitionAssignmentGenerator partitionAssignmentGenerator =
        new ReplicaGroupPartitionAssignmentGenerator(propertyStore);
    partitionAssignmentGenerator.writeReplicaGroupPartitionAssignment(replicaGroupPartitionAssignment);
  }

  private RoutingTableBuilder buildPartitionAwareOfflineRoutingTableBuilder(FakePropertyStore propertyStore,
      TableConfig tableConfig, ExternalView externalView, List<InstanceConfig> instanceConfigs) throws Exception {
    PartitionAwareOfflineRoutingTableBuilder routingTableBuilder = new PartitionAwareOfflineRoutingTableBuilder();
    routingTableBuilder.init(null, tableConfig, propertyStore, null);
    routingTableBuilder.computeOnExternalViewChange(OFFLINE_TABLE_NAME, externalView, instanceConfigs);

    return routingTableBuilder;
  }

  private ExternalView buildExternalView(String tableName, Map<Integer, List<String>> replicaGroupServers)
      throws Exception {
    // Create External View
    ExternalView externalView = new ExternalView(tableName);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "segment" + i;
      int serverIndex = i % (NUM_SERVERS / NUM_REPLICA);
      for (List<String> serversInReplicaGroup : replicaGroupServers.values()) {
        externalView.setState(segmentName, serversInReplicaGroup.get(serverIndex), "ONLINE");
      }
    }
    return externalView;
  }

  private Map<Integer, List<String>> buildReplicaGroupMapping() {
    Map<Integer, List<String>> replicaGroupServers = new HashMap<>();
    int numServersPerReplica = NUM_SERVERS / NUM_REPLICA;
    for (int serverId = 0; serverId < NUM_SERVERS; serverId++) {
      int groupId = serverId / numServersPerReplica;
      if (!replicaGroupServers.containsKey(groupId)) {
        replicaGroupServers.put(groupId, new ArrayList<>());
      }
      String serverName = "Server_localhost_" + serverId;
      replicaGroupServers.get(groupId).add(serverName);
    }
    return replicaGroupServers;
  }

  private RoutingTableLookupRequest buildRoutingTableLookupRequest(String query) {
    return new RoutingTableLookupRequest(COMPILER.compileToBrokerRequest(query));
  }

  private TableConfig buildOfflineTableConfig() throws Exception {
    // Create the replica group aware assignment strategy config
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(NUM_PARTITION);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(true);

    // Create the routing config
    RoutingConfig routingConfig = new RoutingConfig();
    routingConfig.setRoutingTableBuilderName("PartitionAwareOffline");

    // Create table config
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME)
            .setNumReplicas(NUM_REPLICA)
            .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy")
            .build();

    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);
    tableConfig.setRoutingConfig(routingConfig);
    return tableConfig;
  }

  private SegmentZKMetadata buildOfflineSegmentZKMetadata(String segmentName, int partition) {
    OfflineSegmentZKMetadata metadata = new OfflineSegmentZKMetadata();
    Map<String, ColumnPartitionMetadata> columnPartitionMap = new HashMap<>();
    columnPartitionMap.put(PARTITION_COLUMN, new ColumnPartitionMetadata(PARTITION_FUNCTION_NAME, NUM_PARTITION,
        Collections.singletonList(new IntRange(partition))));
    SegmentPartitionMetadata segmentPartitionMetadata = new SegmentPartitionMetadata(columnPartitionMap);

    metadata.setSegmentName(segmentName);
    metadata.setPartitionMetadata(segmentPartitionMetadata);

    return metadata;
  }
}
