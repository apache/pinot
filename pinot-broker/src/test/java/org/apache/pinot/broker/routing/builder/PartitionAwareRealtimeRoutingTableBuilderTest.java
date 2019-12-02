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
package org.apache.pinot.broker.routing.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.routing.RoutingTableLookupRequest;
import org.apache.pinot.broker.util.FakePropertyStore;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.RoutingConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionAwareRealtimeRoutingTableBuilderTest {
  private static final String REALTIME_TABLE_NAME = "myTable_REALTIME";
  private static final String PARTITION_FUNCTION_NAME = "Modulo";
  private static final String PARTITION_COLUMN = "memberId";

  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final Random RANDOM = new Random();

  private int NUM_REPLICA;
  private int NUM_PARTITION;
  private int NUM_SERVERS;
  private int NUM_SEGMENTS;

  @Test
  public void testBrokerSideSegmentPruning()
      throws Exception {
    int numIterations = 50;

    for (int iter = 0; iter < numIterations; iter++) {
      NUM_PARTITION = RANDOM.nextInt(8) + 3;
      NUM_REPLICA = RANDOM.nextInt(3) + 3;
      NUM_SERVERS = RANDOM.nextInt(10) + 3;
      NUM_SEGMENTS = RANDOM.nextInt(100) + 3;

      // Create the fake property store
      FakePropertyStore fakePropertyStore = new FakePropertyStore();

      // Create the table config, partition mapping,
      TableConfig tableConfig = buildRealtimeTableConfig();

      Map<Integer, Integer> partitionSegmentCount = new HashMap<>();
      for (int i = 0; i < NUM_PARTITION; i++) {
        partitionSegmentCount.put(i, 0);
      }

      // Update segment zk metadata.
      List<String> segmentList = updateZkMetadataAndBuildSegmentList(partitionSegmentCount, fakePropertyStore);

      // Create instance Configs
      List<InstanceConfig> instanceConfigs = new ArrayList<>();
      for (int serverId = 0; serverId < NUM_SERVERS; serverId++) {
        String serverName = "Server_localhost_" + serverId;
        instanceConfigs.add(new InstanceConfig(serverName));
      }

      // Create stream partition assignment
      Map<Integer, List<String>> partitionToServerMapping = buildStreamPartitionMapping(instanceConfigs);

      // Create the fake external view
      ExternalView externalView =
          buildExternalView(REALTIME_TABLE_NAME, fakePropertyStore, partitionToServerMapping, segmentList);

      // Create the partition aware realtime routing table builder.
      RoutingTableBuilder routingTableBuilder =
          buildPartitionAwareRealtimeRoutingTableBuilder(fakePropertyStore, tableConfig, externalView, instanceConfigs);

      // Check the query that requires to scan all segment.
      String countStarQuery = "select count(*) from myTable";
      Map<ServerInstance, List<String>> routingTable =
          routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(countStarQuery), null);

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
        routingTable = routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(filterQuery), null);

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
  public void testMultipleConsumingSegments()
      throws Exception {
    NUM_PARTITION = 1;
    NUM_REPLICA = 1;
    NUM_SERVERS = 1;
    NUM_SEGMENTS = 10;
    int ONLINE_SEGMENTS = 8;

    // Create the fake property store
    FakePropertyStore fakePropertyStore = new FakePropertyStore();

    // Create the table config, partition mapping,
    TableConfig tableConfig = buildRealtimeTableConfig();

    Map<Integer, Integer> partitionSegmentCount = new HashMap<>();
    for (int i = 0; i < NUM_PARTITION; i++) {
      partitionSegmentCount.put(i, 0);
    }

    List<String> segmentList = updateZkMetadataAndBuildSegmentList(partitionSegmentCount, fakePropertyStore);

    // Create instance Configs
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    for (int serverId = 0; serverId < NUM_SERVERS; serverId++) {
      String serverName = "Server_localhost_" + serverId;
      instanceConfigs.add(new InstanceConfig(serverName));
    }

    // Create stream partition mapping
    Map<Integer, List<String>> partitionToServerMapping = buildStreamPartitionMapping(instanceConfigs);

    ExternalView externalView =
        buildExternalView(REALTIME_TABLE_NAME, fakePropertyStore, partitionToServerMapping, segmentList);

    LLCSegmentName consumingSegment = new LLCSegmentName(REALTIME_TABLE_NAME, 0, 9, 0);
    externalView.setState(consumingSegment.getSegmentName(), "Server_localhost_0", "CONSUMING");
    consumingSegment = new LLCSegmentName(REALTIME_TABLE_NAME, 0, 8, 0);
    externalView.setState(consumingSegment.getSegmentName(), "Server_localhost_0", "CONSUMING");

    // Create the partition aware realtime routing table builder.
    RoutingTableBuilder routingTableBuilder =
        buildPartitionAwareRealtimeRoutingTableBuilder(fakePropertyStore, tableConfig, externalView, instanceConfigs);

    // Check the query that requires to scan all segment.
    String countStarQuery = "select count(*) from myTable";
    Map<ServerInstance, List<String>> routingTable =
        routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(countStarQuery), null);

    // Check that all segments are covered exactly for once.
    Set<String> assignedSegments = new HashSet<>();
    for (List<String> segmentsForServer : routingTable.values()) {
      for (String segmentName : segmentsForServer) {
        Assert.assertFalse(assignedSegments.contains(segmentName));
        assignedSegments.add(segmentName);
      }
    }
    Assert.assertEquals(assignedSegments.size(), ONLINE_SEGMENTS + 1);

    // Check that only one consuming segment is assigned
    Assert.assertTrue(assignedSegments.contains(segmentList.get(ONLINE_SEGMENTS)));
    for (int i = ONLINE_SEGMENTS + 1; i < NUM_SEGMENTS; i++) {
      Assert.assertFalse(assignedSegments.contains(segmentList.get(i)));
    }
  }

  @Test
  public void testRoutingAfterRebalance()
      throws Exception {
    NUM_PARTITION = 10;
    NUM_REPLICA = 1;
    NUM_SERVERS = 1;
    NUM_SEGMENTS = 10;

    // Create the fake property store
    FakePropertyStore fakePropertyStore = new FakePropertyStore();

    // Create the table config, partition mapping,
    TableConfig tableConfig = buildRealtimeTableConfig();

    Map<Integer, Integer> partitionSegmentCount = new HashMap<>();
    for (int i = 0; i < NUM_PARTITION; i++) {
      partitionSegmentCount.put(i, 0);
    }

    List<String> segmentList = updateZkMetadataAndBuildSegmentList(partitionSegmentCount, fakePropertyStore);

    // Create instance Configs
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    for (int serverId = 0; serverId < NUM_SERVERS; serverId++) {
      String serverName = "Server_localhost_" + serverId;
      instanceConfigs.add(new InstanceConfig(serverName));
    }

    // Create stream partition mapping
    Map<Integer, List<String>> partitionToServerMapping = buildStreamPartitionMapping(instanceConfigs);

    ExternalView externalView =
        buildExternalView(REALTIME_TABLE_NAME, fakePropertyStore, partitionToServerMapping, segmentList);

    // Create the partition aware realtime routing table builder.
    RoutingTableBuilder routingTableBuilder =
        buildPartitionAwareRealtimeRoutingTableBuilder(fakePropertyStore, tableConfig, externalView, instanceConfigs);

    NUM_REPLICA = 2;
    NUM_SERVERS = 2;

    // Update instance configs
    String newServerName = "Server_localhost_" + (NUM_SERVERS - 1);
    instanceConfigs.add(new InstanceConfig(newServerName));

    // Update external view
    partitionToServerMapping = buildStreamPartitionMapping(instanceConfigs);
    ExternalView newExternalView =
        buildExternalView(REALTIME_TABLE_NAME, fakePropertyStore, partitionToServerMapping, segmentList);

    // Compute routing table
    routingTableBuilder.computeOnExternalViewChange(REALTIME_TABLE_NAME, newExternalView, instanceConfigs);

    Set<ServerInstance> servers = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      String countStarQuery = "select count(*) from " + REALTIME_TABLE_NAME;
      Map<ServerInstance, List<String>> routingTable =
          routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(countStarQuery), null);
      Assert.assertEquals(routingTable.keySet().size(), 1);
      servers.addAll(routingTable.keySet());
    }

    // Check if both servers are get picked
    Assert.assertEquals(servers.size(), 2);
  }

  private List<String> updateZkMetadataAndBuildSegmentList(Map<Integer, Integer> partitionSegmentCount,
      FakePropertyStore propertyStore)
      throws Exception {
    // Update segment zk metadata.
    List<String> segmentList = new ArrayList<>();
    int seqId = 0;
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      int partitionId = i % NUM_PARTITION;
      partitionSegmentCount.put(partitionId, partitionSegmentCount.get(partitionId) + 1);

      LLCSegmentName segment = new LLCSegmentName(REALTIME_TABLE_NAME, partitionId, seqId, 0);
      String segmentName = segment.getSegmentName();

      SegmentZKMetadata metadata = buildSegmentZKMetadata(segmentName, partitionId);
      propertyStore
          .setContents(ZKMetadataProvider.constructPropertyStorePathForSegment(REALTIME_TABLE_NAME, segmentName),
              metadata.toZNRecord());
      segmentList.add(segmentName);
      if (partitionId % NUM_PARTITION == 0) {
        seqId++;
      }
    }
    return segmentList;
  }

  private Map<Integer, List<String>> buildStreamPartitionMapping(List<InstanceConfig> instanceConfigs) {
    // Create partition assignment mapping table.
    Map<Integer, List<String>> partitionToServers = new HashMap<>();
    int serverIndex = 0;
    for (int partitionId = 0; partitionId < NUM_PARTITION; partitionId++) {
      List<String> assignedServers = new ArrayList<>();
      for (int replicaId = 0; replicaId < NUM_REPLICA; replicaId++) {
        assignedServers.add(instanceConfigs.get(serverIndex % NUM_SERVERS).getInstanceName());
        serverIndex++;
      }
      partitionToServers.put(partitionId, assignedServers);
    }
    return partitionToServers;
  }

  private RoutingTableBuilder buildPartitionAwareRealtimeRoutingTableBuilder(FakePropertyStore propertyStore,
      TableConfig tableConfig, ExternalView externalView, List<InstanceConfig> instanceConfigs) {

    PartitionAwareRealtimeRoutingTableBuilder routingTableBuilder = new PartitionAwareRealtimeRoutingTableBuilder();
    routingTableBuilder.init(null, tableConfig, propertyStore, null);
    routingTableBuilder.computeOnExternalViewChange(REALTIME_TABLE_NAME, externalView, instanceConfigs);

    return routingTableBuilder;
  }

  private ExternalView buildExternalView(String tableName, FakePropertyStore propertyStore,
      Map<Integer, List<String>> partitionToServerMapping, List<String> segmentList)
      throws Exception {

    // Create External View
    ExternalView externalView = new ExternalView(tableName);
    for (String segmentName : segmentList) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionId = llcSegmentName.getPartitionId();
      for (String server : partitionToServerMapping.get(partitionId)) {
        externalView.setState(segmentName, server, "ONLINE");
      }
    }
    return externalView;
  }

  private RoutingTableLookupRequest buildRoutingTableLookupRequest(String query) {
    return new RoutingTableLookupRequest(COMPILER.compileToBrokerRequest(query));
  }

  private TableConfig buildRealtimeTableConfig()
      throws Exception {
    // Create partition config
    Map<String, ColumnPartitionConfig> metadataMap = new HashMap<>();
    metadataMap.put(PARTITION_COLUMN, new ColumnPartitionConfig(PARTITION_FUNCTION_NAME, NUM_PARTITION));
    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(metadataMap);

    // Create the routing config
    RoutingConfig routingConfig = new RoutingConfig("PartitionAwareOffline", null);

    // Create table config
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(REALTIME_TABLE_NAME)
            .setNumReplicas(NUM_REPLICA).build();

    tableConfig.getValidationConfig().setReplicasPerPartition(Integer.toString(NUM_REPLICA));
    tableConfig.getIndexingConfig().setSegmentPartitionConfig(partitionConfig);
    tableConfig.setRoutingConfig(routingConfig);
    return tableConfig;
  }

  private SegmentZKMetadata buildSegmentZKMetadata(String segmentName, int partition) {
    LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
    Map<String, ColumnPartitionMetadata> columnPartitionMap = new HashMap<>();
    columnPartitionMap.put(PARTITION_COLUMN,
        new ColumnPartitionMetadata(PARTITION_FUNCTION_NAME, NUM_PARTITION, Collections.singleton(partition)));
    SegmentPartitionMetadata segmentPartitionMetadata = new SegmentPartitionMetadata(columnPartitionMap);

    metadata.setSegmentName(segmentName);
    metadata.setPartitionMetadata(segmentPartitionMetadata);
    metadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    return metadata;
  }
}
