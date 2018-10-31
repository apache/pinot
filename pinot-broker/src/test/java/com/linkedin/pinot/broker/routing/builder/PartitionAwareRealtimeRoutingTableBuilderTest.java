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

package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.broker.routing.FakePropertyStore;
import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.ColumnPartitionConfig;
import com.linkedin.pinot.common.config.RoutingConfig;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.ColumnPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentPartitionMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
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
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
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
  public void testBrokerSideSegmentPruning() throws Exception {
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
      for (int serverId = 0; serverId <= NUM_SERVERS; serverId++) {
        String serverName = "Server_localhost_" + serverId;
        instanceConfigs.add(new InstanceConfig(serverName));
      }

      // Update replica group mapping zk metadata
      Map<Integer, List<String>> partitionToServerMapping =
          buildKafkaPartitionMapping(REALTIME_TABLE_NAME, fakePropertyStore, instanceConfigs);

      // Create the fake external view
      ExternalView externalView =
          buildExternalView(REALTIME_TABLE_NAME, fakePropertyStore, partitionToServerMapping, segmentList);


      // Create the partition aware realtime routing table builder.
      RoutingTableBuilder routingTableBuilder =
          buildPartitionAwareRealtimeRoutingTableBuilder(fakePropertyStore, tableConfig, externalView, instanceConfigs);

      // Check the query that requires to scan all segment.
      String countStarQuery = "select count(*) from myTable";
      Map<String, List<String>> routingTable =
          routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(countStarQuery));

      // Check that all segments are covered exactly for once.
      Set<String> assignedSegments = new HashSet<>();
      for (List<String> segmentsForServer : routingTable.values()) {
        for (String segmentName: segmentsForServer) {
          Assert.assertFalse(assignedSegments.contains(segmentName));
          assignedSegments.add(segmentName);
        }
      }
      Assert.assertEquals(assignedSegments.size(), NUM_SEGMENTS);

      // Check the broker side server and segment pruning.
      for (int queryPartition = 0; queryPartition < 100; queryPartition++) {
        String filterQuery = "select count(*) from myTable where " + PARTITION_COLUMN + " = " + queryPartition;
        routingTable = routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(filterQuery));

        int partition = queryPartition % NUM_PARTITION;
        assignedSegments = new HashSet<>();
        for (List<String> segmentsForServer : routingTable.values()) {
          for (String segmentName: segmentsForServer) {
            Assert.assertFalse(assignedSegments.contains(segmentName));
            assignedSegments.add(segmentName);
          }
        }
        Assert.assertEquals(assignedSegments.size(), partitionSegmentCount.get(partition).intValue());
      }
    }
  }

  @Test
  public void testMultipleConsumingSegments() throws Exception {
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
    for (int serverId = 0; serverId <= NUM_SERVERS; serverId++) {
      String serverName = "Server_localhost_" + serverId;
      instanceConfigs.add(new InstanceConfig(serverName));
    }

    // Update replica group mapping zk metadata
    Map<Integer, List<String>> partitionToServerMapping =
        buildKafkaPartitionMapping(REALTIME_TABLE_NAME, fakePropertyStore, instanceConfigs);

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
    Map<String, List<String>> routingTable =
        routingTableBuilder.getRoutingTable(buildRoutingTableLookupRequest(countStarQuery));

    // Check that all segments are covered exactly for once.
    Set<String> assignedSegments = new HashSet<>();
    for (List<String> segmentsForServer : routingTable.values()) {
      for (String segmentName: segmentsForServer) {
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

  private List<String> updateZkMetadataAndBuildSegmentList(Map<Integer, Integer> partitionSegmentCount,
      FakePropertyStore propertyStore) throws Exception {
    // Update segment zk metadata.
    List<String> segmentList = new ArrayList<>();
    int seqId = 0;
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      int partitionId = i % NUM_PARTITION;
      partitionSegmentCount.put(partitionId, partitionSegmentCount.get(partitionId) + 1);

      LLCSegmentName segment = new LLCSegmentName(REALTIME_TABLE_NAME, partitionId, seqId, 0);
      String segmentName = segment.getSegmentName();

      SegmentZKMetadata metadata = buildSegmentZKMetadata(segmentName, partitionId);
      propertyStore.setContents(
          ZKMetadataProvider.constructPropertyStorePathForSegment(REALTIME_TABLE_NAME, segmentName),
          metadata.toZNRecord());
      segmentList.add(segmentName);
      if (partitionId % NUM_PARTITION == 0) {
        seqId++;
      }
    }
    return segmentList;
  }

  private Map<Integer, List<String>> buildKafkaPartitionMapping(String tableName, FakePropertyStore propertyStore,
      List<InstanceConfig> instanceConfigs) {
    // Create partition assignment mapping table.
    Map<Integer, List<String>> partitionToServers = new HashMap<>();
    ZNRecord kafkaPartitionMapping = new ZNRecord(REALTIME_TABLE_NAME);

    int serverIndex = 0;
    for (int partitionId = 0; partitionId < NUM_PARTITION; partitionId++) {
      List<String> assignedServers = new ArrayList<>();
      for (int replicaId = 0; replicaId < NUM_REPLICA; replicaId++) {
        assignedServers.add(instanceConfigs.get(serverIndex % NUM_SERVERS).getInstanceName());
        serverIndex++;
      }
      partitionToServers.put(partitionId, assignedServers);
      kafkaPartitionMapping.setListField(Integer.toString(partitionId), assignedServers);
    }

    String path = ZKMetadataProvider.constructPropertyStorePathForKafkaPartitions(tableName);
    propertyStore.set(path, kafkaPartitionMapping, AccessOption.PERSISTENT);

    return partitionToServers;
  }

  private RoutingTableBuilder buildPartitionAwareRealtimeRoutingTableBuilder(FakePropertyStore propertyStore,
      TableConfig tableConfig, ExternalView externalView, List<InstanceConfig> instanceConfigs) throws Exception{

    PartitionAwareRealtimeRoutingTableBuilder routingTableBuilder = new PartitionAwareRealtimeRoutingTableBuilder();
    routingTableBuilder.init(null, tableConfig, propertyStore);
    routingTableBuilder.computeRoutingTableFromExternalView(REALTIME_TABLE_NAME, externalView, instanceConfigs);

    return routingTableBuilder;
  }

  private ExternalView buildExternalView(String tableName, FakePropertyStore propertyStore,
      Map<Integer, List<String>> partitionToServerMapping, List<String> segmentList) throws Exception {

    // Create External View
    ExternalView externalView = new ExternalView(tableName);
    for (String segmentName: segmentList) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionId = llcSegmentName.getPartitionId();
      for (String server: partitionToServerMapping.get(partitionId)) {
        externalView.setState(segmentName, server, "ONLINE");
      }
    }
    return externalView;
  }

  private RoutingTableLookupRequest buildRoutingTableLookupRequest(String query) {
    return new RoutingTableLookupRequest(COMPILER.compileToBrokerRequest(query));
  }

  private TableConfig buildRealtimeTableConfig() throws Exception {
    // Create partition config
    Map<String, ColumnPartitionConfig> metadataMap = new HashMap<>();
    metadataMap.put(PARTITION_COLUMN, new ColumnPartitionConfig(PARTITION_FUNCTION_NAME, NUM_PARTITION));
    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(metadataMap);

    // Create the routing config
    RoutingConfig routingConfig = new RoutingConfig();
    routingConfig.setRoutingTableBuilderName("PartitionAwareOffline");

    // Create table config
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME)
        .setTableName(REALTIME_TABLE_NAME)
        .setNumReplicas(NUM_REPLICA)
        .build();

    tableConfig.getValidationConfig().setReplicasPerPartition(Integer.toString(NUM_REPLICA));
    tableConfig.getIndexingConfig().setSegmentPartitionConfig(partitionConfig);
    tableConfig.setRoutingConfig(routingConfig);
    return tableConfig;
  }

  private SegmentZKMetadata buildSegmentZKMetadata(String segmentName, int partition) {
    LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
    Map<String, ColumnPartitionMetadata> columnPartitionMap = new HashMap<>();
    columnPartitionMap.put(PARTITION_COLUMN, new ColumnPartitionMetadata(PARTITION_FUNCTION_NAME, NUM_PARTITION,
        Collections.singletonList(new IntRange(partition))));
    SegmentPartitionMetadata segmentPartitionMetadata = new SegmentPartitionMetadata(columnPartitionMap);

    metadata.setSegmentName(segmentName);
    metadata.setPartitionMetadata(segmentPartitionMetadata);
    metadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
    return metadata;
  }

}
