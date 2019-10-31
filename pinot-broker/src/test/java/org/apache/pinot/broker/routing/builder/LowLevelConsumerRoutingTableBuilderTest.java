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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Test for the Kafka low level consumer routing table builder.
 */
public class LowLevelConsumerRoutingTableBuilderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(LowLevelConsumerRoutingTableBuilderTest.class);

  @Test
  public void testAllOnlineRoutingTable() {
    final int ITERATIONS = 50;
    Random random = new Random();

    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName("tableName").build();
    LowLevelConsumerRoutingTableBuilder routingTableBuilder = new LowLevelConsumerRoutingTableBuilder();
    routingTableBuilder.init(new BaseConfiguration(), tableConfig, null, null);

    long totalNanos = 0L;

    for (int i = 0; i < ITERATIONS; i++) {
      int instanceCount = random.nextInt(12) + 3; // 3 to 14 instances
      int partitionCount = random.nextInt(8) + 4; // 4 to 11 partitions
      int replicationFactor = random.nextInt(3) + 3; // 3 to 5 replicas

      // Generate instances
      String[] instanceNames = new String[instanceCount];
      for (int serverInstanceId = 0; serverInstanceId < instanceCount; serverInstanceId++) {
        instanceNames[serverInstanceId] = "Server_localhost_" + serverInstanceId;
      }

      // Generate partitions
      String[][] segmentNames = new String[partitionCount][];
      int totalSegmentCount = 0;
      for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
        int segmentCount = random.nextInt(32); // 0 to 31 segments in partition
        segmentNames[partitionId] = new String[segmentCount];
        for (int sequenceNumber = 0; sequenceNumber < segmentCount; sequenceNumber++) {
          segmentNames[partitionId][sequenceNumber] =
              new LLCSegmentName("table", partitionId, sequenceNumber, System.currentTimeMillis()).getSegmentName();
        }
        totalSegmentCount += segmentCount;
      }

      // Generate instance configurations
      List<InstanceConfig> instanceConfigs = new ArrayList<InstanceConfig>();
      for (String instanceName : instanceNames) {
        InstanceConfig instanceConfig = new InstanceConfig(instanceName);
        instanceConfigs.add(instanceConfig);
        instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, "false");
      }

      // Generate a random external view
      ExternalView externalView = new ExternalView("table_REALTIME");
      int[] segmentCountForInstance = new int[instanceCount];
      int maxSegmentCountOnInstance = 0;
      for (int partitionId = 0; partitionId < segmentNames.length; partitionId++) {
        String[] segments = segmentNames[partitionId];

        // Assign each segment for this partition
        for (int replicaId = 0; replicaId < replicationFactor; ++replicaId) {
          for (int segmentIndex = 0; segmentIndex < segments.length; segmentIndex++) {
            int instanceIndex = -1;
            int randomOffset = random.nextInt(instanceCount);

            // Pick the first random instance that has fewer than maxSegmentCountOnInstance segments assigned to it
            for (int j = 0; j < instanceCount; j++) {
              int potentialInstanceIndex = (j + randomOffset) % instanceCount;
              if (segmentCountForInstance[potentialInstanceIndex] < maxSegmentCountOnInstance) {
                instanceIndex = potentialInstanceIndex;
                break;
              }
            }

            // All replicas have exactly maxSegmentCountOnInstance, pick a replica and increment the max
            if (instanceIndex == -1) {
              maxSegmentCountOnInstance++;
              instanceIndex = randomOffset;
            }

            // Increment the segment count for the instance
            segmentCountForInstance[instanceIndex]++;

            // Add the segment to the external view
            externalView.setState(segmentNames[partitionId][segmentIndex], instanceNames[instanceIndex], "ONLINE");
          }
        }
      }

      // Create routing tables
      long startTime = System.nanoTime();
      routingTableBuilder.computeOnExternalViewChange("table_REALTIME", externalView, instanceConfigs);

      List<Map<String, List<String>>> routingTables = routingTableBuilder.getRoutingTables();

      long endTime = System.nanoTime();
      totalNanos += endTime - startTime;

      // Check that all routing tables generated match all segments, with no duplicates
      for (Map<String, List<String>> routingTable : routingTables) {
        Set<String> assignedSegments = new HashSet<>();

        for (List<String> segmentsForServer : routingTable.values()) {
          for (String segment : segmentsForServer) {
            assertFalse(assignedSegments.contains(segment));
            assignedSegments.add(segment);
          }
        }

        assertEquals(assignedSegments.size(), totalSegmentCount);
      }
    }

    LOGGER.warn("Routing table building avg ms: " + totalNanos / (ITERATIONS * 1000000.0));
  }

  @Test
  public void testMultipleConsumingSegments() {
    String rawTableName = "testTable";
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    String instance1 = "Server_localhost_1234";
    String instance2 = "Server_localhost_5678";
    int numSegments = 10;
    int numConsumingSegments = 2;
    int numOnlineSegments = numSegments - numConsumingSegments;

    LowLevelConsumerRoutingTableBuilder routingTableBuilder = new LowLevelConsumerRoutingTableBuilder();
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(rawTableName).build();
    routingTableBuilder.init(new BaseConfiguration(), tableConfig, null, null);

    List<InstanceConfig> instanceConfigs = Arrays.asList(new InstanceConfig(instance1), new InstanceConfig(instance2));

    List<String> segments = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      segments.add(new LLCSegmentName(rawTableName, 0, i, System.currentTimeMillis()).getSegmentName());
    }

    // Generate an external view for two servers with some CONSUMING segments
    ExternalView externalView = new ExternalView(realtimeTableName);
    for (int i = 0; i < numOnlineSegments; i++) {
      String segmentName = segments.get(i);
      externalView.setState(segmentName, instance1, RealtimeSegmentOnlineOfflineStateModel.ONLINE);
      externalView.setState(segmentName, instance2, RealtimeSegmentOnlineOfflineStateModel.ONLINE);
    }
    // The first CONSUMING segment has one instance in ONLINE state and the other in CONSUMING state (only one instance
    // finished the CONSUMING -> ONLINE state transition)
    String consumingSegment1 = segments.get(numOnlineSegments);
    externalView.setState(consumingSegment1, instance1, RealtimeSegmentOnlineOfflineStateModel.ONLINE);
    externalView.setState(consumingSegment1, instance2, RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
    // The second CONSUMING segment has both instances in CONSUMING state
    String consumingSegment2 = segments.get(numOnlineSegments + 1);
    externalView.setState(consumingSegment2, instance1, RealtimeSegmentOnlineOfflineStateModel.CONSUMING);
    externalView.setState(consumingSegment2, instance2, RealtimeSegmentOnlineOfflineStateModel.CONSUMING);

    routingTableBuilder.computeOnExternalViewChange(realtimeTableName, externalView, instanceConfigs);
    List<Map<String, List<String>>> routingTables = routingTableBuilder.getRoutingTables();
    for (Map<String, List<String>> routingTable : routingTables) {
      ArrayList<String> segmentsInRoutingTable = new ArrayList<>();
      for (List<String> segmentsForServer : routingTable.values()) {
        segmentsInRoutingTable.addAll(segmentsForServer);
      }
      assertEquals(segmentsInRoutingTable.size(), numOnlineSegments + 1);

      // Should only contain the first consuming segment, not the second
      assertTrue(segmentsInRoutingTable.contains(segments.get(numOnlineSegments)),
          "Segment set does not contain the first segment in consuming state");
      assertFalse(segmentsInRoutingTable.contains(segments.get(numOnlineSegments + 1)),
          "Segment set contains a segment in consuming state that should not be there");
    }
  }

  @Test
  public void testShutdownInProgressServer() {
    final int SEGMENT_COUNT = 10;
    final int ONLINE_SEGMENT_COUNT = 8;
    final String rawTableName = "table";
    final String realtimeTableName = TableNameBuilder.forType(CommonConstants.Helix.TableType.REALTIME).
        tableNameWithType(rawTableName);
    final String serverInstanceName = "Server_localhost_1234";

    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.REALTIME).setTableName(rawTableName).build();
    LowLevelConsumerRoutingTableBuilder routingTableBuilder = new LowLevelConsumerRoutingTableBuilder();
    routingTableBuilder.init(new BaseConfiguration(), tableConfig, null, null);

    List<SegmentName> segmentNames = new ArrayList<>();
    for (int i = 0; i < SEGMENT_COUNT; ++i) {
      segmentNames.add(new LLCSegmentName(rawTableName, 0, i, System.currentTimeMillis()));
    }

    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    final InstanceConfig instanceConfig = new InstanceConfig(serverInstanceName);
    instanceConfigs.add(instanceConfig);
    instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, "true");

    // Generate an external view for a single server with some consuming segments
    ExternalView externalView = new ExternalView(realtimeTableName);
    for (int i = 0; i < ONLINE_SEGMENT_COUNT; i++) {
      externalView.setState(segmentNames.get(i).getSegmentName(), serverInstanceName, "ONLINE");
    }
    for (int i = ONLINE_SEGMENT_COUNT; i < SEGMENT_COUNT; ++i) {
      externalView.setState(segmentNames.get(i).getSegmentName(), serverInstanceName, "CONSUMING");
    }

    routingTableBuilder.computeOnExternalViewChange(rawTableName, externalView, instanceConfigs);
    List<Map<String, List<String>>> routingTables = routingTableBuilder.getRoutingTables();
    for (Map<String, List<String>> routingTable : routingTables) {
      Assert.assertTrue(routingTable.isEmpty());
    }

    instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, "false");
    routingTableBuilder.computeOnExternalViewChange(rawTableName, externalView, instanceConfigs);
    routingTables = routingTableBuilder.getRoutingTables();
    for (Map<String, List<String>> routingTable : routingTables) {
      Assert.assertFalse(routingTable.isEmpty());
    }

    instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.QUERIES_DISABLED, "true");
    routingTableBuilder.computeOnExternalViewChange(rawTableName, externalView, instanceConfigs);
    routingTables = routingTableBuilder.getRoutingTables();
    for (Map<String, List<String>> routingTable : routingTables) {
      Assert.assertTrue(routingTable.isEmpty());
    }
  }
}
