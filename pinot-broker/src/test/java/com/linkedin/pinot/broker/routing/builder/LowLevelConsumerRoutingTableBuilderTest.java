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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Test for the Kafka low level consumer routing table builder.
 */
public class LowLevelConsumerRoutingTableBuilderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(LowLevelConsumerRoutingTableBuilderTest.class);

  @Test
  public void testAllOnlineRoutingTable() {
    final int ITERATIONS = 50;
    Random random = new Random();

    LowLevelConsumerRoutingTableBuilder routingTableBuilder = new LowLevelConsumerRoutingTableBuilder();
    routingTableBuilder.init(new BaseConfiguration(), new TableConfig(), null, null);

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
          segmentNames[partitionId][sequenceNumber] = new LLCSegmentName("table", partitionId, sequenceNumber,
              System.currentTimeMillis()).getSegmentName();
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
      routingTableBuilder.computeOnExternalViewChange(
          "table_REALTIME", externalView, instanceConfigs);

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
    final int SEGMENT_COUNT = 10;
    final int ONLINE_SEGMENT_COUNT = 8;
    final int CONSUMING_SEGMENT_COUNT = SEGMENT_COUNT - ONLINE_SEGMENT_COUNT;

    LowLevelConsumerRoutingTableBuilder routingTableBuilder = new LowLevelConsumerRoutingTableBuilder();
    routingTableBuilder.init(new BaseConfiguration(), new TableConfig(), null, null);

    List<SegmentName> segmentNames = new ArrayList<>();
    for(int i = 0; i < SEGMENT_COUNT; ++i) {
      segmentNames.add(new LLCSegmentName("table", 0, i, System.currentTimeMillis()));
    }

    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig instanceConfig = new InstanceConfig("Server_localhost_1234");
    instanceConfigs.add(instanceConfig);
    instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, "false");

    // Generate an external view for a single server with some consuming segments
    ExternalView externalView = new ExternalView("table_REALTIME");
    for (int i = 0; i < ONLINE_SEGMENT_COUNT; i++) {
      externalView.setState(segmentNames.get(i).getSegmentName(), "Server_localhost_1234", "ONLINE");
    }
    for (int i = ONLINE_SEGMENT_COUNT; i < SEGMENT_COUNT; ++i) {
      externalView.setState(segmentNames.get(i).getSegmentName(), "Server_localhost_1234", "CONSUMING");
    }

    routingTableBuilder.computeOnExternalViewChange("table", externalView, instanceConfigs);
    List<Map<String, List<String>>> routingTables = routingTableBuilder.getRoutingTables();
    for (Map<String, List<String>> routingTable : routingTables) {
      for (List<String> segmentsForServer : routingTable.values()) {
        assertEquals(segmentsForServer.size(), ONLINE_SEGMENT_COUNT + 1);

        // Should only contain the first consuming segment, not the second
        assertTrue(segmentsForServer.contains(segmentNames.get(ONLINE_SEGMENT_COUNT).getSegmentName()),
            "Segment set does not contain the first segment in consuming state");
        for (int i = ONLINE_SEGMENT_COUNT + 1; i < SEGMENT_COUNT; i++) {
          assertFalse(segmentsForServer.contains(segmentNames.get(i).getSegmentName()),
              "Segment set contains a segment in consuming state that should not be there");
        }
      }
    }
  }

  @Test
  public void testShutdownInProgressServer() {
    final int SEGMENT_COUNT = 10;
    final int ONLINE_SEGMENT_COUNT = 8;

    LowLevelConsumerRoutingTableBuilder routingTableBuilder = new LowLevelConsumerRoutingTableBuilder();
    routingTableBuilder.init(new BaseConfiguration(), new TableConfig(), null, null);

    List<SegmentName> segmentNames = new ArrayList<>();
    for(int i = 0; i < SEGMENT_COUNT; ++i) {
      segmentNames.add(new LLCSegmentName("table", 0, i, System.currentTimeMillis()));
    }

    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig instanceConfig = new InstanceConfig("Server_localhost_1234");
    instanceConfigs.add(instanceConfig);
    instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, "true");

    // Generate an external view for a single server with some consuming segments
    ExternalView externalView = new ExternalView("table_REALTIME");
    for (int i = 0; i < ONLINE_SEGMENT_COUNT; i++) {
      externalView.setState(segmentNames.get(i).getSegmentName(), "Server_localhost_1234", "ONLINE");
    }
    for (int i = ONLINE_SEGMENT_COUNT; i < SEGMENT_COUNT; ++i) {
      externalView.setState(segmentNames.get(i).getSegmentName(), "Server_localhost_1234", "CONSUMING");
    }

    routingTableBuilder.computeOnExternalViewChange("table", externalView, instanceConfigs);
    List<Map<String, List<String>>> routingTables = routingTableBuilder.getRoutingTables();
    for (Map<String, List<String>> routingTable : routingTables) {
      Assert.assertTrue(routingTable.isEmpty());
    }
  }
}
