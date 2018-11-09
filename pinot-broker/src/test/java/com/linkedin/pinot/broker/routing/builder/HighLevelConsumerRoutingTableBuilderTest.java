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

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.HLCSegmentName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HighLevelConsumerRoutingTableBuilderTest {

  public static final String ALL_PARTITIONS = "ALL";

  @Test
  public void testHlcRoutingTableBuilder() {
    final int ITERATIONS = 50;
    final int MAX_INSTANCES_PER_GROUP = 5;
    final int MAX_NUM_GROUPS = 5;
    final int MAX_NUM_SEGMENTS = 100;

    Random random = new Random();

    HighLevelConsumerBasedRoutingTableBuilder routingTableBuilder =
        new HighLevelConsumerBasedRoutingTableBuilder();
    routingTableBuilder.init(new BaseConfiguration(), new TableConfig(), null, null);

    String tableNameWithType = "table_REALTIME";
    String groupPrefix = "table_REALTIME_" + System.currentTimeMillis();

    // Generate instances
    for (int i = 0; i < ITERATIONS; i++) {
      int numInstancesPerGroup = random.nextInt(MAX_INSTANCES_PER_GROUP) + 1;
      int numGroups = random.nextInt(MAX_NUM_GROUPS) + 1;
      int numSegments = random.nextInt(MAX_NUM_SEGMENTS) + 5;
      int instanceCount = numInstancesPerGroup * numGroups;

      // Generate instance names
      String[] instanceNames = new String[instanceCount];
      for (int serverInstanceId = 0; serverInstanceId < instanceCount; serverInstanceId++) {
        instanceNames[serverInstanceId] = "Server_localhost_" + serverInstanceId;
      }

      // Generate instance configurations
      List<InstanceConfig> instanceConfigs = new ArrayList<>();
      for (String instanceName : instanceNames) {
        InstanceConfig instanceConfig = new InstanceConfig(instanceName);
        instanceConfigs.add(instanceConfig);
        instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, "false");
      }

      // Generate fake external view
      ExternalView externalView = new ExternalView(tableNameWithType);
      Map<String, Set<String>> groupIdToSegmentsMapping = new HashMap<>();
      for (int segmentNum = 0; segmentNum < numSegments; segmentNum++) {
        int groupId = segmentNum % numGroups;
        int groupOffset = segmentNum % numInstancesPerGroup;
        String instanceName = instanceNames[groupId * numInstancesPerGroup + groupOffset];
        String groupName = groupPrefix + "_" + groupId;
        String segmentName = new HLCSegmentName(groupName, ALL_PARTITIONS, String.valueOf(segmentNum)).getSegmentName();
        externalView.setState(segmentName, instanceName, "ONLINE");

        Set<String> segmentsForGroupId = groupIdToSegmentsMapping.computeIfAbsent(groupName, key -> new HashSet<>());
        segmentsForGroupId.add(segmentName);
      }

      // Compute routing table
      routingTableBuilder.computeOnExternalViewChange(tableNameWithType, externalView, instanceConfigs);

      // Check if the routing table result is correct
      for (int run = 0; run < MAX_NUM_GROUPS * 10; run++) {
        RoutingTableLookupRequest request = new RoutingTableLookupRequest(tableNameWithType);
        Map<String, List<String>> routingTable = routingTableBuilder.getRoutingTable(request);
        Set<String> coveredSegments = new HashSet<>();
        for (List<String> segmentsForServer : routingTable.values()) {
          coveredSegments.addAll(segmentsForServer);
        }

        boolean match = false;
        for (Set<String> expectedResult : groupIdToSegmentsMapping.values()) {
          if (coveredSegments.equals(expectedResult)) {
            match = true;
          }
        }
        Assert.assertTrue(match);
      }
    }
  }
}
