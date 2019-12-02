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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.transport.ServerInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/**
 * Test for the large cluster routing table builder.
 */
public class LargeClusterRoutingTableBuilderTest {
  private static final boolean EXHAUSTIVE = false;
  private LargeClusterRoutingTableBuilder _largeClusterRoutingTableBuilder = new LargeClusterRoutingTableBuilder();

  private interface RoutingTableValidator {
    boolean isRoutingTableValid(Map<ServerInstance, List<String>> routingTable, ExternalView externalView,
        List<InstanceConfig> instanceConfigs);
  }

  @Test
  public void testRoutingTableCoversAllSegmentsExactlyOnce() {
    validateAssertionOverMultipleRoutingTables(new RoutingTableValidator() {
      @Override
      public boolean isRoutingTableValid(Map<ServerInstance, List<String>> routingTable, ExternalView externalView,
          List<InstanceConfig> instanceConfigs) {
        Set<String> unassignedSegments = new HashSet<>();
        unassignedSegments.addAll(externalView.getPartitionSet());

        for (List<String> segmentsForServer : routingTable.values()) {
          if (!unassignedSegments.containsAll(segmentsForServer)) {
            // A segment is already assigned to another server and/or doesn't exist in external view
            return false;
          }

          unassignedSegments.removeAll(segmentsForServer);
        }

        return unassignedSegments.isEmpty();
      }
    }, "Routing table should contain all segments exactly once");
  }

  @Test
  public void testRoutingTableExcludesDisabledAndRebootingInstances() {
    final String tableName = "fakeTable_OFFLINE";
    final int segmentCount = 100;
    final int replicationFactor = 6;
    final int instanceCount = 50;

    ExternalView externalView = createExternalView(tableName, segmentCount, replicationFactor, instanceCount);
    List<InstanceConfig> instanceConfigs = createInstanceConfigs(instanceCount);

    InstanceConfig helixDisabledInstanceConfig = instanceConfigs.get(0);
    helixDisabledInstanceConfig.setInstanceEnabled(false);
    ServerInstance helixDisabledInstance = new ServerInstance(helixDisabledInstanceConfig);

    InstanceConfig shuttingDownInstanceConfig = instanceConfigs.get(1);
    shuttingDownInstanceConfig.getRecord()
        .setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, Boolean.toString(true));
    ServerInstance shuttingDownInstance = new ServerInstance(shuttingDownInstanceConfig);

    InstanceConfig queriesDisabledInstanceConfig = instanceConfigs.get(2);
    queriesDisabledInstanceConfig.getRecord()
        .setSimpleField(CommonConstants.Helix.QUERIES_DISABLED, Boolean.toString(true));
    ServerInstance queriesDisabledInstance = new ServerInstance(queriesDisabledInstanceConfig);

    validateAssertionForOneRoutingTable(new RoutingTableValidator() {
      @Override
      public boolean isRoutingTableValid(Map<ServerInstance, List<String>> routingTable, ExternalView externalView,
          List<InstanceConfig> instanceConfigs) {
        for (ServerInstance serverInstance : routingTable.keySet()) {
          // These servers should not appear in the routing table
          if (serverInstance.equals(helixDisabledInstance) || serverInstance.equals(shuttingDownInstance)
              || serverInstance.equals(queriesDisabledInstance)) {
            return false;
          }
        }

        return true;
      }
    }, "Routing table should not contain disabled instances", externalView, instanceConfigs, tableName);
  }

  @Test
  public void testRoutingTableSizeGenerallyHasConfiguredServerCount() {
    final String tableName = "fakeTable_OFFLINE";
    final int segmentCount = 100;
    final int replicationFactor = 10;
    final int instanceCount = 50;
    final int desiredServerCount = 20;

    ExternalView externalView = createExternalView(tableName, segmentCount, replicationFactor, instanceCount);
    List<InstanceConfig> instanceConfigs = createInstanceConfigs(instanceCount);

    _largeClusterRoutingTableBuilder.computeOnExternalViewChange(tableName, externalView, instanceConfigs);

    List<Map<ServerInstance, List<String>>> routingTables = _largeClusterRoutingTableBuilder.getRoutingTables();

    int routingTableCount = 0;
    int largerThanDesiredRoutingTableCount = 0;

    for (Map<ServerInstance, List<String>> routingTable : routingTables) {
      routingTableCount++;
      if (desiredServerCount < routingTable.size()) {
        largerThanDesiredRoutingTableCount++;
      }
    }

    assertTrue(largerThanDesiredRoutingTableCount / 0.6 < routingTableCount,
        "More than 60% of routing tables exceed the desired routing table size");
  }

  @Test
  public void testRoutingTableServerLoadIsRelativelyEqual() {
    final String tableName = "fakeTable_OFFLINE";
    final int segmentCount = 300;
    final int replicationFactor = 10;
    final int instanceCount = 50;
    final int maxNumIterations = 5;   // max times we try to satisfy the load criteria. after that we fail the test
    boolean done = false;
    int iterationCount = 1;

    while (!done) {
      ExternalView externalView = createExternalView(tableName, segmentCount, replicationFactor, instanceCount);
      List<InstanceConfig> instanceConfigs = createInstanceConfigs(instanceCount);

      _largeClusterRoutingTableBuilder.computeOnExternalViewChange(tableName, externalView, instanceConfigs);

      List<Map<ServerInstance, List<String>>> routingTables = _largeClusterRoutingTableBuilder.getRoutingTables();

      Map<ServerInstance, Integer> segmentCountPerServer = new HashMap<>();

      // Count number of segments assigned per server
      for (Map<ServerInstance, List<String>> routingTable : routingTables) {
        for (Map.Entry<ServerInstance, List<String>> entry : routingTable.entrySet()) {
          ServerInstance serverInstance = entry.getKey();
          Integer numSegmentsForServer = segmentCountPerServer.get(serverInstance);

          if (numSegmentsForServer == null) {
            numSegmentsForServer = 0;
          }

          numSegmentsForServer += entry.getValue().size();
          segmentCountPerServer.put(serverInstance, numSegmentsForServer);
        }
      }

      int minNumberOfSegmentsAssignedPerServer = Integer.MAX_VALUE;
      int maxNumberOfSegmentsAssignedPerServer = 0;

      for (Integer segmentCountForServer : segmentCountPerServer.values()) {
        if (segmentCountForServer < minNumberOfSegmentsAssignedPerServer) {
          minNumberOfSegmentsAssignedPerServer = segmentCountForServer;
        }

        if (maxNumberOfSegmentsAssignedPerServer < segmentCountForServer) {
          maxNumberOfSegmentsAssignedPerServer = segmentCountForServer;
        }
      }

      if (maxNumberOfSegmentsAssignedPerServer < minNumberOfSegmentsAssignedPerServer * 1.5) {
        done = true;
      } else {
        if (iterationCount++ >= maxNumIterations) {
          Assert.fail(
              "At least one server has more than 150% of the load of the least loaded server, minNumberOfSegmentsAssignedPerServer = "
                  + minNumberOfSegmentsAssignedPerServer + " maxNumberOfSegmentsAssignedPerServer = "
                  + maxNumberOfSegmentsAssignedPerServer);
        }
      }
    }
  }

  private String buildInstanceName(int instanceId) {
    return "Server_127.0.0.1_" + instanceId;
  }

  private ExternalView createExternalView(String tableName, int segmentCount, int replicationFactor,
      int instanceCount) {
    ExternalView externalView = new ExternalView(tableName);

    String[] instanceNames = new String[instanceCount];
    for (int i = 0; i < instanceCount; i++) {
      instanceNames[i] = buildInstanceName(i);
    }

    int assignmentCount = 0;
    for (int i = 0; i < segmentCount; i++) {
      String segmentName = tableName + "_" + i;
      for (int j = 0; j < replicationFactor; j++) {
        externalView.setState(segmentName, instanceNames[assignmentCount % instanceCount], "ONLINE");
        ++assignmentCount;
      }
    }

    return externalView;
  }

  private void validateAssertionOverMultipleRoutingTables(RoutingTableValidator routingTableValidator, String message) {
    if (EXHAUSTIVE) {
      for (int instanceCount = 1; instanceCount < 100; instanceCount += 1) {
        for (int replicationFactor = 1; replicationFactor < 10; replicationFactor++) {
          for (int segmentCount = 0; segmentCount < 300; segmentCount += 10) {
            validateAssertionForOneRoutingTable(routingTableValidator, message, instanceCount, replicationFactor,
                segmentCount);
          }
        }
      }
    } else {
      validateAssertionForOneRoutingTable(routingTableValidator, message, 50, 6, 200);
    }
  }

  private void validateAssertionForOneRoutingTable(RoutingTableValidator routingTableValidator, String message,
      int instanceCount, int replicationFactor, int segmentCount) {
    final String tableName = "fakeTable_OFFLINE";

    ExternalView externalView = createExternalView(tableName, segmentCount, replicationFactor, instanceCount);
    List<InstanceConfig> instanceConfigs = createInstanceConfigs(instanceCount);

    validateAssertionForOneRoutingTable(routingTableValidator, message, externalView, instanceConfigs, tableName);
  }

  private void validateAssertionForOneRoutingTable(RoutingTableValidator routingTableValidator, String message,
      ExternalView externalView, List<InstanceConfig> instanceConfigs, String tableName) {

    _largeClusterRoutingTableBuilder.computeOnExternalViewChange(tableName, externalView, instanceConfigs);
    List<Map<ServerInstance, List<String>>> routingTables = _largeClusterRoutingTableBuilder.getRoutingTables();

    for (Map<ServerInstance, List<String>> routingTable : routingTables) {
      assertTrue(routingTableValidator.isRoutingTableValid(routingTable, externalView, instanceConfigs), message);
    }
  }

  private List<InstanceConfig> createInstanceConfigs(int instanceCount) {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();

    for (int i = 0; i < instanceCount; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(buildInstanceName(i));
      instanceConfig.setInstanceEnabled(true);
      instanceConfigs.add(instanceConfig);
    }

    return instanceConfigs;
  }
}
