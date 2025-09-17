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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class QueryWorkloadIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int NUM_REALTIME_SEGMENTS = 6;

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zk, Kafka and Pinot
    startZk();
    startController();
    startBroker();
    Tracing.unregisterThreadAccountant();
    startServer();
    startKafka();

    List<File> avroFiles = getAllAvroFiles();
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles, NUM_REALTIME_SEGMENTS);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    // Add offline table config
    TableConfig offlineTableConfig = createOfflineTableConfig();
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap =
        Collections.singletonMap("OFFLINE", createInstanceAssignmentConfig(true, TableType.OFFLINE));
    offlineTableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    addTableConfig(offlineTableConfig);
    // Add realtime table config
    TableConfig realtimeTableConfig = createRealtimeTableConfig(realtimeAvroFiles.get(0));
    instanceAssignmentConfigMap =
        Collections.singletonMap("COMPLETED", createInstanceAssignmentConfig(false, TableType.REALTIME));
    realtimeTableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    addTableConfig(realtimeTableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _segmentDir,
        _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Push data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(100_000L);
  }

  // TODO: Expand tests to cover more scenarios for workload enforcement
  @Test
  public void testQueryWorkloadConfig() throws Exception {
    EnforcementProfile enforcementProfile = new EnforcementProfile(1000, 1000);
    PropagationEntity entity = new PropagationEntity(DEFAULT_TABLE_NAME + "_OFFLINE", 1000L, 1000L, null);
    PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, List.of(entity));
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, enforcementProfile, propagationScheme);
    QueryWorkloadConfig queryWorkloadConfig = new QueryWorkloadConfig("testWorkload", List.of(nodeConfig));
    try {
      getControllerRequestClient().updateQueryWorkloadConfig(queryWorkloadConfig);
      TestUtils.waitForCondition(aVoid -> {
        try {
          QueryWorkloadConfig retrievedConfig = getControllerRequestClient().getQueryWorkloadConfig("testWorkload");
          return retrievedConfig != null && retrievedConfig.equals(queryWorkloadConfig);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, 60_000L, "Failed to retrieve the created query workload config");
      // Get server instances that actually serve this specific table
      String tableName = getTableName();
      Set<String> serverInstances = getServerInstancesForTable(tableName);
      long expectedCpuCostNs = entity.getCpuCostNs() / serverInstances.size();
      long expectedMemoryCostBytes = entity.getMemoryCostBytes() / serverInstances.size();
      // Test calling the endpoints on each server that serves this table
      for (String serverInstance : serverInstances) {
        testServerQueryWorkloadEndpoints(serverInstance, "testWorkload", expectedCpuCostNs, expectedMemoryCostBytes);
      }
    } finally {
      getControllerRequestClient().deleteQueryWorkloadConfig("testWorkload");
    }
  }

  /**
   * Test QueryWorkloadResource endpoints on a specific server instance for a specific workload
   */
  private void testServerQueryWorkloadEndpoints(String serverInstance, String workloadName,
                                                long expectedCpuBudgetNs, long expectedMemoryBudgetBytes)
      throws Exception {
    // Extract host from server instance name (format: Server_hostname_port)
    String[] parts = serverInstance.split("_");
    String host = parts[1];

    // Use the proper admin API port (not the netty port from instance name)
    String serverBaseApiUrl = "http://" + host + ":" + getServerAdminApiPort();

    // Test the get specific workload endpoint (GET /queryWorkloadCost/{workloadName})
    String getWorkloadUrl = serverBaseApiUrl + "/debug/queryWorkloadCost/" + workloadName;
    String workloadResponse = sendGetRequest(getWorkloadUrl);

    // Verify response is valid JSON and contains InstanceCost structure
    JsonNode workloadResponseJson = JsonUtils.stringToJsonNode(workloadResponse);
    assertNotNull(workloadResponseJson);
    long actualCpuCostNs = workloadResponseJson.get("cpuBudgetNs").asLong();
    long actualMemoryCostBytes = workloadResponseJson.get("memoryBudgetBytes").asLong();
    assertEquals(actualCpuCostNs, expectedCpuBudgetNs);
    assertEquals(actualMemoryCostBytes, expectedMemoryBudgetBytes);
  }

  /**
   * Get the definitive list of server instances that serve a specific table
   */
  private Set<String> getServerInstancesForTable(String tableName) throws Exception {
    // Use the controller API to get server instances for the specific table
    String url = _controllerRequestURLBuilder.forTableGetServerInstances(tableName);
    String response = sendGetRequest(url);

    // Parse the JSON response to extract server instance names
    JsonNode responseJson = JsonUtils.stringToJsonNode(response);
    JsonNode serverInstancesNode = responseJson.get("server");

    Set<String> serverInstances = new HashSet<>();
    if (serverInstancesNode != null && serverInstancesNode.isArray()) {
      for (JsonNode instanceNode : serverInstancesNode) {
        for (JsonNode instance : instanceNode.get("instances")) {
          serverInstances.add(instance.asText());
        }
      }
    }
    return serverInstances;
  }

  private InstanceAssignmentConfig createInstanceAssignmentConfig(boolean minimizeDataMovement, TableType tableType) {
    InstanceTagPoolConfig instanceTagPoolConfig =
        new InstanceTagPoolConfig(TagNameUtils.getServerTagForTenant(getServerTenant(), tableType), false, 1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1, 1,
            1, 1, 1, minimizeDataMovement,
            null);
    return new InstanceAssignmentConfig(instanceTagPoolConfig,
        instanceConstraintConfig, instanceReplicaGroupPartitionConfig, null, minimizeDataMovement);
  }
}
