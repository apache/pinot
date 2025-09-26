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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.core.query.scheduler.QuerySchedulerFactory;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class QueryWorkloadIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int NUM_REALTIME_SEGMENTS = 6;
  private static final int NUM_SERVERS = 2;
  private static final int NUM_BROKERS = 2;

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    enableQueryWorkloadWithEnforcement(configuration, InstanceType.BROKER);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    enableQueryWorkloadWithEnforcement(configuration, InstanceType.SERVER);
  }

  private void enableQueryWorkloadWithEnforcement(PinotConfiguration configuration, InstanceType instanceType) {
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION, true);
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, true);
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.ResourceUsageAccountantFactory");
    // Set the sleep time to 0 to enable precise measurement and enforcement in tests
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_WORKLOAD_SLEEP_TIME_MS, 0);
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT, true);
    if (instanceType == InstanceType.BROKER) {
      configuration.setProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT,
          true);
      configuration.setProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT,
          true);
    } else {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT,
          true);
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT,
          true);
      configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
          + QuerySchedulerFactory.ALGORITHM_NAME_CONFIG_KEY, QuerySchedulerFactory.WORKLOAD_SCHEDULER_ALGORITHM);
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zk, Kafka and Pinot
    startZk();
    startController();
    startBrokers(NUM_BROKERS);
    Tracing.unregisterThreadAccountant();
    startServers(NUM_SERVERS);
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
        Collections.singletonMap("OFFLINE", createInstanceAssignmentConfig());
    offlineTableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    addTableConfig(offlineTableConfig);

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

  /**
   * Test basic workload config creation and cost propagation
   */
  @Test
  public void testQueryWorkloadConfigPropagation() throws Exception {
    EnforcementProfile enforcementProfile = new EnforcementProfile(Long.MAX_VALUE, Long.MAX_VALUE);
    PropagationEntity entity = new PropagationEntity(DEFAULT_TABLE_NAME, Long.MAX_VALUE, Long.MAX_VALUE, null);
    PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, List.of(entity));
    NodeConfig serverNode = new NodeConfig(NodeConfig.Type.SERVER_NODE, enforcementProfile, propagationScheme);
    NodeConfig brokerNode = new NodeConfig(NodeConfig.Type.BROKER_NODE, enforcementProfile, propagationScheme);
    String workloadName = "testWorkload";
    QueryWorkloadConfig workloadConfig = new QueryWorkloadConfig(workloadName, List.of(serverNode, brokerNode));
    try {
      updateAndValidateWorkloadConfigPropagation(workloadConfig);
    } finally {
      getControllerRequestClient().deleteQueryWorkloadConfig(workloadName);
    }
  }

  /**
   * Test query execution with budget configuration
   */
  @Test
  public void testWorkloadEnforcement() throws Exception {
    // Test enforcement with high budget - should succeed (no rejection)
    EnforcementProfile enforcementProfile = new EnforcementProfile(Long.MAX_VALUE, Long.MAX_VALUE);
    PropagationEntity entity = new PropagationEntity(DEFAULT_TABLE_NAME, Long.MAX_VALUE, Long.MAX_VALUE, null);
    PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, List.of(entity));
    NodeConfig brokerConfig = new NodeConfig(NodeConfig.Type.BROKER_NODE, enforcementProfile, propagationScheme);
    NodeConfig serverConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, enforcementProfile, propagationScheme);
    String workloadName = "highBudgetWorkload";
    QueryWorkloadConfig workloadConfig = new QueryWorkloadConfig(workloadName, List.of(brokerConfig, serverConfig));
    testWorkloadEnforcementWithBudgets(workloadConfig, false);

    // Test broker enforcement with low budgets (rejection expected)
    enforcementProfile = new EnforcementProfile(1, 1);
    entity = new PropagationEntity(DEFAULT_TABLE_NAME, 1L, 1L, null);
    propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, List.of(entity));
    brokerConfig = new NodeConfig(NodeConfig.Type.BROKER_NODE, enforcementProfile, propagationScheme);
    workloadConfig = new QueryWorkloadConfig("lowBudgetBrokerWorkload", List.of(brokerConfig));
    testWorkloadEnforcementWithBudgets(workloadConfig, true);

    // Test server enforcement with low budgets (rejection expected)
    enforcementProfile = new EnforcementProfile(1, 1);
    entity = new PropagationEntity(DEFAULT_TABLE_NAME, 1L, 1L, null);
    propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, List.of(entity));
    serverConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, enforcementProfile, propagationScheme);
    workloadConfig = new QueryWorkloadConfig("lowBudgetServerWorkload", List.of(serverConfig));
    testWorkloadEnforcementWithBudgets(workloadConfig, true);
  }

  private void testWorkloadEnforcementWithBudgets(QueryWorkloadConfig workloadConfig, boolean expectRejection)
      throws Exception {
    String workloadName = workloadConfig.getQueryWorkloadName();
    try {
      updateAndValidateWorkloadConfigPropagation(workloadConfig);
      // Test query execution
      testQueryExecution(workloadName, expectRejection);
    } finally {
      getControllerRequestClient().deleteQueryWorkloadConfig(workloadName);
    }
  }

  /**
   * Helper method to test cost propagation to all instances (servers and brokers)
   */
  private void testCostPropagation(QueryWorkloadConfig queryWorkloadConfig) throws Exception {
    for (NodeConfig nodeConfig : queryWorkloadConfig.getNodeConfigs()) {
      PropagationScheme propagationScheme = nodeConfig.getPropagationScheme();
      NodeConfig.Type nodeType = nodeConfig.getNodeType();
      if (propagationScheme.getPropagationType() != PropagationScheme.Type.TABLE) {
        throw new IllegalStateException("Only TABLE propagation test is supported currently");
      }
      for (PropagationEntity entity : propagationScheme.getPropagationEntities()) {
        String tableName = TableNameBuilder.extractRawTableName(entity.getEntity());
        Set<String> instances;
        if (nodeType == NodeConfig.Type.BROKER_NODE) {
          instances = getInstancesForTable(tableName, InstanceType.BROKER);
        } else if (nodeType == NodeConfig.Type.SERVER_NODE) {
          instances = getInstancesForTable(tableName, InstanceType.SERVER);
        } else {
          throw new IllegalStateException("Unsupported node type for cost propagation test: " + nodeType);
        }
        long expectedCpuCostNs = entity.getCpuCostNs() / instances.size();
        long expectedMemoryCostBytes = entity.getMemoryCostBytes() / instances.size();
        for (String instance : instances) {
          testCostPropagationOnInstances(instance, queryWorkloadConfig.getQueryWorkloadName(), expectedCpuCostNs,
              expectedMemoryCostBytes);
        }
      }
    }
  }

  /**
   * Helper method to test query execution with workload and verify expected behavior
   */
  private void testQueryExecution(String workloadName, boolean expectRejection) throws Exception {
    String query = "SELECT DISTINCTCOUNT(AirlineID), DISTINCTCOUNT(Carrier) FROM myTable GROUP BY ArrTimeBlk"
        + " LIMIT 10000;" + "SET workloadName='" + workloadName + "'";
    JsonNode response = postQuery(query);
    JsonNode exceptions = response.get("exceptions");

    if (expectRejection) {
      assertFalse(exceptions.isEmpty(), "Expected workload enforcement to reject query for: " + workloadName);
      int errorCode = exceptions.get(0).get("errorCode").asInt();
      assertEquals(errorCode, QueryErrorCode.WORKLOAD_BUDGET_EXCEEDED.getId(),
          "Expected workload budget exceeded error but got: " + exceptions);
    } else {
      // For high budget scenarios, we expect the query to succeed (no exceptions or empty exceptions)
      if (exceptions != null && !exceptions.isEmpty()) {
        // If there are exceptions, they should not be budget-related
        int errorCode = exceptions.get(0).get("errorCode").asInt();
        assertNotEquals(errorCode, QueryErrorCode.WORKLOAD_BUDGET_EXCEEDED.getId(),
            "Unexpected workload budget exceeded error for high budget scenario: " + exceptions);
      }
    }
  }

  /**
   * Test QueryWorkloadResource endpoints on a specific server instance for a specific workload
   */
  private void testCostPropagationOnInstances(String instance, String workloadName,
                                              long expectedCpuBudgetNs, long expectedMemoryBudgetBytes)
      throws Exception {
    // Extract host from server instance name (format: Server_hostname_port)
    String[] parts = instance.split("_");
    String host = parts[1];

    String baseUrl;
    if (InstanceTypeUtils.isServer(instance)) {
      baseUrl = "http://" + host + ":" + getServerAdminApiPort();
    } else if (InstanceTypeUtils.isBroker(instance)) {
      baseUrl = "http://" + host + ":" + parts[2];
    } else {
      throw new IllegalArgumentException("Instance is neither server nor broker: " + instance);
    }
    // Test the get specific workload endpoint (GET /queryWorkloadCost/{workloadName})
    String getWorkloadUrl = baseUrl + "/debug/queryWorkloadCost/" + workloadName;
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
   * Get the definitive list of instances that serve a specific table
   */
  private Set<String> getInstancesForTable(String tableName, InstanceType instanceType) throws Exception {
    String url;
    String tag;
    if (instanceType == InstanceType.BROKER) {
      url = _controllerRequestURLBuilder.forTableGetBrokerInstances(tableName);
      tag = "brokers";
    } else if (instanceType == InstanceType.SERVER) {
      url = _controllerRequestURLBuilder.forTableGetServerInstances(tableName);
      tag = "server";
    } else {
      throw new IllegalArgumentException("Unsupported instance type: " + instanceType);
    }
    String response = sendGetRequest(url);

    // Parse the JSON response to extract server instance names
    JsonNode responseJson = JsonUtils.stringToJsonNode(response);
    JsonNode instanceNodes = responseJson.get(tag);

    Set<String> serverInstances = new HashSet<>();
    if (instanceNodes != null && instanceNodes.isArray()) {
      for (JsonNode instanceNode : instanceNodes) {
        for (JsonNode instance : instanceNode.get("instances")) {
          serverInstances.add(instance.asText());
        }
      }
    }
    return serverInstances;
  }

  /**
   * Helper method to wait for workload config propagation
   */
  private void updateAndValidateWorkloadConfigPropagation(QueryWorkloadConfig queryWorkloadConfig)
      throws Exception {
    getControllerRequestClient().updateQueryWorkloadConfig(queryWorkloadConfig);
    String workloadName = queryWorkloadConfig.getQueryWorkloadName();
    TestUtils.waitForCondition(aVoid -> {
      try {
        QueryWorkloadConfig retrievedConfig = getControllerRequestClient().getQueryWorkloadConfig(workloadName);
        return retrievedConfig != null && retrievedConfig.equals(queryWorkloadConfig);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to retrieve the created query workload config: " + workloadName);
    testCostPropagation(queryWorkloadConfig);
  }


  private InstanceAssignmentConfig createInstanceAssignmentConfig() {
    InstanceTagPoolConfig instanceTagPoolConfig =
        new InstanceTagPoolConfig(TagNameUtils.getServerTagForTenant(getServerTenant(), TableType.OFFLINE), false,
            1, null);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1, NUM_SERVERS,
            1, 1, 1, false,
            null);
    return new InstanceAssignmentConfig(instanceTagPoolConfig,
        null, instanceReplicaGroupPartitionConfig, null, false);
  }
}
