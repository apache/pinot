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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
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
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class QueryWorkloadIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int NUM_REALTIME_SEGMENTS = 6;
  private static final String SHARED_TABLE_NAME = "query_workload";
  private static final String WORKLOAD_NAME = "testWorkload";

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(Accounting.BROKER_PREFIX + "." + Accounting.Keys.WORKLOAD_ENABLE_COST_COLLECTION, true);
    try {
      configuration.setProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT,
          org.apache.pinot.spi.utils.NetUtils.findOpenPort());
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to allocate mailbox port", e);
    }
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(Accounting.SERVER_PREFIX + "." + Accounting.Keys.WORKLOAD_ENABLE_COST_COLLECTION, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start Zk, Kafka and Pinot
    startZk();
    startKafka();
    startController();
    startBroker();
    startServer();

    cleanupQueryWorkloadConfig();
    cleanTableAndSchema();
    resetKafkaTopic();

    List<File> avroFiles = getAllAvroFiles(_classTempDir);
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
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    // Push data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(100_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanupQueryWorkloadConfig);
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopController);
    exception = runCleanup(exception, this::stopKafka);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanTempDirectory);
    if (exception != null) {
      throw exception;
    }
  }

  // TODO: Expand tests to cover more scenarios for workload enforcement
  @Test
  public void testQueryWorkloadConfig()
      throws Exception {
    EnforcementProfile enforcementProfile = new EnforcementProfile(1000, 1000);
    PropagationEntity entity =
        new PropagationEntity(TableNameBuilder.OFFLINE.tableNameWithType(getTableName()), 1000L, 1000L, null);
    PropagationScheme propagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, List.of(entity));
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, enforcementProfile, propagationScheme);
    QueryWorkloadConfig queryWorkloadConfig = new QueryWorkloadConfig(WORKLOAD_NAME, List.of(nodeConfig));
    try {
      getOrCreateAdminClient().getQueryWorkloadClient()
          .updateQueryWorkloadConfig(JsonUtils.objectToString(queryWorkloadConfig));
      TestUtils.waitForCondition(aVoid -> {
        try {
          QueryWorkloadConfig retrievedConfig =
              getOrCreateAdminClient().getQueryWorkloadClient().getQueryWorkloadConfigObject(WORKLOAD_NAME);
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
        testServerQueryWorkloadEndpoints(serverInstance, WORKLOAD_NAME, expectedCpuCostNs, expectedMemoryCostBytes);
      }
    } finally {
      cleanupQueryWorkloadConfig();
    }
  }

  /**
   * Test QueryWorkloadResource endpoints on a specific server instance for a specific workload
   */
  private void testServerQueryWorkloadEndpoints(String serverInstance, String workloadName, long expectedCpuBudgetNs,
      long expectedMemoryBudgetBytes)
      throws Exception {
    // Test the get specific workload endpoint (GET /queryWorkloadCost/{workloadName})
    String getWorkloadUrl = getServerBaseApiUrl(serverInstance) + "/debug/queryWorkloadCost/" + workloadName;
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
  private Set<String> getServerInstancesForTable(String tableName)
      throws Exception {
    // Use the controller API to get server instances for the specific table
    String response = getOrCreateAdminClient().getTableClient().getTableInstances(tableName, "server");

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
        new InstanceReplicaGroupPartitionConfig(true, 1, 1, 1, 1, 1, minimizeDataMovement, null);
    return new InstanceAssignmentConfig(instanceTagPoolConfig, instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig, null, minimizeDataMovement);
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private List<File> getAllAvroFiles(File outputDir)
      throws Exception {
    int numSegments = unpackAvroData(outputDir).size();
    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(outputDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }
    return avroFiles;
  }

  private void cleanupQueryWorkloadConfig()
      throws Exception {
    if (_controllerStarter == null) {
      return;
    }
    getOrCreateAdminClient().getQueryWorkloadClient().deleteQueryWorkloadConfig(WORKLOAD_NAME);
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    if (_helixResourceManager.getAllTables().contains(realtimeTableName) || _helixResourceManager.hasRealtimeTable(
        tableName)) {
      dropRealtimeTable(tableName);
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }

    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void resetKafkaTopic() {
    deleteKafkaTopicIfPresent();
    createKafkaTopic(getKafkaTopic());
  }

  private void deleteKafkaTopicIfPresent() {
    if (isKafkaTopicPresent()) {
      deleteKafkaTopic(getKafkaTopic());
    }
  }

  private boolean isKafkaTopicPresent() {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return false;
    }
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(getKafkaTopic());
    } catch (Exception e) {
      return false;
    }
  }

  private String getServerBaseApiUrl(String serverInstance) {
    String host = serverInstance.split("_")[1];
    for (BaseServerStarter serverStarter : _serverStarters) {
      if (serverStarter.getInstanceId().equals(serverInstance)) {
        String adminApiPort = serverStarter.getConfig().getProperty(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT);
        if (adminApiPort != null) {
          return "http://" + host + ":" + adminApiPort;
        }
      }
    }
    return "http://" + host + ":" + getServerAdminApiPort();
  }

  private void closeH2Connection()
      throws Exception {
    if (_h2Connection != null) {
      _h2Connection.close();
      _h2Connection = null;
    }
  }

  private void closePinotConnections() {
    if (_pinotConnection != null) {
      _pinotConnection.close();
      _pinotConnection = null;
    }
    if (_pinotConnectionV2 != null) {
      _pinotConnectionV2.close();
      _pinotConnectionV2 = null;
    }
  }

  private void cleanTempDirectory()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
