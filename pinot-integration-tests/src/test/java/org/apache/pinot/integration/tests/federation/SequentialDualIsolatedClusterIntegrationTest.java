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
package org.apache.pinot.integration.tests.federation;

import java.io.File;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotClientTransport;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.helix.ControllerTest.LOCAL_HOST;
import static org.apache.pinot.controller.helix.ControllerTest.sendGetRequest;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_BROKER_HOSTNAME;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_BROKER_TIMEOUT_MS;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS;
import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_TIMEZONE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SERVER;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration test that starts two completely isolated Pinot clusters sequentially.
 * Cluster 1 is set up completely first with all necessary tables, and only after that
 * Cluster 2 is set up. This tests the sequential federation setup scenario.
 *
 * Note: These tests are expected to fail as they test a scenario where clusters are
 * not aware of each other during initial setup.
 */
public class SequentialDualIsolatedClusterIntegrationTest extends BaseDualIsolatedClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SequentialDualIsolatedClusterIntegrationTest.class);

  /**
   * Overridden setUp that does NOT start both clusters in parallel.
   * This version is designed to be called per-test to allow sequential setup.
   */
  @BeforeClass
  public void setUp() throws Exception {
    LOGGER.info("Sequential cluster setup - initializing cluster components only");

    // Initialize cluster components but don't start clusters yet
    _cluster1 = new ClusterComponents();
    _cluster2 = new ClusterComponents();

    // Create test directories
    _cluster1._tempDir = new File(FileUtils.getTempDirectory(), "cluster1_" + getClass().getSimpleName());
    _cluster1._segmentDir = new File(_cluster1._tempDir, "segmentDir");
    _cluster1._tarDir = new File(_cluster1._tempDir, "tarDir");

    _cluster2._tempDir = new File(FileUtils.getTempDirectory(), "cluster2_" + getClass().getSimpleName());
    _cluster2._segmentDir = new File(_cluster2._tempDir, "segmentDir");
    _cluster2._tarDir = new File(_cluster2._tempDir, "tarDir");

    LOGGER.info("Cluster components initialized - clusters will be started sequentially in individual tests");
  }

  /**
   * Sets up cluster 1 completely first with all components and tables
   */
  private void setupCluster1Complete() throws Exception {
    LOGGER.info("Setting up Cluster 1 completely first");

    // Setup directories for cluster 1
    TestUtils.ensureDirectoriesExistAndEmpty(
        _cluster1._tempDir, _cluster1._segmentDir, _cluster1._tarDir);

    // Start Zookeeper for cluster 1
    LOGGER.info("Starting Zookeeper for cluster 1");
    _cluster1._zkInstance = ZkStarter.startLocalZkServer();
    _cluster1._zkUrl = _cluster1._zkInstance.getZkUrl();

    // Start controller for cluster 1
    _cluster1._controllerPort = findAvailablePort(CLUSTER_1_CONFIG._basePort);
    startController(_cluster1, CLUSTER_1_CONFIG);

    // Start broker for cluster 1 (without secondary cluster reference initially)
    _cluster1._brokerPort = findAvailablePort(_cluster1._controllerPort + 1000);
    startBrokerWithoutSecondary(_cluster1, CLUSTER_1_CONFIG);

    // Start server for cluster 1
    _cluster1._serverPort = findAvailablePort(_cluster1._brokerPort + 1000);
    startServerWithMSE(_cluster1, CLUSTER_1_CONFIG);

    // Setup Pinot connection for cluster 1
    PinotClientTransport transport1 = new JsonAsyncHttpPinotClientTransportFactory().buildTransport();
    _cluster1._pinotConnection = ConnectionFactory.fromZookeeper(
        _cluster1._zkUrl + "/" + CLUSTER_1_CONFIG._name, transport1);

    LOGGER.info("Cluster 1 setup completed");
  }

  /**
   * Sets up cluster 2 after cluster 1 is completely ready
   */
  private void setupCluster2AfterCluster1() throws Exception {
    LOGGER.info("Setting up Cluster 2 after Cluster 1 is ready");

    // Setup directories for cluster 2
    TestUtils.ensureDirectoriesExistAndEmpty(
        _cluster2._tempDir, _cluster2._segmentDir, _cluster2._tarDir);

    // Start Zookeeper for cluster 2
    LOGGER.info("Starting Zookeeper for cluster 2");
    _cluster2._zkInstance = ZkStarter.startLocalZkServer();
    _cluster2._zkUrl = _cluster2._zkInstance.getZkUrl();

    // Start controller for cluster 2
    _cluster2._controllerPort = findAvailablePort(CLUSTER_2_CONFIG._basePort);
    startController(_cluster2, CLUSTER_2_CONFIG);

    // Start broker for cluster 2 with reference to cluster 1
    _cluster2._brokerPort = findAvailablePort(_cluster2._controllerPort + 1000);
    startBroker(_cluster2, _cluster1, CLUSTER_2_CONFIG);

    // Start server for cluster 2
    _cluster2._serverPort = findAvailablePort(_cluster2._brokerPort + 1000);
    startServerWithMSE(_cluster2, CLUSTER_2_CONFIG);

    // Setup Pinot connection for cluster 2
    PinotClientTransport transport2 = new JsonAsyncHttpPinotClientTransportFactory().buildTransport();
    _cluster2._pinotConnection = ConnectionFactory.fromZookeeper(
        _cluster2._zkUrl + "/" + CLUSTER_2_CONFIG._name, transport2);

    LOGGER.info("Cluster 2 setup completed");
  }

  /**
   * Starts a broker without secondary cluster configuration
   */
  private void startBrokerWithoutSecondary(ClusterComponents cluster, ClusterConfig config) throws Exception {
    PinotConfiguration brokerConfig = new PinotConfiguration();
    brokerConfig.setProperty(CONFIG_OF_ZOOKEEPER_SERVER, cluster._zkUrl);
    brokerConfig.setProperty(CONFIG_OF_CLUSTER_NAME, config._name);
    brokerConfig.setProperty(CONFIG_OF_BROKER_HOSTNAME, LOCAL_HOST);
    brokerConfig.setProperty(KEY_OF_BROKER_QUERY_PORT, cluster._brokerPort);
    brokerConfig.setProperty(CONFIG_OF_BROKER_TIMEOUT_MS, 60 * 1000L);
    brokerConfig.setProperty(CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    brokerConfig.setProperty(CONFIG_OF_TIMEZONE, "UTC");

    cluster._brokerStarter = createBrokerStarter();
    cluster._brokerStarter.init(brokerConfig);
    cluster._brokerStarter.start();

    LOGGER.info("Broker started without secondary cluster configuration for {}", config._name);
  }

  /**
   * Common setup for both clusters sequentially: sets up cluster 1 with data, then cluster 2 with data
   */
  private void setupBothClustersSequentially(String[] cluster1Tables, String[] cluster2Tables) throws Exception {
    // Setup Cluster 1 completely
    setupCluster1Complete();

    // Create and load tables on Cluster 1
    for (String tableName : cluster1Tables) {
      dropTableAndSchemaIfExists(tableName, _cluster1._controllerBaseApiUrl);
      createSchemaAndTableForCluster(tableName, _cluster1._controllerBaseApiUrl);
    }
    FileUtils.cleanDirectory(_cluster1._segmentDir);
    FileUtils.cleanDirectory(_cluster1._tarDir);

    // Load data into Cluster 1 tables
    _cluster1AvroFiles = createAvroData(CLUSTER_1_SIZE, 1);
    loadDataIntoCluster(_cluster1AvroFiles, cluster1Tables[0], _cluster1);

    if (cluster1Tables.length > 1) {
      _cluster1AvroFiles2 = createAvroDataMultipleSegments(CLUSTER_1_SIZE, 1, SEGMENTS_PER_CLUSTER);
      loadDataIntoCluster(_cluster1AvroFiles2, cluster1Tables[1], _cluster1);
    }

    LOGGER.info("Cluster 1 is fully operational with data loaded");
    // Wait a bit to ensure cluster 1 is fully stable before starting cluster 2.
    Thread.sleep(5000);
    System.out.println("\n\n\nCluster 1 setup complete. Proceeding to set up Cluster 2...\n\n\n");

    // Setup Cluster 2
    setupCluster2AfterCluster1();

    // Create and load tables on Cluster 2
    for (String tableName : cluster2Tables) {
      dropTableAndSchemaIfExists(tableName, _cluster2._controllerBaseApiUrl);
      createSchemaAndTableForCluster(tableName, _cluster2._controllerBaseApiUrl);
    }
    FileUtils.cleanDirectory(_cluster2._segmentDir);
    FileUtils.cleanDirectory(_cluster2._tarDir);

    // Load data into Cluster 2 tables
    _cluster2AvroFiles = createAvroData(CLUSTER_2_SIZE, 2);
    loadDataIntoCluster(_cluster2AvroFiles, cluster2Tables[0], _cluster2);

    if (cluster2Tables.length > 1) {
      _cluster2AvroFiles2 = createAvroDataMultipleSegments(CLUSTER_2_SIZE, 2, SEGMENTS_PER_CLUSTER);
      loadDataIntoCluster(_cluster2AvroFiles2, cluster2Tables[1], _cluster2);
    }

    LOGGER.info("Cluster 2 is fully operational with data loaded");
  }

  /**
   * Creates logical tables on both clusters for federation
   */
  private void createLogicalTablesOnBothClusters(Map<String, PhysicalTableConfig> physicalTableConfigMap,
      String logicalTableName, String refCluster1Table, String refCluster2Table) throws Exception {
    createLogicalTable(CLUSTER_1_NAME, SCHEMA_FILE, physicalTableConfigMap, DEFAULT_TENANT,
      _cluster1._controllerBaseApiUrl, logicalTableName, refCluster1Table, null);
    createLogicalTable(CLUSTER_2_NAME, SCHEMA_FILE, physicalTableConfigMap, DEFAULT_TENANT,
        _cluster2._controllerBaseApiUrl, logicalTableName, refCluster2Table, null);
  }

  @Test
  public void testSequentialLogicalFederationSSE() throws Exception {
    LOGGER.info("Starting sequential federation test for SSE");

    // Setup both clusters sequentially with data
    setupBothClustersSequentially(
        new String[]{LOGICAL_FEDERATION_CLUSTER_1_TABLE},
        new String[]{LOGICAL_FEDERATION_CLUSTER_2_TABLE});

    // Create logical tables on both clusters
    Map<String, PhysicalTableConfig> physicalTableConfigMap = Map.of(
        LOGICAL_FEDERATION_CLUSTER_1_TABLE + "_OFFLINE",
        new PhysicalTableConfig(true),
        LOGICAL_FEDERATION_CLUSTER_2_TABLE + "_OFFLINE",
        new PhysicalTableConfig(true));

    createLogicalTablesOnBothClusters(physicalTableConfigMap, LOGICAL_TABLE_NAME,
        LOGICAL_FEDERATION_CLUSTER_1_TABLE + "_OFFLINE", LOGICAL_FEDERATION_CLUSTER_2_TABLE + "_OFFLINE");

    long cluster2Count = getCount(LOGICAL_TABLE_NAME, _cluster2);
    LOGGER.info("Cluster 2 count: {}, expected: {}", cluster2Count, CLUSTER_1_SIZE + CLUSTER_2_SIZE);

    // Assertions expected to fail for sequential setup
    assertEquals(cluster2Count, CLUSTER_1_SIZE + CLUSTER_2_SIZE,
        "Cluster 2 should see federated data from both clusters");
  }

  @Test
  public void testSequentialLogicalFederationMSE() throws Exception {
    LOGGER.info("Starting sequential federation test for MSE");

    // Setup both clusters sequentially with two tables each for join
    setupBothClustersSequentially(
        new String[]{LOGICAL_FEDERATION_CLUSTER_1_TABLE, LOGICAL_FEDERATION_CLUSTER_1_TABLE_2},
        new String[]{LOGICAL_FEDERATION_CLUSTER_2_TABLE, LOGICAL_FEDERATION_CLUSTER_2_TABLE_2});

    // Create two logical tables on both clusters for join query
    Map<String, PhysicalTableConfig> physicalTableConfigMap1 = Map.of(
        LOGICAL_FEDERATION_CLUSTER_1_TABLE + "_OFFLINE",
        new PhysicalTableConfig(true),
        LOGICAL_FEDERATION_CLUSTER_2_TABLE + "_OFFLINE",
        new PhysicalTableConfig(true));

    Map<String, PhysicalTableConfig> physicalTableConfigMap2 = Map.of(
        LOGICAL_FEDERATION_CLUSTER_1_TABLE_2 + "_OFFLINE",
        new PhysicalTableConfig(true),
        LOGICAL_FEDERATION_CLUSTER_2_TABLE_2 + "_OFFLINE",
        new PhysicalTableConfig(true));

    createLogicalTablesOnBothClusters(physicalTableConfigMap1, LOGICAL_TABLE_NAME, LOGICAL_FEDERATION_CLUSTER_1_TABLE + "_OFFLINE",
      LOGICAL_FEDERATION_CLUSTER_2_TABLE + "_OFFLINE");
    createLogicalTablesOnBothClusters(physicalTableConfigMap2, LOGICAL_TABLE_NAME_2,         LOGICAL_FEDERATION_CLUSTER_1_TABLE_2 + "_OFFLINE",
      LOGICAL_FEDERATION_CLUSTER_2_TABLE_2 + "_OFFLINE");
    Thread.sleep(2000);

    // Execute MSE join query (expected to fail due to sequential setup)
    String joinQuery = "SET useMultistageEngine=true; SELECT t1." + JOIN_COLUMN + ", COUNT(*) as count "
        + "FROM " + LOGICAL_TABLE_NAME + " t1 "
        + "JOIN " + LOGICAL_TABLE_NAME_2 + " t2 ON t1." + JOIN_COLUMN + " = t2." + JOIN_COLUMN + " "
        + "GROUP BY t1." + JOIN_COLUMN + " LIMIT 20";

    String result = executeQuery(joinQuery, _cluster2);
    assertNotNull(result);
    assertTrue(result.contains("resultTable"));
    assertResultRows(result);
  }

  /**
   * Simple validation test to ensure cluster 1 can operate independently
   */
  @Test
  public void testCluster1IndependentOperation() throws Exception {
    setupCluster1Complete();

    // Create a simple table on cluster 1
    dropTableAndSchemaIfExists(FEDERATION_TABLE, _cluster1._controllerBaseApiUrl);
    createSchemaAndTableForCluster(FEDERATION_TABLE, _cluster1._controllerBaseApiUrl);

    // Verify table exists
    String cluster1Tables = sendGetRequest(_cluster1._controllerBaseApiUrl + "/tables");
    assertTrue(cluster1Tables.contains(FEDERATION_TABLE));
  }
}
