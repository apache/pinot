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

import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotClientTransport;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
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

public class SequentialDualIsolatedClusterIntegrationTest extends BaseDualIsolatedClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SequentialDualIsolatedClusterIntegrationTest.class);

  @BeforeClass
  public void setUp() throws Exception {
    _cluster1 = new ClusterComponents();
    _cluster2 = new ClusterComponents();
    setupDirectories();
  }

  private void setupCluster1Complete() throws Exception {
    startZookeeper(_cluster1);
    _cluster1._controllerPort = findAvailablePort(CLUSTER_1_CONFIG._basePort);
    startController(_cluster1, CLUSTER_1_CONFIG);
    _cluster1._brokerPort = findAvailablePort(_cluster1._controllerPort + 1000);
    startBrokerWithoutSecondary(_cluster1, CLUSTER_1_CONFIG);
    _cluster1._serverPort = findAvailablePort(_cluster1._brokerPort + 1000);
    startServerWithMSE(_cluster1, CLUSTER_1_CONFIG);
    PinotClientTransport transport = new JsonAsyncHttpPinotClientTransportFactory().buildTransport();
    _cluster1._pinotConnection = ConnectionFactory.fromZookeeper(
        _cluster1._zkUrl + "/" + CLUSTER_1_CONFIG._name, transport);
  }

  private void setupCluster2AfterCluster1() throws Exception {
    startZookeeper(_cluster2);
    _cluster2._controllerPort = findAvailablePort(CLUSTER_2_CONFIG._basePort);
    startController(_cluster2, CLUSTER_2_CONFIG);
    _cluster2._brokerPort = findAvailablePort(_cluster2._controllerPort + 1000);
    startBroker(_cluster2, _cluster1, CLUSTER_2_CONFIG);
    _cluster2._serverPort = findAvailablePort(_cluster2._brokerPort + 1000);
    startServerWithMSE(_cluster2, CLUSTER_2_CONFIG);
    PinotClientTransport transport = new JsonAsyncHttpPinotClientTransportFactory().buildTransport();
    _cluster2._pinotConnection = ConnectionFactory.fromZookeeper(
        _cluster2._zkUrl + "/" + CLUSTER_2_CONFIG._name, transport);
  }

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
  }

  private void setupBothClustersSequentially(String[] cluster1Tables, String[] cluster2Tables) throws Exception {
    setupCluster1Complete();
    setupTablesWithData(_cluster1, cluster1Tables, 1);
    Thread.sleep(5000);
    setupCluster2AfterCluster1();
    setupTablesWithData(_cluster2, cluster2Tables, 2);
  }

  private void setupTablesWithData(ClusterComponents cluster, String[] tables, int clusterId) throws Exception {
    for (String table : tables) {
      dropTableAndSchemaIfExists(table, cluster._controllerBaseApiUrl);
      createSchemaAndTableForCluster(table, cluster._controllerBaseApiUrl);
    }
    FileUtils.cleanDirectory(cluster._segmentDir);
    FileUtils.cleanDirectory(cluster._tarDir);
    loadDataIntoCluster(createAvroData(clusterId == 1 ? CLUSTER_1_SIZE : CLUSTER_2_SIZE, clusterId),
        tables[0], cluster);
    if (tables.length > 1) {
      loadDataIntoCluster(createAvroDataMultipleSegments(clusterId == 1 ? CLUSTER_1_SIZE : CLUSTER_2_SIZE,
          clusterId, SEGMENTS_PER_CLUSTER), tables[1], cluster);
    }
  }

  private void createLogicalTablesOnBothClusters(Map<String, PhysicalTableConfig> physicalTableConfigMap,
      String logicalTableName, String refCluster1Table, String refCluster2Table) throws Exception {
    createLogicalTable(CLUSTER_1_NAME, SCHEMA_FILE, physicalTableConfigMap, DEFAULT_TENANT,
        _cluster1._controllerBaseApiUrl, logicalTableName, refCluster1Table, null);
    createLogicalTable(CLUSTER_2_NAME, SCHEMA_FILE, physicalTableConfigMap, DEFAULT_TENANT,
        _cluster2._controllerBaseApiUrl, logicalTableName, refCluster2Table, null);
  }

  @Test
  public void testSequentialLogicalFederationSSE() throws Exception {
    setupBothClustersSequentially(
        new String[]{LOGICAL_FEDERATION_CLUSTER_1_TABLE},
        new String[]{LOGICAL_FEDERATION_CLUSTER_2_TABLE});
    Map<String, PhysicalTableConfig> configMap = Map.of(
        LOGICAL_FEDERATION_CLUSTER_1_TABLE + "_OFFLINE", new PhysicalTableConfig(true),
        LOGICAL_FEDERATION_CLUSTER_2_TABLE + "_OFFLINE", new PhysicalTableConfig(true));
    createLogicalTablesOnBothClusters(configMap, LOGICAL_TABLE_NAME,
        LOGICAL_FEDERATION_CLUSTER_1_TABLE + "_OFFLINE", LOGICAL_FEDERATION_CLUSTER_2_TABLE + "_OFFLINE");
    assertEquals(getCount(LOGICAL_TABLE_NAME, _cluster2, true), CLUSTER_1_SIZE + CLUSTER_2_SIZE);
  }

  @Test
  public void testSequentialLogicalFederationMSE() throws Exception {
    setupBothClustersSequentially(
        new String[]{LOGICAL_FEDERATION_CLUSTER_1_TABLE, LOGICAL_FEDERATION_CLUSTER_1_TABLE_2},
        new String[]{LOGICAL_FEDERATION_CLUSTER_2_TABLE, LOGICAL_FEDERATION_CLUSTER_2_TABLE_2});
    createLogicalTablesOnBothClusters(
        Map.of(LOGICAL_FEDERATION_CLUSTER_1_TABLE + "_OFFLINE", new PhysicalTableConfig(true),
            LOGICAL_FEDERATION_CLUSTER_2_TABLE + "_OFFLINE", new PhysicalTableConfig(true)),
        LOGICAL_TABLE_NAME, LOGICAL_FEDERATION_CLUSTER_1_TABLE + "_OFFLINE",
        LOGICAL_FEDERATION_CLUSTER_2_TABLE + "_OFFLINE");
    createLogicalTablesOnBothClusters(
        Map.of(LOGICAL_FEDERATION_CLUSTER_1_TABLE_2 + "_OFFLINE", new PhysicalTableConfig(true),
            LOGICAL_FEDERATION_CLUSTER_2_TABLE_2 + "_OFFLINE", new PhysicalTableConfig(true)),
        LOGICAL_TABLE_NAME_2, LOGICAL_FEDERATION_CLUSTER_1_TABLE_2 + "_OFFLINE",
        LOGICAL_FEDERATION_CLUSTER_2_TABLE_2 + "_OFFLINE");
    Thread.sleep(2000);
    String joinQuery = "SET enableFederation=true; SET useMultistageEngine=true; "
        + "SELECT t1." + JOIN_COLUMN + ", COUNT(*) as count FROM " + LOGICAL_TABLE_NAME + " t1 "
        + "JOIN " + LOGICAL_TABLE_NAME_2 + " t2 ON t1." + JOIN_COLUMN + " = t2." + JOIN_COLUMN + " "
        + "GROUP BY t1." + JOIN_COLUMN + " LIMIT 20";
    String result = executeQuery(joinQuery, _cluster2);
    assertNotNull(result);
    assertTrue(result.contains("resultTable"));
    assertResultRows(result);
  }

  @Test
  public void testCluster1IndependentOperation() throws Exception {
    setupCluster1Complete();
    dropTableAndSchemaIfExists(FEDERATION_TABLE, _cluster1._controllerBaseApiUrl);
    createSchemaAndTableForCluster(FEDERATION_TABLE, _cluster1._controllerBaseApiUrl);
    assertTrue(sendGetRequest(_cluster1._controllerBaseApiUrl + "/tables").contains(FEDERATION_TABLE));
  }
}
