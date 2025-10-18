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
import java.util.List;
import org.apache.pinot.controller.helix.ControllerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration test that starts two completely isolated Pinot clusters in parallel.
 * Each cluster has its own Zookeeper, Controller, Broker, and Server.
 * The clusters are aware of each other and can perform federated queries.
 */
public class DualIsolatedClusterIntegrationTest extends BaseDualIsolatedClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(DualIsolatedClusterIntegrationTest.class);

  @BeforeClass
  public void setUp() throws Exception {
    LOGGER.info("Setting up dual isolated Pinot clusters in parallel");

    // Initialize cluster components
    _cluster1 = new ClusterComponents();
    _cluster2 = new ClusterComponents();

    // Create test directories
    setupDirectories();

    startZookeeper(_cluster1);
    startZookeeper(_cluster2);

    startControllerInit(_cluster1, CLUSTER_1_CONFIG);
    startControllerInit(_cluster2, CLUSTER_2_CONFIG);

    // Start clusters with cross-references (parallel setup)
    startCluster(_cluster1, _cluster2, CLUSTER_1_CONFIG);
    startCluster(_cluster2, _cluster1, CLUSTER_2_CONFIG);

    // Setup connections
    setupPinotConnections();

    LOGGER.info("Dual isolated Pinot clusters setup completed");
  }

  @Test
  public void testClusterIsolation() throws Exception {
    setupFederationTable();

    String cluster1Tables = ControllerTest.sendGetRequest(_cluster1._controllerBaseApiUrl + "/tables");
    String cluster2Tables = ControllerTest.sendGetRequest(_cluster2._controllerBaseApiUrl + "/tables");

    assertTrue(cluster1Tables.contains(FEDERATION_TABLE));
    assertTrue(cluster2Tables.contains(FEDERATION_TABLE));

    String cluster1Schemas = ControllerTest.sendGetRequest(_cluster1._controllerBaseApiUrl + "/schemas");
    String cluster2Schemas = ControllerTest.sendGetRequest(_cluster2._controllerBaseApiUrl + "/schemas");

    assertTrue(cluster1Schemas.contains(FEDERATION_TABLE));
    assertTrue(cluster2Schemas.contains(FEDERATION_TABLE));
  }

  @Test
  public void testIndependentOperations() throws Exception {
    setupFederationTable();

    String cluster1TableInfo = ControllerTest.sendGetRequest(
        _cluster1._controllerBaseApiUrl + "/tables/" + FEDERATION_TABLE);
    String cluster2TableInfo = ControllerTest.sendGetRequest(
        _cluster2._controllerBaseApiUrl + "/tables/" + FEDERATION_TABLE);

    assertNotNull(cluster1TableInfo);
    assertNotNull(cluster2TableInfo);
    assertTrue(cluster1TableInfo.contains(FEDERATION_TABLE));
    assertTrue(cluster2TableInfo.contains(FEDERATION_TABLE));
  }

  @Test
  public void testLogicalFederationTwoOfflineTablesSSE() throws Exception {
    // Clean up any logical tables from previous tests
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster2._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster2._controllerBaseApiUrl);

    setupFirstLogicalFederatedTable();
    createLogicalTableOnBothClusters(LOGICAL_TABLE_NAME,
        LOGICAL_FEDERATION_CLUSTER_1_TABLE, LOGICAL_FEDERATION_CLUSTER_2_TABLE);
    cleanSegmentDirs();

    // Generate and load data
    _cluster1AvroFiles = createAvroData(CLUSTER_1_SIZE, 1);
    _cluster2AvroFiles = createAvroData(CLUSTER_2_SIZE, 2);

    loadDataIntoCluster(_cluster1AvroFiles, LOGICAL_FEDERATION_CLUSTER_1_TABLE, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles, LOGICAL_FEDERATION_CLUSTER_2_TABLE, _cluster2);

    // Verify counts
    long cluster1Count = getCount(LOGICAL_TABLE_NAME, _cluster1);
    long cluster2Count = getCount(LOGICAL_TABLE_NAME, _cluster2);

    assertEquals(cluster1Count, CLUSTER_1_SIZE + CLUSTER_2_SIZE);
    assertEquals(cluster2Count, CLUSTER_2_SIZE + CLUSTER_1_SIZE);
  }

  @Test
  public void testLogicalFederationTwoLogicalTablesMSE() throws Exception {
    // Clean up any logical tables from previous tests
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster2._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster2._controllerBaseApiUrl);

    setupFirstLogicalFederatedTable();
    setupSecondLogicalFederatedTable();

    // Create first logical table
    createLogicalTableOnBothClusters(LOGICAL_TABLE_NAME,
        LOGICAL_FEDERATION_CLUSTER_1_TABLE, LOGICAL_FEDERATION_CLUSTER_2_TABLE);

    // Create second logical table
    createLogicalTableOnBothClusters(LOGICAL_TABLE_NAME_2,
        LOGICAL_FEDERATION_CLUSTER_1_TABLE_2, LOGICAL_FEDERATION_CLUSTER_2_TABLE_2);

    cleanSegmentDirs();

    // Generate and load data
    _cluster1AvroFiles = createAvroData(CLUSTER_1_SIZE, 1);
    _cluster2AvroFiles = createAvroData(CLUSTER_2_SIZE, 2);

    loadDataIntoCluster(_cluster1AvroFiles, LOGICAL_FEDERATION_CLUSTER_1_TABLE, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles, LOGICAL_FEDERATION_CLUSTER_2_TABLE, _cluster2);
    // Generate and load data for second logical table
    _cluster1AvroFiles2 = createAvroDataMultipleSegments(CLUSTER_1_SIZE, 1, SEGMENTS_PER_CLUSTER);
    _cluster2AvroFiles2 = createAvroDataMultipleSegments(CLUSTER_2_SIZE, 2, SEGMENTS_PER_CLUSTER);

    loadDataIntoCluster(_cluster1AvroFiles2, LOGICAL_FEDERATION_CLUSTER_1_TABLE_2, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles2, LOGICAL_FEDERATION_CLUSTER_2_TABLE_2, _cluster2);

    // Test join query with MSE
    String joinQuery = "SET useMultistageEngine=true; SELECT t1." + JOIN_COLUMN + ", COUNT(*) as count "
        + "FROM " + LOGICAL_TABLE_NAME + " t1 "
        + "JOIN " + LOGICAL_TABLE_NAME_2 + " t2 ON t1." + JOIN_COLUMN + " = t2." + JOIN_COLUMN + " "
        + "GROUP BY t1." + JOIN_COLUMN + " LIMIT 20";

    String result = executeQuery(joinQuery, _cluster1);
    assertNotNull(result);
    assertTrue(result.contains("resultTable"));
    System.out.println(result);
    assertResultRows(result);

    LOGGER.info("MSE Federation Join Test completed successfully");
  }

  @Test
  public void testDataGeneration() throws Exception {
    List<File> cluster1Files = createAvroData(CLUSTER_1_SIZE, 1);
    List<File> cluster2Files = createAvroData(CLUSTER_2_SIZE, 2);

    for (File file : cluster1Files) {
      assertTrue(file.exists());
      assertTrue(file.length() > 0);
    }

    for (File file : cluster2Files) {
      assertTrue(file.exists());
      assertTrue(file.length() > 0);
    }
  }
}
