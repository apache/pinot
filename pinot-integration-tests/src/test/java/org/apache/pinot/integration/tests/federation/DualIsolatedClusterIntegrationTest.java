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

public class DualIsolatedClusterIntegrationTest extends BaseDualIsolatedClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(DualIsolatedClusterIntegrationTest.class);

  @BeforeClass
  public void setUp() throws Exception {
    _cluster1 = new ClusterComponents();
    _cluster2 = new ClusterComponents();
    setupDirectories();
    startZookeeper(_cluster1);
    startZookeeper(_cluster2);
    startControllerInit(_cluster1, CLUSTER_1_CONFIG);
    startControllerInit(_cluster2, CLUSTER_2_CONFIG);
    startCluster(_cluster1, _cluster2, CLUSTER_1_CONFIG);
    startCluster(_cluster2, _cluster1, CLUSTER_2_CONFIG);
    setupPinotConnections();
  }

  @Test
  public void testClusterIsolation() throws Exception {
    setupFederationTable();
    assertResourceExists("/tables", FEDERATION_TABLE);
    assertResourceExists("/schemas", FEDERATION_TABLE);
  }

  @Test
  public void testIndependentOperations() throws Exception {
    setupFederationTable();
    String cluster1Info = ControllerTest.sendGetRequest(
        _cluster1._controllerBaseApiUrl + "/tables/" + FEDERATION_TABLE);
    String cluster2Info = ControllerTest.sendGetRequest(
        _cluster2._controllerBaseApiUrl + "/tables/" + FEDERATION_TABLE);
    assertNotNull(cluster1Info);
    assertNotNull(cluster2Info);
    assertTrue(cluster1Info.contains(FEDERATION_TABLE));
    assertTrue(cluster2Info.contains(FEDERATION_TABLE));
  }

  private void assertResourceExists(String endpoint, String resourceName) throws Exception {
    String c1Response = ControllerTest.sendGetRequest(_cluster1._controllerBaseApiUrl + endpoint);
    String c2Response = ControllerTest.sendGetRequest(_cluster2._controllerBaseApiUrl + endpoint);
    assertTrue(c1Response.contains(resourceName));
    assertTrue(c2Response.contains(resourceName));
  }

  @Test
  public void testLogicalFederationTwoOfflineTablesSSE() throws Exception {
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster2._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster2._controllerBaseApiUrl);
    setupFirstLogicalFederatedTable();
    createLogicalTableOnBothClusters(LOGICAL_TABLE_NAME,
        LOGICAL_FEDERATION_CLUSTER_1_TABLE, LOGICAL_FEDERATION_CLUSTER_2_TABLE);
    cleanSegmentDirs();
    _cluster1AvroFiles = createAvroData(CLUSTER_1_SIZE, 1);
    _cluster2AvroFiles = createAvroData(CLUSTER_2_SIZE, 2);
    loadDataIntoCluster(_cluster1AvroFiles, LOGICAL_FEDERATION_CLUSTER_1_TABLE, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles, LOGICAL_FEDERATION_CLUSTER_2_TABLE, _cluster2);
    long expectedTotal = CLUSTER_1_SIZE + CLUSTER_2_SIZE;
    assertEquals(getCount(LOGICAL_TABLE_NAME, _cluster1, true), expectedTotal);
    assertEquals(getCount(LOGICAL_TABLE_NAME, _cluster2, true), expectedTotal);
  }

  @Test
  public void testLogicalFederationTwoLogicalTablesMSE() throws Exception {
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME, _cluster2._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_TABLE_NAME_2, _cluster2._controllerBaseApiUrl);
    setupFirstLogicalFederatedTable();
    setupSecondLogicalFederatedTable();
    createLogicalTableOnBothClusters(LOGICAL_TABLE_NAME,
        LOGICAL_FEDERATION_CLUSTER_1_TABLE, LOGICAL_FEDERATION_CLUSTER_2_TABLE);
    createLogicalTableOnBothClusters(LOGICAL_TABLE_NAME_2,
        LOGICAL_FEDERATION_CLUSTER_1_TABLE_2, LOGICAL_FEDERATION_CLUSTER_2_TABLE_2);
    cleanSegmentDirs();
    loadDataIntoCluster(createAvroData(CLUSTER_1_SIZE, 1), LOGICAL_FEDERATION_CLUSTER_1_TABLE, _cluster1);
    loadDataIntoCluster(createAvroData(CLUSTER_2_SIZE, 2), LOGICAL_FEDERATION_CLUSTER_2_TABLE, _cluster2);
    loadDataIntoCluster(createAvroDataMultipleSegments(CLUSTER_1_SIZE, 1, SEGMENTS_PER_CLUSTER),
        LOGICAL_FEDERATION_CLUSTER_1_TABLE_2, _cluster1);
    loadDataIntoCluster(createAvroDataMultipleSegments(CLUSTER_2_SIZE, 2, SEGMENTS_PER_CLUSTER),
        LOGICAL_FEDERATION_CLUSTER_2_TABLE_2, _cluster2);
    String joinQuery = "SET useMultistageEngine=true; SET enableFederation=true; "
        + "SELECT t1." + JOIN_COLUMN + ", COUNT(*) as count FROM " + LOGICAL_TABLE_NAME + " t1 "
        + "JOIN " + LOGICAL_TABLE_NAME_2 + " t2 ON t1." + JOIN_COLUMN + " = t2." + JOIN_COLUMN + " "
        + "GROUP BY t1." + JOIN_COLUMN + " LIMIT 20";
    String result = executeQuery(joinQuery, _cluster1);
    assertNotNull(result);
    assertTrue(result.contains("resultTable"));
    assertResultRows(result);
  }

  @Test
  public void testDataGeneration() throws Exception {
    validateAvroFiles(createAvroData(CLUSTER_1_SIZE, 1));
    validateAvroFiles(createAvroData(CLUSTER_2_SIZE, 2));
  }

  private void validateAvroFiles(List<File> files) {
    for (File file : files) {
      assertTrue(file.exists());
      assertTrue(file.length() > 0);
    }
  }
}
