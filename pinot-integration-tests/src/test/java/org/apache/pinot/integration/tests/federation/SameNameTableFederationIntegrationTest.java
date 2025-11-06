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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class SameNameTableFederationIntegrationTest extends BaseDualIsolatedClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SameNameTableFederationIntegrationTest.class);

  protected static final String SHARED_TABLE_NAME = "shared_events";
  protected static final String SHARED_TABLE_NAME_2 = "shared_events_2";
  protected static final String LOGICAL_FEDERATED_TABLE = "federated_events";
  protected static final String LOGICAL_FEDERATED_TABLE_2 = "federated_events_2";
  protected static final int CLUSTER_1_TABLE_SIZE = 2000;
  protected static final int CLUSTER_2_TABLE_SIZE = 1000;
  protected static final int TOTAL_FEDERATED_SIZE = CLUSTER_1_TABLE_SIZE + CLUSTER_2_TABLE_SIZE;

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
  public void testSameNameTableFederationSSE() throws Exception {
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME);
    cleanSegmentDirs();
    loadDataIntoCluster(createAvroData(CLUSTER_1_TABLE_SIZE, 1), SHARED_TABLE_NAME, _cluster1);
    loadDataIntoCluster(createAvroData(CLUSTER_2_TABLE_SIZE, 2), SHARED_TABLE_NAME, _cluster2);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster1, false), CLUSTER_1_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster2, false), CLUSTER_2_TABLE_SIZE);
    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE, SHARED_TABLE_NAME);
    Thread.sleep(3000);
    assertEquals(getCount(LOGICAL_FEDERATED_TABLE, _cluster1, true), TOTAL_FEDERATED_SIZE);
    assertEquals(getCount(LOGICAL_FEDERATED_TABLE, _cluster2, true), TOTAL_FEDERATED_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster1, false), CLUSTER_1_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster2, false), CLUSTER_2_TABLE_SIZE);
  }

  @Test
  public void testSameNameTableFederationMSE() throws Exception {
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME);
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME_2);
    cleanSegmentDirs();
    loadDataIntoCluster(createAvroDataMultipleSegments(CLUSTER_1_TABLE_SIZE, 1, SEGMENTS_PER_CLUSTER),
        SHARED_TABLE_NAME, _cluster1);
    loadDataIntoCluster(createAvroDataMultipleSegments(CLUSTER_2_TABLE_SIZE, 2, SEGMENTS_PER_CLUSTER),
        SHARED_TABLE_NAME, _cluster2);
    loadDataIntoCluster(createAvroDataMultipleSegments(CLUSTER_1_TABLE_SIZE, 1, SEGMENTS_PER_CLUSTER),
        SHARED_TABLE_NAME_2, _cluster1);
    loadDataIntoCluster(createAvroDataMultipleSegments(CLUSTER_2_TABLE_SIZE, 2, SEGMENTS_PER_CLUSTER),
        SHARED_TABLE_NAME_2, _cluster2);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster1, false), CLUSTER_1_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster2, false), CLUSTER_2_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME_2, _cluster1, false), CLUSTER_1_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME_2, _cluster2, false), CLUSTER_2_TABLE_SIZE);
    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE, SHARED_TABLE_NAME);
    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE_2, SHARED_TABLE_NAME_2);
    Thread.sleep(3000);
    String joinQuery = "SET enableFederation=true; SET useMultistageEngine=true; "
        + "SELECT t1." + JOIN_COLUMN + ", COUNT(*) as count FROM " + LOGICAL_FEDERATED_TABLE + " t1 "
        + "JOIN " + LOGICAL_FEDERATED_TABLE_2 + " t2 ON t1." + JOIN_COLUMN + " = t2." + JOIN_COLUMN + " "
        + "GROUP BY t1." + JOIN_COLUMN + " LIMIT 20";
    String result = executeQuery(joinQuery, _cluster1);
    assertNotNull(result);
    assertTrue(result.contains("resultTable"));
    assertResultRows(result);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster1, false), CLUSTER_1_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster2, false), CLUSTER_2_TABLE_SIZE);
    assertEquals(parseCountResult(executeQuery(
        "SET enableFederation=false; SET useMultistageEngine=true; SELECT COUNT(*) as count FROM " + SHARED_TABLE_NAME,
        _cluster1)), CLUSTER_1_TABLE_SIZE);
  }

  @Test
  public void testPhysicalAndLogicalTableIsolation() throws Exception {
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME);
    cleanSegmentDirs();
    loadDataIntoCluster(createAvroData(CLUSTER_1_TABLE_SIZE, 1), SHARED_TABLE_NAME, _cluster1);
    loadDataIntoCluster(createAvroData(CLUSTER_2_TABLE_SIZE, 2), SHARED_TABLE_NAME, _cluster2);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster1, false), CLUSTER_1_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster2, false), CLUSTER_2_TABLE_SIZE);
    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE, SHARED_TABLE_NAME);
    Thread.sleep(3000);
    assertEquals(getCount(LOGICAL_FEDERATED_TABLE, _cluster1, true), TOTAL_FEDERATED_SIZE);
    assertEquals(getCount(LOGICAL_FEDERATED_TABLE, _cluster2, true), TOTAL_FEDERATED_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster1, false), CLUSTER_1_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME, _cluster2, false), CLUSTER_2_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME + "_OFFLINE", _cluster1, false), CLUSTER_1_TABLE_SIZE);
    assertEquals(getCount(SHARED_TABLE_NAME + "_OFFLINE", _cluster2, false), CLUSTER_2_TABLE_SIZE);
  }

  @Test
  public void testAggregateQueriesIsolation() throws Exception {
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME);
    cleanSegmentDirs();
    loadDataIntoCluster(createAvroData(CLUSTER_1_TABLE_SIZE, 1), SHARED_TABLE_NAME, _cluster1);
    loadDataIntoCluster(createAvroData(CLUSTER_2_TABLE_SIZE, 2), SHARED_TABLE_NAME, _cluster2);
    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE, SHARED_TABLE_NAME);
    Thread.sleep(3000);
    testAggregateQuery("COUNT(*) as total", CLUSTER_1_TABLE_SIZE, CLUSTER_2_TABLE_SIZE);
    testQueryOnBothTables("SELECT MAX(DaysSinceEpoch) as max_days FROM ");
    testQueryOnBothTables("SELECT MIN(DaysSinceEpoch) as min_days FROM ");
  }

  private void setupSameNamedTableInBothClusters(String tableName) throws Exception {
    dropLogicalTableIfExists(LOGICAL_FEDERATED_TABLE, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_FEDERATED_TABLE, _cluster2._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_FEDERATED_TABLE_2, _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(LOGICAL_FEDERATED_TABLE_2, _cluster2._controllerBaseApiUrl);
    dropTableAndSchemaIfExists(tableName, _cluster1._controllerBaseApiUrl);
    dropTableAndSchemaIfExists(tableName, _cluster2._controllerBaseApiUrl);
    createSchemaAndTableForCluster(tableName, _cluster1._controllerBaseApiUrl);
    createSchemaAndTableForCluster(tableName, _cluster2._controllerBaseApiUrl);
  }

  private void createLogicalTableForSameNamedPhysicalTables(String logicalTableName,
      String physicalTableName) throws Exception {
    String tableWithSuffix = physicalTableName + "_OFFLINE";
    java.util.Map<String, org.apache.pinot.spi.data.PhysicalTableConfig> configMap =
        java.util.Map.of(tableWithSuffix, new org.apache.pinot.spi.data.PhysicalTableConfig(true));
    createLogicalTable(CLUSTER_1_NAME, SCHEMA_FILE, configMap, DEFAULT_TENANT,
        _cluster1._controllerBaseApiUrl, logicalTableName, tableWithSuffix, null);
    createLogicalTable(CLUSTER_2_NAME, SCHEMA_FILE, configMap, DEFAULT_TENANT,
        _cluster2._controllerBaseApiUrl, logicalTableName, tableWithSuffix, null);
  }

  private void testAggregateQuery(String selectClause, long expectedCluster1, long expectedCluster2)
      throws Exception {
    String query = "SELECT " + selectClause + " FROM " + SHARED_TABLE_NAME;
    assertEquals(parseCountResult(executeQuery(query, _cluster1)), expectedCluster1);
    assertEquals(parseCountResult(executeQuery(query, _cluster2)), expectedCluster2);
  }

  private void testQueryOnBothTables(String queryPrefix) throws Exception {
    assertNotNull(executeQuery(queryPrefix + SHARED_TABLE_NAME, _cluster1));
    assertNotNull(executeQuery(queryPrefix + SHARED_TABLE_NAME, _cluster2));
    String logicalResult = executeQuery(queryPrefix + LOGICAL_FEDERATED_TABLE, _cluster1);
    assertNotNull(logicalResult);
    assertTrue(logicalResult.contains("resultTable"));
  }
}
