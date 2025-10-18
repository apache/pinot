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

/**
 * Integration test for federation with same table names in both clusters.
 *
 * This test verifies that:
 * 1. Both clusters can have tables with identical names but different data sizes
 * 2. Direct queries to physical tables return only local cluster data
 * 3. Queries through logical tables return federated data from both clusters
 * 4. Both SSE (Single-Stage Engine) and MSE (Multi-Stage Engine) work correctly
 *
 * Test scenario:
 * - Cluster 1: "shared_events" table with 2000 records
 * - Cluster 2: "shared_events" table with 1000 records
 * - Logical table: "federated_events" that federates both
 *
 * Expected behavior:
 * - Query "shared_events" on Cluster 1 → 2000 records
 * - Query "shared_events" on Cluster 2 → 1000 records
 * - Query "federated_events" on either cluster → 3000 records (federated)
 */
public class SameNameTableFederationIntegrationTest extends BaseDualIsolatedClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SameNameTableFederationIntegrationTest.class);

  // Table names - both clusters use the same physical table name
  protected static final String SHARED_TABLE_NAME = "shared_events";
  protected static final String SHARED_TABLE_NAME_2 = "shared_events_2";

  // Logical table names for federation
  protected static final String LOGICAL_FEDERATED_TABLE = "federated_events";
  protected static final String LOGICAL_FEDERATED_TABLE_2 = "federated_events_2";

  // Different sizes for the same-named tables
  protected static final int CLUSTER_1_TABLE_SIZE = 2000;
  protected static final int CLUSTER_2_TABLE_SIZE = 1000;
  protected static final int TOTAL_FEDERATED_SIZE = CLUSTER_1_TABLE_SIZE + CLUSTER_2_TABLE_SIZE;

  @BeforeClass
  public void setUp() throws Exception {
    LOGGER.info("Setting up dual isolated Pinot clusters with same-named tables");

    // Initialize cluster components
    _cluster1 = new ClusterComponents();
    _cluster2 = new ClusterComponents();

    // Create test directories
    setupDirectories();

    // Start both clusters
    startZookeeper(_cluster1);
    startZookeeper(_cluster2);

    startControllerInit(_cluster1, CLUSTER_1_CONFIG);
    startControllerInit(_cluster2, CLUSTER_2_CONFIG);

    // Start clusters with cross-references for federation
    startCluster(_cluster1, _cluster2, CLUSTER_1_CONFIG);
    startCluster(_cluster2, _cluster1, CLUSTER_2_CONFIG);

    // Setup connections
    setupPinotConnections();

    LOGGER.info("Dual isolated Pinot clusters with same-named tables setup completed");
  }

  /**
   * Test SSE (Single-Stage Engine) with same-named tables
   *
   * Tests that:
   * 1. Each cluster returns only its local data when querying the physical table directly
   * 2. Querying through the logical table returns federated data from both clusters
   */
  @Test
  public void testSameNameTableFederationSSE() throws Exception {
    LOGGER.info("Starting SSE test with same-named tables in both clusters");

    // Step 1: Create the same table name in both clusters
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME);

    // Step 2: Load different amounts of data into each cluster's table
    cleanSegmentDirs();

    _cluster1AvroFiles = createAvroData(CLUSTER_1_TABLE_SIZE, 1);
    _cluster2AvroFiles = createAvroData(CLUSTER_2_TABLE_SIZE, 2);

    loadDataIntoCluster(_cluster1AvroFiles, SHARED_TABLE_NAME, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles, SHARED_TABLE_NAME, _cluster2);

    // Step 3: Verify each cluster returns only its local data when querying the physical table
    LOGGER.info("Verifying physical table queries return only local data");

    long cluster1LocalCount = getCount(SHARED_TABLE_NAME, _cluster1);
    long cluster2LocalCount = getCount(SHARED_TABLE_NAME, _cluster2);

    LOGGER.info("Cluster 1 local count for '{}': {}", SHARED_TABLE_NAME, cluster1LocalCount);
    LOGGER.info("Cluster 2 local count for '{}': {}", SHARED_TABLE_NAME, cluster2LocalCount);

    assertEquals(cluster1LocalCount, CLUSTER_1_TABLE_SIZE,
        "Cluster 1 should return only its local data");
    assertEquals(cluster2LocalCount, CLUSTER_2_TABLE_SIZE,
        "Cluster 2 should return only its local data");

    // Step 4: Create logical table for federation
    LOGGER.info("Creating logical table for federation");

    // Note: When physical tables have the same name, we need to create separate logical table configs
    // for each cluster that references its local physical table
    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE, SHARED_TABLE_NAME);

    // Wait for routing to be established
    Thread.sleep(3000);

    // Step 5: Verify logical table returns federated data from both clusters
    LOGGER.info("Verifying logical table queries return federated data");

    long cluster1FederatedCount = getCount(LOGICAL_FEDERATED_TABLE, _cluster1);
    long cluster2FederatedCount = getCount(LOGICAL_FEDERATED_TABLE, _cluster2);

    LOGGER.info("Cluster 1 federated count for '{}': {}", LOGICAL_FEDERATED_TABLE, cluster1FederatedCount);
    LOGGER.info("Cluster 2 federated count for '{}': {}", LOGICAL_FEDERATED_TABLE, cluster2FederatedCount);

    assertEquals(cluster1FederatedCount, TOTAL_FEDERATED_SIZE,
        "Cluster 1 should return federated data from both clusters through logical table");
    assertEquals(cluster2FederatedCount, TOTAL_FEDERATED_SIZE,
        "Cluster 2 should return federated data from both clusters through logical table");

    // Step 6: Verify physical table queries still return only local data
    LOGGER.info("Re-verifying physical table queries still return only local data");

    long cluster1LocalCountAfter = getCount(SHARED_TABLE_NAME, _cluster1);
    long cluster2LocalCountAfter = getCount(SHARED_TABLE_NAME, _cluster2);

    assertEquals(cluster1LocalCountAfter, CLUSTER_1_TABLE_SIZE,
        "Cluster 1 physical table should still return only local data");
    assertEquals(cluster2LocalCountAfter, CLUSTER_2_TABLE_SIZE,
        "Cluster 2 physical table should still return only local data");

    LOGGER.info("SSE test with same-named tables completed successfully");
  }

  /**
   * Test MSE (Multi-Stage Engine) with same-named tables
   *
   * Tests that:
   * 1. Each cluster returns only its local data when querying the physical table directly
   * 2. Cross-cluster joins work correctly through logical tables
   * 3. Physical table queries remain isolated even after federation setup
   */
  @Test
  public void testSameNameTableFederationMSE() throws Exception {
    LOGGER.info("Starting MSE test with same-named tables for join queries");

    // Step 1: Create two sets of same-named tables for join
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME);
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME_2);

    // Step 2: Load data into both tables in both clusters
    cleanSegmentDirs();

    _cluster1AvroFiles = createAvroDataMultipleSegments(CLUSTER_1_TABLE_SIZE, 1, SEGMENTS_PER_CLUSTER);
    _cluster2AvroFiles = createAvroDataMultipleSegments(CLUSTER_2_TABLE_SIZE, 2, SEGMENTS_PER_CLUSTER);
    _cluster1AvroFiles2 = createAvroDataMultipleSegments(CLUSTER_1_TABLE_SIZE, 1, SEGMENTS_PER_CLUSTER);
    _cluster2AvroFiles2 = createAvroDataMultipleSegments(CLUSTER_2_TABLE_SIZE, 2, SEGMENTS_PER_CLUSTER);

    loadDataIntoCluster(_cluster1AvroFiles, SHARED_TABLE_NAME, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles, SHARED_TABLE_NAME, _cluster2);
    loadDataIntoCluster(_cluster1AvroFiles2, SHARED_TABLE_NAME_2, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles2, SHARED_TABLE_NAME_2, _cluster2);

    // Step 3: Verify local counts before federation
    LOGGER.info("Verifying physical table counts before federation");

    long cluster1Table1Count = getCount(SHARED_TABLE_NAME, _cluster1);
    long cluster2Table1Count = getCount(SHARED_TABLE_NAME, _cluster2);
    long cluster1Table2Count = getCount(SHARED_TABLE_NAME_2, _cluster1);
    long cluster2Table2Count = getCount(SHARED_TABLE_NAME_2, _cluster2);

    assertEquals(cluster1Table1Count, CLUSTER_1_TABLE_SIZE);
    assertEquals(cluster2Table1Count, CLUSTER_2_TABLE_SIZE);
    assertEquals(cluster1Table2Count, CLUSTER_1_TABLE_SIZE);
    assertEquals(cluster2Table2Count, CLUSTER_2_TABLE_SIZE);

    // Step 4: Create logical tables for federation
    LOGGER.info("Creating logical tables for federated join");

    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE, SHARED_TABLE_NAME);
    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE_2, SHARED_TABLE_NAME_2);

    // Wait for routing to be established
    Thread.sleep(3000);

    // Step 5: Test MSE join query on logical tables
    LOGGER.info("Testing MSE join query on logical tables");

    String mseJoinQuery = "SET useMultistageEngine=true; "
        + "SELECT t1." + JOIN_COLUMN + ", COUNT(*) as count "
        + "FROM " + LOGICAL_FEDERATED_TABLE + " t1 "
        + "JOIN " + LOGICAL_FEDERATED_TABLE_2 + " t2 "
        + "ON t1." + JOIN_COLUMN + " = t2." + JOIN_COLUMN + " "
        + "GROUP BY t1." + JOIN_COLUMN + " "
        + "LIMIT 20";

    String joinResult = executeQuery(mseJoinQuery, _cluster1);
    assertNotNull(joinResult, "MSE join query should return results");
    assertTrue(joinResult.contains("resultTable"), "Join result should contain resultTable");
    LOGGER.info("MSE join query result: {}", joinResult);
    assertResultRows(joinResult);

    // Step 6: Verify physical tables still return only local data
    LOGGER.info("Verifying physical tables still return only local data after federation");

    long cluster1Table1CountAfter = getCount(SHARED_TABLE_NAME, _cluster1);
    long cluster2Table1CountAfter = getCount(SHARED_TABLE_NAME, _cluster2);

    assertEquals(cluster1Table1CountAfter, CLUSTER_1_TABLE_SIZE,
        "Physical table in Cluster 1 should still return only local data");
    assertEquals(cluster2Table1CountAfter, CLUSTER_2_TABLE_SIZE,
        "Physical table in Cluster 2 should still return only local data");

    // Step 7: Verify local MSE query on physical tables (no federation)
    LOGGER.info("Testing local MSE query on physical tables");

    String localMseQuery = "SET useMultistageEngine=true; "
        + "SELECT COUNT(*) as count FROM " + SHARED_TABLE_NAME;

    String cluster1LocalMseResult = executeQuery(localMseQuery, _cluster1);
    long cluster1LocalMseCount = parseCountResult(cluster1LocalMseResult);

    assertEquals(cluster1LocalMseCount, CLUSTER_1_TABLE_SIZE,
        "Local MSE query should return only local data");

    LOGGER.info("MSE test with same-named tables completed successfully");
  }

  /**
   * Test that validates the isolation and federation behavior comprehensively
   */
  @Test
  public void testPhysicalAndLogicalTableIsolation() throws Exception {
    LOGGER.info("Testing comprehensive isolation between physical and logical tables");

    // Setup
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME);
    cleanSegmentDirs();

    _cluster1AvroFiles = createAvroData(CLUSTER_1_TABLE_SIZE, 1);
    _cluster2AvroFiles = createAvroData(CLUSTER_2_TABLE_SIZE, 2);

    loadDataIntoCluster(_cluster1AvroFiles, SHARED_TABLE_NAME, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles, SHARED_TABLE_NAME, _cluster2);

    // Test 1: Query physical table before logical table creation
    LOGGER.info("Test 1: Physical table queries before logical table exists");

    long preLogicalCluster1Count = getCount(SHARED_TABLE_NAME, _cluster1);
    long preLogicalCluster2Count = getCount(SHARED_TABLE_NAME, _cluster2);

    assertEquals(preLogicalCluster1Count, CLUSTER_1_TABLE_SIZE);
    assertEquals(preLogicalCluster2Count, CLUSTER_2_TABLE_SIZE);

    // Test 2: Create logical table
    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE, SHARED_TABLE_NAME);
    Thread.sleep(3000);

    // Test 3: Query logical table
    LOGGER.info("Test 2: Logical table queries return federated data");

    long logicalCluster1Count = getCount(LOGICAL_FEDERATED_TABLE, _cluster1);
    long logicalCluster2Count = getCount(LOGICAL_FEDERATED_TABLE, _cluster2);

    assertEquals(logicalCluster1Count, TOTAL_FEDERATED_SIZE);
    assertEquals(logicalCluster2Count, TOTAL_FEDERATED_SIZE);

    // Test 4: Query physical table after logical table creation
    LOGGER.info("Test 3: Physical table queries still return local data after logical table creation");

    long postLogicalCluster1Count = getCount(SHARED_TABLE_NAME, _cluster1);
    long postLogicalCluster2Count = getCount(SHARED_TABLE_NAME, _cluster2);

    assertEquals(postLogicalCluster1Count, CLUSTER_1_TABLE_SIZE,
        "Physical table should remain isolated after logical table creation");
    assertEquals(postLogicalCluster2Count, CLUSTER_2_TABLE_SIZE,
        "Physical table should remain isolated after logical table creation");

    // Test 5: Test with explicit table type suffix
    LOGGER.info("Test 4: Query with explicit _OFFLINE suffix");

    long cluster1OfflineCount = getCount(SHARED_TABLE_NAME + "_OFFLINE", _cluster1);
    long cluster2OfflineCount = getCount(SHARED_TABLE_NAME + "_OFFLINE", _cluster2);

    assertEquals(cluster1OfflineCount, CLUSTER_1_TABLE_SIZE,
        "Physical table with _OFFLINE suffix should return local data");
    assertEquals(cluster2OfflineCount, CLUSTER_2_TABLE_SIZE,
        "Physical table with _OFFLINE suffix should return local data");

    LOGGER.info("Comprehensive isolation test completed successfully");
  }

  /**
   * Test aggregate queries on both physical and logical tables
   */
  @Test
  public void testAggregateQueriesIsolation() throws Exception {
    LOGGER.info("Testing aggregate queries on physical vs logical tables");

    // Setup
    setupSameNamedTableInBothClusters(SHARED_TABLE_NAME);
    cleanSegmentDirs();

    _cluster1AvroFiles = createAvroData(CLUSTER_1_TABLE_SIZE, 1);
    _cluster2AvroFiles = createAvroData(CLUSTER_2_TABLE_SIZE, 2);

    loadDataIntoCluster(_cluster1AvroFiles, SHARED_TABLE_NAME, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles, SHARED_TABLE_NAME, _cluster2);

    createLogicalTableForSameNamedPhysicalTables(LOGICAL_FEDERATED_TABLE, SHARED_TABLE_NAME);
    Thread.sleep(3000);

    // Test COUNT(*)
    LOGGER.info("Testing COUNT(*) queries");
    testAggregateQuery("COUNT(*) as total", "total", CLUSTER_1_TABLE_SIZE, CLUSTER_2_TABLE_SIZE);

    // Test MAX
    LOGGER.info("Testing MAX queries");
    String maxQuery = "SELECT MAX(DaysSinceEpoch) as max_days FROM ";
    testQueryOnBothTables(maxQuery);

    // Test MIN
    LOGGER.info("Testing MIN queries");
    String minQuery = "SELECT MIN(DaysSinceEpoch) as min_days FROM ";
    testQueryOnBothTables(minQuery);

    LOGGER.info("Aggregate queries isolation test completed successfully");
  }

  // ==================== Helper Methods ====================

  /**
   * Creates the same table name in both clusters with identical schema
   */
  private void setupSameNamedTableInBothClusters(String tableName) throws Exception {
    LOGGER.info("Setting up table '{}' in both clusters", tableName);

    // Drop existing tables if any
    dropTableAndSchemaIfExists(tableName, _cluster1._controllerBaseApiUrl);
    dropTableAndSchemaIfExists(tableName, _cluster2._controllerBaseApiUrl);

    // Create schema and table in both clusters
    createSchemaAndTableForCluster(tableName, _cluster1._controllerBaseApiUrl);
    createSchemaAndTableForCluster(tableName, _cluster2._controllerBaseApiUrl);

    LOGGER.info("Table '{}' created in both clusters", tableName);
  }

  /**
   * Tests an aggregate query and verifies isolation
   */
  private void testAggregateQuery(String selectClause, String resultColumn,
      long expectedCluster1, long expectedCluster2) throws Exception {

    // Query physical tables
    String physicalQuery = "SELECT " + selectClause + " FROM " + SHARED_TABLE_NAME;

    String cluster1Result = executeQuery(physicalQuery, _cluster1);
    String cluster2Result = executeQuery(physicalQuery, _cluster2);

    long cluster1Value = parseCountResult(cluster1Result);
    long cluster2Value = parseCountResult(cluster2Result);

    assertEquals(cluster1Value, expectedCluster1,
        "Physical table query on Cluster 1 should return local value");
    assertEquals(cluster2Value, expectedCluster2,
        "Physical table query on Cluster 2 should return local value");

    // Query logical table
    String logicalQuery = "SELECT " + selectClause + " FROM " + LOGICAL_FEDERATED_TABLE;

    String logicalResult = executeQuery(logicalQuery, _cluster1);
    long logicalValue = parseCountResult(logicalResult);

    LOGGER.info("Logical table {} returned: {}", selectClause, logicalValue);
  }

  /**
   * Tests a query on both physical and logical tables
   */
  private void testQueryOnBothTables(String queryPrefix) throws Exception {
    // Query physical tables
    String physicalQuery1 = queryPrefix + SHARED_TABLE_NAME;
    String physicalQuery2 = queryPrefix + SHARED_TABLE_NAME;

    String cluster1Physical = executeQuery(physicalQuery1, _cluster1);
    String cluster2Physical = executeQuery(physicalQuery2, _cluster2);

    assertNotNull(cluster1Physical);
    assertNotNull(cluster2Physical);

    // Query logical table
    String logicalQuery = queryPrefix + LOGICAL_FEDERATED_TABLE;
    String logicalResult = executeQuery(logicalQuery, _cluster1);

    assertNotNull(logicalResult);
    assertTrue(logicalResult.contains("resultTable"));
  }
}
