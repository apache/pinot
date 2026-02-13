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
package org.apache.pinot.integration.tests.multicluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests for multi-cluster routing and federation.
 * Tests federation with different table names across clusters and various query modes.
 */
public class MultiClusterIntegrationTest extends BaseMultiClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiClusterIntegrationTest.class);

  // Logical tables are the federated tables that span across clusters
  protected String getLogicalTableName() {
    return "federated_table1";
  }

  protected String getLogicalTableName2() {
    return "federated_table2";
  }

  // Physical tables are the actual tables in each cluster
  protected String getPhysicalTable1InCluster1() {
    return "physical_table1_c1";
  }

  protected String getPhysicalTable1InCluster2() {
    return "physical_table1_c2";
  }

  protected String getPhysicalTable2InCluster1() {
    return "physical_table2_c1";
  }

  protected String getPhysicalTable2InCluster2() {
    return "physical_table2_c2";
  }

  @Test
  public void testMultiClusterBrokerStartsAndIsQueryable() throws Exception {
    LOGGER.info("Testing that multi-cluster broker starts successfully and is queryable");

    // Verify both clusters' brokers are running (MultiClusterHelixBrokerStarter)
    assertNotNull(_cluster1._brokerStarter, "Cluster 1 broker should be started");
    assertNotNull(_cluster2._brokerStarter, "Cluster 2 broker should be started");

    // Setup a test table on both clusters
    String testTableName = "multicluster_test_table";
    createSchemaAndTableOnBothClusters(testTableName);

    // Create and load test data into both clusters
    _cluster1AvroFiles = createAvroData(TABLE_SIZE_CLUSTER_1, 1);
    _cluster2AvroFiles = createAvroData(TABLE_SIZE_CLUSTER_2, 2);

    loadDataIntoCluster(_cluster1AvroFiles, testTableName, _cluster1);
    loadDataIntoCluster(_cluster2AvroFiles, testTableName, _cluster2);

    // Verify cluster 1 is queryable
    String query = "SELECT COUNT(*) FROM " + testTableName;
    String result1 = executeQuery(query, _cluster1);
    assertNotNull(result1, "Query result from cluster 1 should not be null");
    long count1 = parseCountResult(result1);
    assertEquals(count1, TABLE_SIZE_CLUSTER_1);

    // Verify cluster 2 is queryable
    String result2 = executeQuery(query, _cluster2);
    assertNotNull(result2, "Query result from cluster 2 should not be null");
    long count2 = parseCountResult(result2);
    assertEquals(count2, TABLE_SIZE_CLUSTER_2);

    LOGGER.info("Multi-cluster broker test passed: both clusters started and queryable");
  }

  @BeforeGroups("query")
  public void setupTablesForQueryTests() throws Exception {
    dropLogicalTableIfExists(getLogicalTableName(), _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(getLogicalTableName(), _cluster2._controllerBaseApiUrl);
    dropLogicalTableIfExists(getLogicalTableName2(), _cluster1._controllerBaseApiUrl);
    dropLogicalTableIfExists(getLogicalTableName2(), _cluster2._controllerBaseApiUrl);
    setupPhysicalTables();
    createLogicalTableOnBothClusters(getLogicalTableName(),
        getPhysicalTable1InCluster1(), getPhysicalTable1InCluster2());
    createLogicalTableOnBothClusters(getLogicalTableName2(),
        getPhysicalTable2InCluster1(), getPhysicalTable2InCluster2());
    cleanSegmentDirs();
    loadDataIntoCluster(createAvroData(TABLE_SIZE_CLUSTER_1, 1), getPhysicalTable1InCluster1(), _cluster1);
    loadDataIntoCluster(createAvroData(TABLE_SIZE_CLUSTER_2, 2), getPhysicalTable1InCluster2(), _cluster2);
    loadDataIntoCluster(createAvroDataMultipleSegments(TABLE_SIZE_CLUSTER_1, 1, SEGMENTS_PER_CLUSTER),
        getPhysicalTable2InCluster1(), _cluster1);
    loadDataIntoCluster(createAvroDataMultipleSegments(TABLE_SIZE_CLUSTER_2, 2, SEGMENTS_PER_CLUSTER),
        getPhysicalTable2InCluster2(), _cluster2);
  }

  @Test(dataProvider = "queryModes", groups = "query")
  public void testLogicalFederationQueries(String testName, String queryOptions, boolean isJoinQuery,
      int brokerPort, boolean expectUnavailableException)
      throws Exception {
    LOGGER.info("Running {} on broker port {} (expectUnavailableException={})",
        testName, brokerPort, expectUnavailableException);
    long expectedTotal = TABLE_SIZE_CLUSTER_1 + TABLE_SIZE_CLUSTER_2;

    if (isJoinQuery) {
      // Join query test
      String joinQuery = queryOptions
          + "SELECT t1." + JOIN_COLUMN + ", COUNT(*) as count FROM " + getLogicalTableName() + " t1 "
          + "JOIN " + getLogicalTableName2() + " t2 ON t1." + JOIN_COLUMN + " = t2." + JOIN_COLUMN + " "
          + "GROUP BY t1." + JOIN_COLUMN + " LIMIT 20";
      String result = executeQueryOnBrokerPort(joinQuery, brokerPort);
      assertNotNull(result);
      assertTrue(result.contains("resultTable"), "Expected resultTable in response: " + result);
      assertResultRows(result);
      verifyUnavailableClusterException(result, expectUnavailableException);
    }

    // Count query test (all modes)
    String countQuery = queryOptions + "SELECT COUNT(*) as count FROM " + getLogicalTableName();
    String countResult = executeQueryOnBrokerPort(countQuery, brokerPort);
    assertEquals(parseCountResult(countResult), expectedTotal);
    verifyUnavailableClusterException(countResult, expectUnavailableException);
  }

  @Test(dataProvider = "queryModesWithoutMultiClusterOption", groups = "query")
  public void testQueriesWithoutMultiClusterOptions(String testName, String queryOptions, boolean isJoinQuery,
      int brokerPort) throws Exception {
    LOGGER.info("Running {} on broker port {} (expecting NO federation)", testName, brokerPort);

    // When enableMultiClusterRouting is NOT set, only local cluster data should be returned
    long expectedLocalCount = TABLE_SIZE_CLUSTER_1;

    if (isJoinQuery) {
      // Join query test - should only execute locally
      String joinQuery = queryOptions
          + "SELECT t1." + JOIN_COLUMN + ", COUNT(*) as count FROM " + getLogicalTableName() + " t1 "
          + "JOIN " + getLogicalTableName2() + " t2 ON t1." + JOIN_COLUMN + " = t2." + JOIN_COLUMN + " "
          + "GROUP BY t1." + JOIN_COLUMN + " LIMIT 20";
      String result = executeQueryOnBrokerPort(joinQuery, brokerPort);
      assertNotNull(result);
      assertTrue(result.contains("resultTable"), "Expected resultTable in response: " + result);

      // Verify no federation occurred by checking that only local cluster data is present
      JsonNode rows = JsonMapper.builder().build().readTree(result).get("resultTable").get("rows");
      assertNotNull(rows, "Result rows should exist");

      for (JsonNode row : rows) {
        int count = row.get(1).asInt();
        assertTrue(count == 1,
            "Expected local-only join count of 1, but got " + count + " for row: " + row);
      }
    }

    // Count query test - should only count local cluster data
    String countQuery = queryOptions + "SELECT COUNT(*) as count FROM " + getLogicalTableName();
    String countResult = executeQueryOnBrokerPort(countQuery, brokerPort);
    long actualCount = parseCountResult(countResult);

    assertEquals(actualCount, expectedLocalCount,
        "Expected local cluster count of " + expectedLocalCount + " but got " + actualCount);

    // Verify no federation-related exceptions
    JsonNode resultJson = JsonMapper.builder().build().readTree(countResult);
    JsonNode exceptions = resultJson.get("exceptions");
    if (exceptions != null && exceptions.size() > 0) {
      for (JsonNode ex : exceptions) {
        String message = ex.get("message").asText();
        assertTrue(!message.contains("multicluster") && !message.contains("federation"),
            "Unexpected multicluster/federation exception: " + message);
      }
    }

    LOGGER.info("Verified {} returned only local cluster data (count={})", testName, actualCount);
  }

  @Test(dataProvider = "physicalTableQueryModes", groups = "query")
  public void testPhysicalTablesAlwaysQueryLocalCluster(String testName, String queryOptions, int brokerPort,
      boolean expectValidationError)
      throws Exception {
    String physicalTableName = getPhysicalTable1InCluster1() + "_OFFLINE";
    verifyPhysicalTableLocalOnly(physicalTableName, queryOptions, brokerPort, expectValidationError, testName);
  }

  @DataProvider(name = "queryModesWithoutMultiClusterOption")
  public Object[][] queryModesWithoutMultiClusterOption() {
    int normalBroker = _cluster1._brokerPort;

    String noOpts = "";
    String onlyMseOpts = "SET useMultistageEngine=true; ";
    String onlyPhysOptOpts = "SET useMultistageEngine=true; SET usePhysicalOptimizer=true; ";
    String onlyMseLiteOpts = "SET useMultistageEngine=true; SET usePhysicalOptimizer=true; SET runInBroker=true; ";
    String explicitlyDisabledOpts = "SET enableMultiClusterRouting=false; ";
    String explicitlyDisabledMseOpts = "SET enableMultiClusterRouting=false; SET useMultistageEngine=true; ";

    return new Object[][]{
        {"SSE-NoMultiClusterOption", noOpts, false, normalBroker},
        {"SSE-ExplicitlyDisabled", explicitlyDisabledOpts, false, normalBroker},
        {"MSE-NoMultiClusterOption", onlyMseOpts, true, normalBroker},
        {"MSE-ExplicitlyDisabled", explicitlyDisabledMseOpts, true, normalBroker},
        {"PhysicalOptimizer-NoMultiClusterOption", onlyPhysOptOpts, true, normalBroker},
        {"MSELiteMode-NoMultiClusterOption", onlyMseLiteOpts, true, normalBroker},
    };
  }

  @DataProvider(name = "physicalTableQueryModes")
  public Object[][] physicalTableQueryModes() {
    int normalBroker = _cluster1._brokerPort;

    String withMultiClusterOpts = "SET enableMultiClusterRouting=true; ";
    String withMultiClusterMseOpts = "SET enableMultiClusterRouting=true; SET useMultistageEngine=true; ";
    String noOpts = "";
    String onlyMseOpts = "SET useMultistageEngine=true; ";
    String explicitlyDisabledOpts = "SET enableMultiClusterRouting=false; ";

    return new Object[][]{
        {"PhysicalTable-SSE-WithMultiClusterRouting", withMultiClusterOpts, normalBroker, true},
        {"PhysicalTable-MSE-WithMultiClusterRouting", withMultiClusterMseOpts, normalBroker, true},
        {"PhysicalTable-SSE-NoOptions", noOpts, normalBroker, false},
        {"PhysicalTable-MSE-NoOptions", onlyMseOpts, normalBroker, false},
        {"PhysicalTable-SSE-ExplicitlyDisabled", explicitlyDisabledOpts, normalBroker, false},
    };
  }

  @DataProvider(name = "queryModes")
  public Object[][] queryModes() {
    int normalBroker = _cluster1._brokerPort;
    int unavailableBroker = _brokerWithUnavailableCluster._brokerPort;

    String sseOpts = "SET enableMultiClusterRouting=true; ";
    String mseOpts = sseOpts + "SET useMultistageEngine=true; ";
    String physOptOpts = mseOpts + "SET usePhysicalOptimizer=true; ";
    String mseLiteOpts = physOptOpts + "SET runInBroker=true; ";

    return new Object[][]{
        {"SSE-NormalBroker", sseOpts, false, normalBroker, false},
        {"SSE-UnavailableBroker", sseOpts, false, unavailableBroker, true},
        {"MSE-NormalBroker", mseOpts, true, normalBroker, false},
        {"MSE-UnavailableBroker", mseOpts, true, unavailableBroker, true},
        {"PhysicalOptimizer-NormalBroker", physOptOpts, true, normalBroker, false},
        {"PhysicalOptimizer-UnavailableBroker", physOptOpts, true, unavailableBroker, true},
        {"MSELiteMode-NormalBroker", mseLiteOpts, true, normalBroker, false},
        {"MSELiteMode-UnavailableBroker", mseLiteOpts, true, unavailableBroker, true},
    };
  }

  /**
   * Setup physical tables based on configuration. By default, creates different table names
   * in each cluster. Subclasses can override to create same-named tables.
   */
  protected void setupPhysicalTables() throws Exception {
    // Cluster 1 tables
    dropTableAndSchemaIfExists(getPhysicalTable1InCluster1(), _cluster1._controllerBaseApiUrl);
    dropTableAndSchemaIfExists(getPhysicalTable2InCluster1(), _cluster1._controllerBaseApiUrl);
    createSchemaAndTableForCluster(getPhysicalTable1InCluster1(), _cluster1._controllerBaseApiUrl);
    createSchemaAndTableForCluster(getPhysicalTable2InCluster1(), _cluster1._controllerBaseApiUrl);

    // Cluster 2 tables
    dropTableAndSchemaIfExists(getPhysicalTable1InCluster2(), _cluster2._controllerBaseApiUrl);
    dropTableAndSchemaIfExists(getPhysicalTable2InCluster2(), _cluster2._controllerBaseApiUrl);
    createSchemaAndTableForCluster(getPhysicalTable1InCluster2(), _cluster2._controllerBaseApiUrl);
    createSchemaAndTableForCluster(getPhysicalTable2InCluster2(), _cluster2._controllerBaseApiUrl);
  }

  protected void createLogicalTableOnBothClusters(String logicalTableName,
      String cluster1PhysicalTable, String cluster2PhysicalTable) throws IOException {
    // Automatically detect if same-named tables: if so, both must be marked as multi-cluster (true)
    // If different names: local table is false, remote table is true
    boolean areSameNamedTables = cluster1PhysicalTable.equals(cluster2PhysicalTable);

    Map<String, PhysicalTableConfig> cluster1PhysicalTableConfigMap;
    Map<String, PhysicalTableConfig> cluster2PhysicalTableConfigMap;

    if (areSameNamedTables) {
      // Same table name: only one entry, marked as multi-cluster
      cluster1PhysicalTableConfigMap = Map.of(
          cluster1PhysicalTable + "_OFFLINE",
          new PhysicalTableConfig(true)  // Same-named table must be marked as multi-cluster
      );
      cluster2PhysicalTableConfigMap = Map.of(
          cluster1PhysicalTable + "_OFFLINE",
          new PhysicalTableConfig(true)  // Same-named table must be marked as multi-cluster
      );
    } else {
      // Different table names: local is false, remote is true
      cluster1PhysicalTableConfigMap = Map.of(
          cluster1PhysicalTable + "_OFFLINE",
          new PhysicalTableConfig(false),  // Local table
          cluster2PhysicalTable + "_OFFLINE",
          new PhysicalTableConfig(true)  // Remote table
      );
      cluster2PhysicalTableConfigMap = Map.of(
          cluster1PhysicalTable + "_OFFLINE",
          new PhysicalTableConfig(true),  // Remote table
          cluster2PhysicalTable + "_OFFLINE",
          new PhysicalTableConfig(false)  // Local table
      );
    }

    createLogicalTable(SCHEMA_FILE, cluster1PhysicalTableConfigMap, DEFAULT_TENANT,
        _cluster1._controllerBaseApiUrl, logicalTableName, cluster1PhysicalTable + "_OFFLINE", null);
    createLogicalTable(SCHEMA_FILE, cluster2PhysicalTableConfigMap, DEFAULT_TENANT,
        _cluster2._controllerBaseApiUrl, logicalTableName, cluster2PhysicalTable + "_OFFLINE", null);
  }
}
