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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Shared set of common tests for cluster integration tests.
 * <p>To enable the test, override it and add @Test annotation.
 */
public abstract class BaseClusterIntegrationTestSet extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseClusterIntegrationTestSet.class);
  private static final Random RANDOM = new Random();

  // Default settings
  private static final String DEFAULT_PQL_QUERY_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset.test_queries_500";
  private static final String DEFAULT_SQL_QUERY_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset.test_queries_500.sql";
  private static final int DEFAULT_NUM_QUERIES_TO_GENERATE = 100;
  private static final int DEFAULT_MAX_NUM_QUERIES_TO_SKIP_IN_QUERY_FILE = 200;

  /**
   * Can be overridden to change default setting
   */
  protected String getQueryFileName() {
    return DEFAULT_PQL_QUERY_FILE_NAME;
  }

  /**
   * Can be overridden to change default setting
   */
  protected String getSqlQueryFileName() {
    return DEFAULT_SQL_QUERY_FILE_NAME;
  }

  /**
   * Can be overridden to change default setting
   */
  protected int getNumQueriesToGenerate() {
    return DEFAULT_NUM_QUERIES_TO_GENERATE;
  }

  /**
   * Can be overridden to change default setting
   */
  protected int getMaxNumQueriesToSkipInQueryFile() {
    return DEFAULT_MAX_NUM_QUERIES_TO_SKIP_IN_QUERY_FILE;
  }

  /**
   * Test hardcoded queries.
   * <p>NOTE:
   * <p>For queries with <code>LIMIT</code> or <code>TOP</code>, need to remove limit or add <code>LIMIT 10000</code> to
   * the H2 SQL query because the comparison only works on exhausted result with at most 10000 rows.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT a FROM table LIMIT 15 -> [SELECT a FROM table LIMIT 10000]</code>
   *   </li>
   * </ul>
   * <p>For queries with multiple aggregation functions, need to split each of them into a separate H2 SQL query.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT SUM(a), MAX(b) FROM table -> [SELECT SUM(a) FROM table, SELECT MAX(b) FROM table]</code>
   *   </li>
   * </ul>
   * <p>For group-by queries, need to add group-by columns to the select clause for H2 SQL query.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT SUM(a) FROM table GROUP BY b -> [SELECT b, SUM(a) FROM table GROUP BY b]</code>
   *   </li>
   * </ul>
   *
   * @throws Exception
   */
  public void testHardcodedQueries()
      throws Exception {
    // Here are some sample queries.
    String query;
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch >= 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch < 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT MAX(ArrTime), MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 16312";
    testQuery(query, Arrays.asList("SELECT MAX(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 15312",
        "SELECT MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 15312"));
    query =
        "SELECT SUM(TotalAddGTime) FROM mytable WHERE DivArrDelay NOT IN (67, 260) AND Carrier IN ('F9', 'B6') OR DepTime BETWEEN 2144 AND 1926";
    testQuery(query, Collections.singletonList(query));
  }

  /**
   * Test hardcoded queries.
   * <p>NOTE:
   * <p>For queries with <code>LIMIT</code> or <code>TOP</code>, need to remove limit or add <code>LIMIT 10000</code> to
   * the H2 SQL query because the comparison only works on exhausted result with at most 10000 rows.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT a FROM table LIMIT 15 -> [SELECT a FROM table LIMIT 10000]</code>
   *   </li>
   * </ul>
   * <p>For group-by queries, need to add group-by columns to the select clause for H2 SQL query.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT SUM(a) FROM table GROUP BY b -> [SELECT b, SUM(a) FROM table GROUP BY b]</code>
   *   </li>
   * </ul>
   * TODO: Selection queries, Aggregation Group By queries, Order By, Distinct
   *  This list is very basic right now (aggregations only) and needs to be enriched
   */
  public void testHardcodedSqlQueries()
      throws Exception {
    String query;
    List<String> h2queries;
    query = "SELECT COUNT(*) FROM mytable WHERE CarrierDelay=15 AND ArrDelay > CarrierDelay LIMIT 1";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT ArrDelay, CarrierDelay, (ArrDelay - CarrierDelay) AS diff FROM mytable WHERE CarrierDelay=15 AND ArrDelay > CarrierDelay ORDER BY diff, ArrDelay, CarrierDelay LIMIT 100000";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE ArrDelay > CarrierDelay LIMIT 1";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT ArrDelay, CarrierDelay, (ArrDelay - CarrierDelay) AS diff FROM mytable WHERE ArrDelay > CarrierDelay ORDER BY diff, ArrDelay, CarrierDelay LIMIT 100000";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT count(*) FROM mytable WHERE AirlineID > 20355 AND OriginState BETWEEN 'PA' AND 'DE' AND DepTime <> 2202 LIMIT 21";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT SUM(CAST(CAST(ArrTime AS varchar) AS LONG)) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT CAST(CAST(ArrTime AS varchar) AS LONG) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL' ORDER BY ArrTime DESC";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT DistanceGroup FROM mytable WHERE \"Month\" BETWEEN 1 AND 1 AND DivAirportSeqIDs IN (1078102, 1142303, 1530402, 1172102, 1291503) OR SecurityDelay IN (1, 0, 14, -9999) LIMIT 10";
    h2queries = Collections.singletonList(
        "SELECT DistanceGroup FROM mytable WHERE Month BETWEEN 1 AND 1 AND (DivAirportSeqIDs__MV0 IN (1078102, 1142303, 1530402, 1172102, 1291503) OR DivAirportSeqIDs__MV1 IN (1078102, 1142303, 1530402, 1172102, 1291503) OR DivAirportSeqIDs__MV2 IN (1078102, 1142303, 1530402, 1172102, 1291503) OR DivAirportSeqIDs__MV3 IN (1078102, 1142303, 1530402, 1172102, 1291503) OR DivAirportSeqIDs__MV4 IN (1078102, 1142303, 1530402, 1172102, 1291503)) OR SecurityDelay IN (1, 0, 14, -9999) LIMIT 10000");
    testSqlQuery(query, h2queries);
    query = "SELECT MAX(Quarter), MAX(FlightNum) FROM mytable LIMIT 8";
    h2queries = Collections.singletonList("SELECT MAX(Quarter),MAX(FlightNum) FROM mytable LIMIT 10000");
    testSqlQuery(query, h2queries);
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16312 AND Carrier = 'DL'";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT SUM(ArrTime) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT MAX(ArrTime) FROM mytable WHERE DaysSinceEpoch > 16312 AND Carrier = 'DL'";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 16312 AND Carrier = 'DL'";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch < 16312 AND Carrier = 'DL'";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT MAX(ArrTime), MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*), MAX(ArrTime), MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 16312";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*), MAX(ArrTime), MIN(ArrTime), DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT DaysSinceEpoch, COUNT(*), MAX(ArrTime), MIN(ArrTime) FROM mytable GROUP BY DaysSinceEpoch";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT ArrTime, ArrTime * 10 FROM mytable WHERE DaysSinceEpoch >= 16312";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT ArrTime, ArrTime - ArrTime % 10 FROM mytable WHERE DaysSinceEpoch >= 16312";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10 FROM mytable WHERE DaysSinceEpoch >= 16312";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10 FROM mytable WHERE ArrTime - 100 > 0";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10, ADD(ArrTime + 5, ArrDelay), ADD(ArrTime * 5, ArrDelay) FROM mytable WHERE mult((ArrTime - 100), (5 + ArrDelay))> 0";
    h2queries = Collections.singletonList(
        "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10, ArrTime + 5 + ArrDelay, ArrTime * 5 + ArrDelay FROM mytable WHERE (ArrTime - 100) * (5 + ArrDelay)> 0");
    testSqlQuery(query, h2queries);
    query = "SELECT COUNT(*) AS \"date\", MAX(ArrTime) AS \"group\", MIN(ArrTime) AS min FROM myTable";
    testSqlQuery(query, Collections.singletonList(query));

    // Post-aggregation in ORDER-BY
    query = "SELECT MAX(ArrTime) FROM mytable GROUP BY DaysSinceEpoch ORDER BY MAX(ArrTime) - MIN(ArrTime)";
    testSqlQuery(query, Collections.singletonList(query));
    query = "SELECT MAX(ArrDelay), Month FROM mytable GROUP BY Month ORDER BY ABS(Month - 6) + MAX(ArrDelay)";
    testSqlQuery(query, Collections.singletonList(query));

    // Post-aggregation in SELECT
    query = "SELECT MAX(ArrDelay) + MAX(AirTime) FROM mytable";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT MAX(ArrDelay) - MAX(AirTime), DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch ORDER BY MAX(ArrDelay) - MIN(AirTime) DESC";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT DaysSinceEpoch, MAX(ArrDelay) * 2 - MAX(AirTime) - 3 FROM mytable GROUP BY DaysSinceEpoch ORDER BY MAX(ArrDelay) - MIN(AirTime) DESC";
    testSqlQuery(query, Collections.singletonList(query));

    // Having
    query = "SELECT COUNT(*) AS Count, DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch HAVING Count > 350";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT MAX(ArrDelay) - MAX(AirTime) AS Diff, DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch HAVING Diff * 2 > 1000 ORDER BY Diff ASC";
    testSqlQuery(query, Collections.singletonList(query));
    query =
        "SELECT DaysSinceEpoch, MAX(ArrDelay) - MAX(AirTime) AS Diff FROM mytable GROUP BY DaysSinceEpoch HAVING (Diff >= 300 AND Diff < 500) OR Diff < -500 ORDER BY Diff DESC";
    testSqlQuery(query, Collections.singletonList(query));

    // IN_ID_SET
    IdSet idSet = IdSets.create(FieldSpec.DataType.LONG);
    idSet.add(19690L);
    idSet.add(20355L);
    idSet.add(21171L);
    // Also include a non-existing id
    idSet.add(0L);
    String serializedIdSet = idSet.toBase64String();
    String inIdSetQuery = "SELECT COUNT(*) FROM mytable WHERE INIDSET(AirlineID, '" + serializedIdSet + "') = 1";
    String inQuery = "SELECT COUNT(*) FROM mytable WHERE AirlineID IN (19690, 20355, 21171, 0)";
    testSqlQuery(inIdSetQuery, Collections.singletonList(inQuery));
    String notInIdSetQuery = "SELECT COUNT(*) FROM mytable WHERE INIDSET(AirlineID, '" + serializedIdSet + "') = 0";
    String notInQuery = "SELECT COUNT(*) FROM mytable WHERE AirlineID NOT IN (19690, 20355, 21171, 0)";
    testSqlQuery(notInIdSetQuery, Collections.singletonList(notInQuery));
  }

  /**
   * Test to ensure that broker response contains expected stats
   *
   * @throws Exception
   */
  public void testBrokerResponseMetadata()
      throws Exception {
    String[] pqlQueries = new String[]{ //
        "SELECT count(*) FROM mytable", // matching query
        "SELECT count(*) FROM mytable where non_existing_column='non_existing_value", // query that does not match any row
        "SELECT count(*) FROM mytable_foo" // query a non existing table
    };
    String[] statNames =
        new String[]{"totalDocs", "numServersQueried", "numServersResponded", "numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched", "numDocsScanned", "totalDocs", "timeUsedMs", "numEntriesScannedInFilter", "numEntriesScannedPostFilter"};

    for (String query : pqlQueries) {
      JsonNode response = postQuery(query);
      for (String statName : statNames) {
        assertTrue(response.has(statName));
      }
    }
  }

  public void testVirtualColumnQueries() {
    // Check that there are no virtual columns in the query results
    ResultSetGroup resultSetGroup = getPinotConnection().execute("select * from mytable");
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    for (int i = 0; i < resultSet.getColumnCount(); i++) {
      assertFalse(resultSet.getColumnName(i).startsWith("$"),
          "Virtual column " + resultSet.getColumnName(i) + " is present in the results!");
    }

    // Check that the virtual columns work as expected (throws no exceptions)
    getPinotConnection().execute("select $docId, $segmentName, $hostName from mytable");
    getPinotConnection().execute("select $docId, $segmentName, $hostName from mytable where $docId < 5 limit 50");
    getPinotConnection().execute("select $docId, $segmentName, $hostName from mytable where $docId = 5 limit 50");
    getPinotConnection().execute("select $docId, $segmentName, $hostName from mytable where $docId > 19998 limit 50");
    getPinotConnection().execute("select max($docId) from mytable group by $segmentName");
  }

  /**
   * Test random queries from the query file.
   *
   * @throws Exception
   */
  public void testQueriesFromQueryFile()
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTestSet.class.getClassLoader().getResource(getQueryFileName());
    assertNotNull(resourceUrl);
    File queryFile = new File(resourceUrl.getFile());

    try (BufferedReader reader = new BufferedReader(new FileReader(queryFile))) {
      String queryString;
      while ((queryString = reader.readLine()) != null) {
        // Skip commented line and empty line.
        queryString = queryString.trim();
        if (queryString.startsWith("#") || queryString.isEmpty()) {
          continue;
        }

        JsonNode query = JsonUtils.stringToJsonNode(queryString);
        String pqlQuery = query.get("pql").asText();
        JsonNode hsqls = query.get("hsqls");
        List<String> sqlQueries = new ArrayList<>();
        int length = hsqls.size();
        for (int i = 0; i < length; i++) {
          sqlQueries.add(hsqls.get(i).asText());
        }
        testQuery(pqlQuery, sqlQueries);
      }
    }
  }

  /**
   * Test random SQL queries from the query file.
   */
  public void testSqlQueriesFromQueryFile()
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTestSet.class.getClassLoader().getResource(getSqlQueryFileName());
    assertNotNull(resourceUrl);
    File queryFile = new File(resourceUrl.getFile());

    try (BufferedReader reader = new BufferedReader(new FileReader(queryFile))) {
      String queryString;
      while ((queryString = reader.readLine()) != null) {
        // Skip commented line and empty line.
        queryString = queryString.trim();
        if (queryString.startsWith("#") || queryString.isEmpty()) {
          continue;
        }

        JsonNode query = JsonUtils.stringToJsonNode(queryString);
        String sqlQuery = query.get("sql").asText();
        JsonNode hsqls = query.get("hsqls");
        List<String> sqlQueries = new ArrayList<>();
        if (hsqls == null || hsqls.size() == 0) {
          sqlQueries.add(sqlQuery);
        } else {
          for (int i = 0; i < hsqls.size(); i++) {
            sqlQueries.add(hsqls.get(i).asText());
          }
        }
        try {
          testSqlQuery(sqlQuery, sqlQueries);
        } catch (Exception e) {
          e.printStackTrace();
          LOGGER.error("Failed to test SQL query: {} with H2 queries: {}.", sqlQuery, sqlQueries, e);
          throw e;
        }
      }
    }
  }

  /**
   * Test queries without multi values generated by query generator.
   *
   * @throws Exception
   */
  public void testGeneratedQueriesWithoutMultiValues()
      throws Exception {
    testGeneratedQueries(false);
  }

  /**
   * Test queries with multi values generated by query generator.
   *
   * @throws Exception
   */
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    testGeneratedQueries(true);
  }

  private void testGeneratedQueries(boolean withMultiValues)
      throws Exception {
    QueryGenerator queryGenerator = getQueryGenerator();
    queryGenerator.setSkipMultiValuePredicates(!withMultiValues);
    int numQueriesToGenerate = getNumQueriesToGenerate();
    for (int i = 0; i < numQueriesToGenerate; i++) {
      QueryGenerator.Query query = queryGenerator.generateQuery();
      testQuery(query.generatePql(), query.generateH2Sql());
    }
  }

  /**
   * Test invalid queries which should cause query exceptions.
   *
   * @throws Exception
   */
  public void testQueryExceptions()
      throws Exception {
    testQueryException("POTATO");
    testQueryException("SELECT COUNT(*) FROM potato");
    testQueryException("SELECT POTATO(ArrTime) FROM mytable");
    testQueryException("SELECT COUNT(*) FROM mytable where ArrTime = 'potato'");
  }

  private void testQueryException(String query)
      throws Exception {
    JsonNode jsonObject = postQuery(query);
    assertTrue(jsonObject.get("exceptions").size() > 0);
  }

  /**
   * Test if routing table get updated when instance is shutting down.
   *
   * @throws Exception
   */
  public void testInstanceShutdown()
      throws Exception {
    List<String> instances = _helixAdmin.getInstancesInCluster(getHelixClusterName());
    assertFalse(instances.isEmpty(), "List of instances should not be empty");

    // Mark all instances in the cluster as shutting down
    for (String instance : instances) {
      InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(getHelixClusterName(), instance);
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
      _helixAdmin.setInstanceConfig(getHelixClusterName(), instance, instanceConfig);
    }

    // Check that the routing table is empty
    checkForEmptyRoutingTable(true);

    // Mark all instances as not shutting down
    for (String instance : instances) {
      InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(getHelixClusterName(), instance);
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false);
      _helixAdmin.setInstanceConfig(getHelixClusterName(), instance, instanceConfig);
    }

    // Check that the routing table is not empty
    checkForEmptyRoutingTable(false);

    // Check on each server instance
    for (String instance : instances) {
      if (!instance.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
        continue;
      }

      // Ensure that the random instance is in the routing table
      checkForInstanceInRoutingTable(instance, true);

      // Mark the server instance as shutting down
      InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(getHelixClusterName(), instance);
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
      _helixAdmin.setInstanceConfig(getHelixClusterName(), instance, instanceConfig);

      // Check that it is not in the routing table
      checkForInstanceInRoutingTable(instance, false);

      // Re-enable the server instance
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false);
      _helixAdmin.setInstanceConfig(getHelixClusterName(), instance, instanceConfig);

      // Check that it is in the routing table
      checkForInstanceInRoutingTable(instance, true);
    }
  }

  private void checkForInstanceInRoutingTable(String instance, boolean shouldExist) {
    String errorMessage;
    if (shouldExist) {
      errorMessage = "Routing table does not contain expected instance: " + instance;
    } else {
      errorMessage = "Routing table contains unexpected instance: " + instance;
    }
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode routingTables = getDebugInfo("debug/routingTable/" + getTableName());
        for (JsonNode routingTable : routingTables) {
          if (routingTable.has(instance)) {
            return shouldExist;
          }
        }
        return !shouldExist;
      } catch (Exception e) {
        return null;
      }
    }, 60_000L, errorMessage);
  }

  private void checkForEmptyRoutingTable(boolean shouldBeEmpty) {
    String errorMessage;
    if (shouldBeEmpty) {
      errorMessage = "Routing table is not empty";
    } else {
      errorMessage = "Routing table is empty";
    }
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode routingTables = getDebugInfo("debug/routingTable/" + getTableName());
        for (JsonNode routingTable : routingTables) {
          if ((routingTable.size() == 0) != shouldBeEmpty) {
            return false;
          }
        }
        return true;
      } catch (Exception e) {
        return null;
      }
    }, 60_000L, errorMessage);
  }

  /**
   * TODO: Support removing new added columns for MutableSegment and remove the new added columns before running the
   *       next test. Use this to replace {@link OfflineClusterIntegrationTest#testDefaultColumns()}.
   */
  public void testReload(boolean includeOfflineTable)
      throws Exception {
    String rawTableName = getTableName();
    Schema schema = getSchema();

    String selectStarQuery = "SELECT * FROM " + rawTableName;
    JsonNode queryResponse = postQuery(selectStarQuery);
    assertEquals(queryResponse.get("selectionResults").get("columns").size(), schema.size());
    long numTotalDocs = queryResponse.get("totalDocs").asLong();

    schema.addField(constructNewDimension(FieldSpec.DataType.INT, true));
    schema.addField(constructNewDimension(FieldSpec.DataType.LONG, true));
    schema.addField(constructNewDimension(FieldSpec.DataType.FLOAT, true));
    schema.addField(constructNewDimension(FieldSpec.DataType.DOUBLE, true));
    schema.addField(constructNewDimension(FieldSpec.DataType.STRING, true));
    schema.addField(constructNewDimension(FieldSpec.DataType.INT, false));
    schema.addField(constructNewDimension(FieldSpec.DataType.LONG, false));
    schema.addField(constructNewDimension(FieldSpec.DataType.FLOAT, false));
    schema.addField(constructNewDimension(FieldSpec.DataType.DOUBLE, false));
    schema.addField(constructNewDimension(FieldSpec.DataType.STRING, false));
    schema.addField(constructNewMetric(FieldSpec.DataType.INT));
    schema.addField(constructNewMetric(FieldSpec.DataType.LONG));
    schema.addField(constructNewMetric(FieldSpec.DataType.FLOAT));
    schema.addField(constructNewMetric(FieldSpec.DataType.DOUBLE));
    schema.addField(constructNewMetric(FieldSpec.DataType.BYTES));

    // Upload the schema with extra columns
    addSchema(schema);

    // Reload the table
    if (includeOfflineTable) {
      reloadOfflineTable(rawTableName);
    }
    reloadRealtimeTable(rawTableName);

    // Wait for all segments to finish reloading, and test querying the new columns
    // NOTE: Use count query to prevent schema inconsistency error
    String testQuery = "SELECT COUNT(*) FROM " + rawTableName + " WHERE NewIntSVDimension < 0";
    long countStarResult = getCountStarResult();
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode testQueryResponse = postQuery(testQuery);
        // Should not throw exception during reload
        assertEquals(testQueryResponse.get("exceptions").size(), 0);
        // Total docs should not change during reload
        assertEquals(testQueryResponse.get("totalDocs").asLong(), numTotalDocs);
        return testQueryResponse.get("aggregationResults").get(0).get("value").asLong() == countStarResult;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate default values for new columns");

    // Select star query should return all the columns
    queryResponse = postQuery(selectStarQuery);
    assertEquals(queryResponse.get("exceptions").size(), 0);
    JsonNode selectionResults = queryResponse.get("selectionResults");
    assertEquals(selectionResults.get("columns").size(), schema.size());
    assertEquals(selectionResults.get("results").size(), 10);

    // Test filter on all new added columns
    String countStarQuery = "SELECT COUNT(*) FROM " + rawTableName
        + " WHERE NewIntSVDimension < 0 AND NewLongSVDimension < 0 AND NewFloatSVDimension < 0 AND NewDoubleSVDimension < 0 AND NewStringSVDimension = 'null'"
        + " AND NewIntMVDimension < 0 AND NewLongMVDimension < 0 AND NewFloatMVDimension < 0 AND NewDoubleMVDimension < 0 AND NewStringMVDimension = 'null'"
        + " AND NewIntMetric = 0 AND NewLongMetric = 0 AND NewFloatMetric = 0 AND NewDoubleMetric = 0 AND NewBytesMetric = ''";
    queryResponse = postQuery(countStarQuery);
    assertEquals(queryResponse.get("exceptions").size(), 0);
    assertEquals(queryResponse.get("aggregationResults").get(0).get("value").asLong(), countStarResult);
  }

  private DimensionFieldSpec constructNewDimension(FieldSpec.DataType dataType, boolean singleValue) {
    String column =
        "New" + StringUtils.capitalize(dataType.toString().toLowerCase()) + (singleValue ? "SV" : "MV") + "Dimension";
    return new DimensionFieldSpec(column, dataType, singleValue);
  }

  private MetricFieldSpec constructNewMetric(FieldSpec.DataType dataType) {
    String column = "New" + StringUtils.capitalize(dataType.toString().toLowerCase()) + "Metric";
    return new MetricFieldSpec(column, dataType);
  }
}
