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
import com.google.common.base.Function;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;


/**
 * Shared set of common tests for cluster integration tests.
 * <p>To enable the test, override it and add @Test annotation.
 */
@SuppressWarnings("unused")
public abstract class BaseClusterIntegrationTestSet extends BaseClusterIntegrationTest {
  private static final Random RANDOM = new Random();

  // Default settings
  private static final String DEFAULT_QUERY_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset.test_queries_10K";
  private static final int DEFAULT_NUM_QUERIES_TO_GENERATE = 100;
  private static final int DEFAULT_MAX_NUM_QUERIES_TO_SKIP_IN_QUERY_FILE = 200;

  /**
   * Can be overridden to change default setting
   */
  protected String getQueryFileName() {
    return DEFAULT_QUERY_FILE_NAME;
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
        "SELECT ActualElapsedTime, OriginStateFips, MIN(DivReachedDest), SUM(ArrDelay), AVG(CRSDepTime) FROM mytable "
            + "WHERE OriginCityName > 'Beaumont/Port Arthur, TX' OR FlightDate IN ('2014-12-09', '2014-10-05')"
            + " GROUP BY ActualElapsedTime, OriginStateFips "
            + "HAVING SUM(ArrDelay) <> 6325.973 AND AVG(CRSDepTime) <= 1569.8755 OR SUM(TaxiIn) = 1003.87274 TOP 29";
    testQuery(query, Arrays.asList(
        "SELECT ActualElapsedTime, OriginStateFips, MIN(DivReachedDest) FROM mytable WHERE OriginCityName > 'Beaumont/Port Arthur, "
            + "TX' OR FlightDate IN ('2014-12-09', '2014-10-05') GROUP BY ActualElapsedTime, OriginStateFips "
            + "HAVING SUM(ArrDelay) <> 6325.973 AND AVG(CAST(CRSDepTime AS DOUBLE)) <= 1569.8755 OR SUM(TaxiIn) = 1003.87274",
        "SELECT ActualElapsedTime, OriginStateFips, SUM(ArrDelay) FROM mytable WHERE OriginCityName > 'Beaumont/Port Arthur, TX' OR "
            + "FlightDate IN ('2014-12-09', '2014-10-05') GROUP BY ActualElapsedTime, OriginStateFips "
            + "HAVING SUM(ArrDelay) <> 6325.973 AND AVG(CAST(CRSDepTime AS DOUBLE)) <= 1569.8755 OR SUM(TaxiIn) = 1003.87274",
        "SELECT ActualElapsedTime, OriginStateFips,"
            + " AVG(CAST(CRSDepTime AS DOUBLE)) FROM mytable WHERE OriginCityName > 'Beaumont/Port Arthur, TX' OR "
            + "FlightDate IN ('2014-12-09', '2014-10-05') GROUP BY ActualElapsedTime, OriginStateFips "
            + "HAVING SUM(ArrDelay) <> 6325.973 AND AVG(CAST(CRSDepTime AS DOUBLE)) <= 1569.8755 OR SUM(TaxiIn) = 1003.87274"));
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
    String h2query =
        "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10, ArrTime + 5 + ArrDelay, ArrTime * 5 + ArrDelay FROM mytable WHERE (ArrTime - 100) * (5 + ArrDelay)> 0";
    testSqlQuery(query, Collections.singletonList(h2query));
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
        Assert.assertTrue(response.has(statName));
      }
    }
  }

  public void testVirtualColumnQueries() {
    // Check that there are no virtual columns in the query results
    ResultSetGroup resultSetGroup = getPinotConnection().execute("select * from mytable");
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    for (int i = 0; i < resultSet.getColumnCount(); i++) {
      Assert.assertFalse(resultSet.getColumnName(i).startsWith("$"),
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
    Assert.assertNotNull(resourceUrl);
    File queryFile = new File(resourceUrl.getFile());

    int maxNumQueriesToSkipInQueryFile = getMaxNumQueriesToSkipInQueryFile();
    try (BufferedReader reader = new BufferedReader(new FileReader(queryFile))) {
      while (true) {
        int numQueriesSkipped = RANDOM.nextInt(maxNumQueriesToSkipInQueryFile);
        for (int i = 0; i < numQueriesSkipped; i++) {
          reader.readLine();
        }

        String queryString = reader.readLine();
        // Reach end of file.
        if (queryString == null) {
          return;
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
   * TODO: fix this test by adding the SQL query file
   */
  public void testSqlQueriesFromQueryFile()
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTestSet.class.getClassLoader().getResource(getQueryFileName());
    Assert.assertNotNull(resourceUrl);
    File queryFile = new File(resourceUrl.getFile());

    int maxNumQueriesToSkipInQueryFile = getMaxNumQueriesToSkipInQueryFile();
    try (BufferedReader reader = new BufferedReader(new FileReader(queryFile))) {
      while (true) {
        int numQueriesSkipped = RANDOM.nextInt(maxNumQueriesToSkipInQueryFile);
        for (int i = 0; i < numQueriesSkipped; i++) {
          reader.readLine();
        }

        String queryString = reader.readLine();
        // Reach end of file.
        if (queryString == null) {
          return;
        }

        JsonNode query = JsonUtils.stringToJsonNode(queryString);
        String sqlQuery = query.get("pql").asText();
        JsonNode hsqls = query.get("hsqls");
        List<String> sqlQueries = new ArrayList<>();
        int length = hsqls.size();
        for (int i = 0; i < length; i++) {
          sqlQueries.add(hsqls.get(i).asText());
        }
        testSqlQuery(sqlQuery, sqlQueries);
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
    Assert.assertTrue(jsonObject.get("exceptions").size() > 0);
  }

  /**
   * Test if routing table get updated when instance is shutting down.
   *
   * @throws Exception
   */
  public void testInstanceShutdown()
      throws Exception {
    List<String> instances = _helixAdmin.getInstancesInCluster(getHelixClusterName());
    Assert.assertFalse(instances.isEmpty(), "List of instances should not be empty");

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
    for (String instanceName : instances) {
      if (!instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
        continue;
      }

      // Ensure that the random instance is in the routing table
      checkForInstanceInRoutingTable(true, instanceName);

      // Mark the server instance as shutting down
      InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(getHelixClusterName(), instanceName);
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
      _helixAdmin.setInstanceConfig(getHelixClusterName(), instanceName, instanceConfig);

      // Check that it is not in the routing table
      checkForInstanceInRoutingTable(false, instanceName);

      // Re-enable the server instance
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false);
      _helixAdmin.setInstanceConfig(getHelixClusterName(), instanceName, instanceConfig);

      // Check that it is in the routing table
      checkForInstanceInRoutingTable(true, instanceName);
    }
  }

  private void checkForInstanceInRoutingTable(final boolean shouldExist, final String instanceName) {
    String errorMessage;
    if (shouldExist) {
      errorMessage = "Routing table does not contain expected instance: " + instanceName;
    } else {
      errorMessage = "Routing table contains unexpected instance: " + instanceName;
    }
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          JsonNode routingTableSnapshot =
              getDebugInfo("debug/routingTable/" + getTableName()).get("routingTableSnapshot");
          int numTables = routingTableSnapshot.size();
          for (int i = 0; i < numTables; i++) {
            JsonNode tableRouting = routingTableSnapshot.get(i);
            String tableNameWithType = tableRouting.get("tableName").asText();
            if (TableNameBuilder.extractRawTableName(tableNameWithType).equals(getTableName())) {
              JsonNode routingTableEntries = tableRouting.get("routingTableEntries");
              int numRoutingTableEntries = routingTableEntries.size();
              for (int j = 0; j < numRoutingTableEntries; j++) {
                JsonNode routingTableEntry = routingTableEntries.get(j);
                if (routingTableEntry.has(instanceName)) {
                  return shouldExist;
                }
              }
            }
          }
          return !shouldExist;
        } catch (Exception e) {
          return null;
        }
      }
    }, 60_000L, errorMessage);
  }

  private void checkForEmptyRoutingTable(final boolean shouldBeEmpty)
      throws Exception {
    String errorMessage;
    if (shouldBeEmpty) {
      errorMessage = "Routing table is not empty";
    } else {
      errorMessage = "Routing table is empty";
    }
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          JsonNode routingTableSnapshot =
              getDebugInfo("debug/routingTable/" + getTableName()).get("routingTableSnapshot");
          int numTables = routingTableSnapshot.size();
          for (int i = 0; i < numTables; i++) {
            JsonNode tableRouting = routingTableSnapshot.get(i);
            String tableNameWithType = tableRouting.get("tableName").asText();
            if (TableNameBuilder.extractRawTableName(tableNameWithType).equals(getTableName())) {
              JsonNode routingTableEntries = tableRouting.get("routingTableEntries");
              int numRoutingTableEntries = routingTableEntries.size();
              for (int j = 0; j < numRoutingTableEntries; j++) {
                JsonNode routingTableEntry = routingTableEntries.get(j);
                if (routingTableEntry.size() == 0) {
                  if (!shouldBeEmpty) {
                    return false;
                  }
                } else {
                  if (shouldBeEmpty) {
                    return false;
                  }
                }
              }
            }
          }
          return true;
        } catch (Exception e) {
          return null;
        }
      }
    }, 60_000L, errorMessage);
  }
}
