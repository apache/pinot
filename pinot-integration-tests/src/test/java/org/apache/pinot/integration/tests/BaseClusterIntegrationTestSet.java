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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;

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

  // Default settings
  private static final String DEFAULT_QUERY_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset.test_queries_200.sql";
  private static final int DEFAULT_NUM_QUERIES_TO_GENERATE = 100;

  @BeforeMethod
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(false);
  }

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
   * Test hard-coded queries.
   * @throws Exception
   */
  public void testHardcodedQueries()
      throws Exception {
    testHardcodedQueriesCommon();
    if (useMultiStageQueryEngine()) {
      testHardcodedQueriesV2();
    } else {
      testHardCodedQueriesV1();
    }
  }

  /**
   * Test hardcoded queries.
   * <p>NOTE:
   * <p>For queries with <code>LIMIT</code>, need to remove limit or add <code>LIMIT 10000</code> to the H2 SQL query
   * because the comparison only works on exhausted result with at most 10000 rows.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT a FROM table LIMIT 15 -> [SELECT a FROM table LIMIT 10000]</code>
   *   </li>
   * </ul>
   * <p>For group-by queries, need to add group-by columns to the select clause for H2 queries.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT SUM(a) FROM table GROUP BY b -> [SELECT b, SUM(a) FROM table GROUP BY b]</code>
   *   </li>
   * </ul>
   * TODO: Selection queries, Aggregation Group By queries, Order By, Distinct
   *  This list is very basic right now (aggregations only) and needs to be enriched
   */
  private void testHardcodedQueriesCommon()
      throws Exception {
    String query;
    String h2Query;

    // SUM INTEGER result will be BIGINT
    query = "SELECT SUM(ActualElapsedTime) FROM mytable";
    testQuery(query);
    // SUM FLOAT result will be FLOAT
    query = "SELECT SUM(CAST(ActualElapsedTime AS FLOAT)) FROM mytable";
    testQuery(query);
    // SUM DOUBLE result will be DOUBLE
    query = "SELECT SUM(CAST(ActualElapsedTime AS DOUBLE)) FROM mytable";
    testQuery(query);
    query = "SELECT COUNT(*) FROM mytable WHERE CarrierDelay=15 AND ArrDelay > CarrierDelay LIMIT 1";
    testQuery(query);
    query = "SELECT ArrDelay, CarrierDelay, (ArrDelay - CarrierDelay) AS diff FROM mytable WHERE CarrierDelay=15 AND "
        + "ArrDelay > CarrierDelay ORDER BY diff, ArrDelay, CarrierDelay LIMIT 100000";
    testQuery(query);
    query = "SELECT COUNT(*) FROM mytable WHERE ArrDelay > CarrierDelay LIMIT 1";
    testQuery(query);
    query =
        "SELECT ArrDelay, CarrierDelay, (ArrDelay - CarrierDelay) AS diff FROM mytable WHERE ArrDelay > CarrierDelay "
            + "ORDER BY diff, ArrDelay, CarrierDelay LIMIT 100000";
    testQuery(query);
    query = "SELECT count(*) FROM mytable WHERE AirlineID > 20355 AND OriginState BETWEEN 'PA' AND 'DE' AND DepTime <> "
        + "2202 LIMIT 21";
    testQuery(query);
    query = "SELECT MAX(Quarter), MAX(FlightNum) FROM mytable LIMIT 8";
    h2Query = "SELECT MAX(Quarter),MAX(FlightNum) FROM mytable LIMIT 10000";
    testQuery(query, h2Query);
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16312 AND Carrier = 'DL'";
    testQuery(query);
    query = "SELECT SUM(ArrTime) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'";
    testQuery(query);
    query = "SELECT MAX(ArrTime) FROM mytable WHERE DaysSinceEpoch > 16312 AND Carrier = 'DL'";
    testQuery(query);
    query = "SELECT MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 16312 AND Carrier = 'DL'";
    testQuery(query);
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch < 16312 AND Carrier = 'DL'";
    testQuery(query);
    query = "SELECT MAX(ArrTime), MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testQuery(query);
    query = "SELECT COUNT(*), MAX(ArrTime), MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 16312";
    testQuery(query);
    query = "SELECT COUNT(*), MAX(ArrTime), MIN(ArrTime), DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch";
    testQuery(query);
    query = "SELECT DaysSinceEpoch, COUNT(*), MAX(ArrTime), MIN(ArrTime) FROM mytable GROUP BY DaysSinceEpoch";
    testQuery(query);
    query = "SELECT ArrTime, ArrTime * 10 FROM mytable WHERE DaysSinceEpoch >= 16312";
    testQuery(query);
    query = "SELECT ArrTime, ArrTime - ArrTime % 10 FROM mytable WHERE DaysSinceEpoch >= 16312";
    testQuery(query);
    query = "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10 FROM mytable WHERE DaysSinceEpoch >= 16312";
    testQuery(query);
    query = "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10 FROM mytable WHERE ArrTime - 100 > 0";
    testQuery(query);
    query = "SELECT COUNT(*) AS \"date\", MAX(ArrTime) AS \"group\", MIN(ArrTime) AS \"min\" FROM mytable";
    testQuery(query);

    // NOT
    query = "SELECT count(*) FROM mytable WHERE OriginState NOT BETWEEN 'DE' AND 'PA'";
    testQuery(query);
    query = "SELECT count(*) FROM mytable WHERE OriginState NOT LIKE 'A_'";
    testQuery(query);
    query = "SELECT count(*) FROM mytable WHERE NOT (DaysSinceEpoch = 16312 AND Carrier = 'DL')";
    testQuery(query);
    query = "SELECT count(*) FROM mytable WHERE (NOT DaysSinceEpoch = 16312) AND Carrier = 'DL'";
    testQuery(query);

    // Post-aggregation in ORDER-BY
    query = "SELECT MAX(ArrTime) FROM mytable GROUP BY DaysSinceEpoch ORDER BY MAX(ArrTime) - MIN(ArrTime)";
    testQuery(query);
    query = "SELECT MAX(ArrDelay), Month FROM mytable GROUP BY Month ORDER BY ABS(Month - 6) + MAX(ArrDelay)";
    h2Query = "SELECT MAX(ArrDelay), `Month` FROM mytable GROUP BY `Month` ORDER BY ABS(`Month` - 6) + MAX(ArrDelay)";
    testQuery(query, h2Query);

    // Post-aggregation in SELECT
    query = "SELECT MAX(ArrDelay) + MAX(AirTime) FROM mytable";
    testQuery(query);
    query = "SELECT MAX(ArrDelay) - MAX(AirTime), DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch ORDER BY MAX"
        + "(ArrDelay) - MIN(AirTime) DESC, DaysSinceEpoch";
    testQuery(query);
    query = "SELECT DaysSinceEpoch, MAX(ArrDelay) * 2 - MAX(AirTime) - 3 FROM mytable GROUP BY DaysSinceEpoch ORDER BY "
        + "MAX(ArrDelay) - MIN(AirTime) DESC, DaysSinceEpoch";
    testQuery(query);

    // Having
    query = "SELECT COUNT(*) AS Count, DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch HAVING Count > 350";
    testQuery(query);
    query =
        "SELECT MAX(ArrDelay) - MAX(AirTime) AS Diff, DaysSinceEpoch FROM mytable GROUP BY DaysSinceEpoch HAVING Diff"
            + " * 2 > 1000 ORDER BY Diff ASC";
    testQuery(query);
    query = "SELECT DaysSinceEpoch, MAX(ArrDelay) - MAX(AirTime) AS Diff FROM mytable GROUP BY DaysSinceEpoch HAVING "
        + "(Diff >= 300 AND Diff < 500) OR Diff < -500 ORDER BY Diff DESC";
    testQuery(query);

    // LIKE
    query = "SELECT count(*) FROM mytable WHERE OriginState LIKE 'A_'";
    testQuery(query);
    query = "SELECT count(*) FROM mytable WHERE DestCityName LIKE 'C%'";
    testQuery(query);
    query = "SELECT count(*) FROM mytable WHERE DestCityName LIKE '_h%'";
    testQuery(query);

    // Non-Standard functions
    // mult is not a standard function.
    query =
        "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10, ADD(ArrTime + 5, ArrDelay), ADD(ArrTime * 5, ArrDelay)"
            + " FROM mytable WHERE mult((ArrTime - 100), (5 + ArrDelay))> 0";
    h2Query =
        "SELECT ArrTime, ArrTime + ArrTime * 9 - ArrTime * 10, ArrTime + 5 + ArrDelay, ArrTime * 5 + ArrDelay FROM "
            + "mytable WHERE (ArrTime - 100) * (5 + ArrDelay)> 0";
    testQuery(query, h2Query);

    // Escape quotes
    query = "SELECT DistanceGroup FROM mytable WHERE DATE_TIME_CONVERT(DaysSinceEpoch, '1:DAYS:EPOCH', "
        + "'1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd''T''HH:mm:ss.SSS''Z''', '1:DAYS') = '2014-09-05T00:00:00.000Z'";
    h2Query = "SELECT DistanceGroup FROM mytable WHERE DaysSinceEpoch = 16318 LIMIT 10000";
    testQuery(query, h2Query);

    // DateTimeConverter
    query = "SELECT dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS'), COUNT(*) FROM mytable "
        + "GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS') "
        + "ORDER BY COUNT(*), dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS') DESC";
    h2Query = "SELECT DaysSinceEpoch * 24, COUNT(*) FROM mytable "
        + "GROUP BY DaysSinceEpoch * 24 "
        + "ORDER BY COUNT(*), DaysSinceEpoch DESC";
    testQuery(query, h2Query);

    // TimeConvert
    query = "SELECT timeConvert(DaysSinceEpoch,'DAYS','SECONDS'), COUNT(*) FROM mytable "
        + "GROUP BY timeConvert(DaysSinceEpoch,'DAYS','SECONDS') "
        + "ORDER BY COUNT(*), timeConvert(DaysSinceEpoch,'DAYS','SECONDS') DESC";
    h2Query = "SELECT DaysSinceEpoch * 86400, COUNT(*) FROM mytable "
        + "GROUP BY DaysSinceEpoch * 86400"
        + "ORDER BY COUNT(*), DaysSinceEpoch * 86400 DESC";
    testQuery(query, h2Query);

    // test arithmetic operations on date time columns
    query = "SELECT sub(DaysSinceEpoch,25), COUNT(*) FROM mytable "
        + "GROUP BY sub(DaysSinceEpoch,25) "
        + "ORDER BY COUNT(*),sub(DaysSinceEpoch,25) DESC";
    h2Query = "SELECT DaysSinceEpoch - 25, COUNT(*) FROM mytable "
        + "GROUP BY DaysSinceEpoch "
        + "ORDER BY COUNT(*), DaysSinceEpoch DESC";
    testQuery(query, h2Query);
  }

  private void testHardcodedQueriesV2()
      throws Exception {
    String query;
    String h2Query;

    query =
        "SELECT DistanceGroup FROM mytable WHERE \"Month\" BETWEEN 1 AND 1 AND arrayToMV(DivAirportSeqIDs) IN "
            + "(1078102, 1142303, 1530402, 1172102, 1291503) OR SecurityDelay IN (1, 0, 14, -9999) LIMIT 10";
    h2Query =
        "SELECT DistanceGroup FROM mytable WHERE `Month` BETWEEN 1 AND 1 AND (DivAirportSeqIDs[1] IN (1078102, "
            + "1142303, 1530402, 1172102, 1291503) OR DivAirportSeqIDs[2] IN (1078102, 1142303, 1530402, 1172102, "
            + "1291503) OR DivAirportSeqIDs[3] IN (1078102, 1142303, 1530402, 1172102, 1291503) OR "
            + "DivAirportSeqIDs[4] IN (1078102, 1142303, 1530402, 1172102, 1291503) OR DivAirportSeqIDs[5] IN "
            + "(1078102, 1142303, 1530402, 1172102, 1291503)) OR SecurityDelay IN (1, 0, 14, -9999) LIMIT 10000";
    testQuery(query, h2Query);

    query = "SELECT MIN(ArrDelayMinutes), AVG(CAST(DestCityMarketID AS DOUBLE)) FROM mytable WHERE DivArrDelay < 196";
    h2Query =
        "SELECT MIN(CAST(`ArrDelayMinutes` AS DOUBLE)), AVG(CAST(`DestCityMarketID` AS DOUBLE)) FROM mytable WHERE "
            + "`DivArrDelay` < 196";
    testQuery(query, h2Query);
  }

  private void testHardCodedQueriesV1()
      throws Exception {
    String query;
    String h2Query;
    // TODO: move to common when multistage support CAST AS 'LONG', for now it must use: CAST AS BIGINT
    query =
        "SELECT SUM(CAST(CAST(ArrTime AS varchar) AS LONG)) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = "
            + "'DL'";
    testQuery(query);
    query =
        "SELECT CAST(CAST(ArrTime AS varchar) AS LONG) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL' "
            + "ORDER BY ArrTime DESC";
    testQuery(query);

    // Non-Standard SQL syntax:
    // IN_ID_SET
    {
      IdSet idSet = IdSets.create(FieldSpec.DataType.LONG);
      idSet.add(19690L);
      idSet.add(20355L);
      idSet.add(21171L);
      // Also include a non-existing id
      idSet.add(0L);
      String serializedIdSet = idSet.toBase64String();
      String inIdSetQuery = "SELECT COUNT(*) FROM mytable WHERE INIDSET(AirlineID, '" + serializedIdSet + "') = 1";
      String inQuery = "SELECT COUNT(*) FROM mytable WHERE AirlineID IN (19690, 20355, 21171, 0)";
      testQuery(inIdSetQuery, inQuery);
      String notInIdSetQuery = "SELECT COUNT(*) FROM mytable WHERE INIDSET(AirlineID, '" + serializedIdSet + "') = 0";
      String notInQuery = "SELECT COUNT(*) FROM mytable WHERE AirlineID NOT IN (19690, 20355, 21171, 0)";
      testQuery(notInIdSetQuery, notInQuery);
    }

    // IN_SUBQUERY
    {
      String inSubqueryQuery =
          "SELECT COUNT(*) FROM mytable WHERE INSUBQUERY(DestAirportID, 'SELECT IDSET(DestAirportID) FROM mytable "
              + "WHERE DaysSinceEpoch = 16430') = 1";
      String inQuery = "SELECT COUNT(*) FROM mytable WHERE DestAirportID IN (SELECT DestAirportID FROM mytable WHERE "
          + "DaysSinceEpoch = 16430)";
      testQuery(inSubqueryQuery, inQuery);

      String notInSubqueryQuery =
          "SELECT COUNT(*) FROM mytable WHERE INSUBQUERY(DestAirportID, 'SELECT IDSET(DestAirportID) FROM mytable "
              + "WHERE DaysSinceEpoch = 16430') = 0";
      String notInQuery =
          "SELECT COUNT(*) FROM mytable WHERE DestAirportID NOT IN (SELECT DestAirportID FROM mytable WHERE "
              + "DaysSinceEpoch = 16430)";
      testQuery(notInSubqueryQuery, notInQuery);
    }
  }

  /**
   * Test hardcoded queries on server partitioned data (all the segments for a partition is served by a single server).
   */
  public void testHardcodedServerPartitionedSqlQueries()
      throws Exception {
    // IN_PARTITIONED_SUBQUERY
    {
      String inPartitionedSubqueryQuery =
          "SELECT COUNT(*) FROM mytable WHERE INPARTITIONEDSUBQUERY(DestAirportID, 'SELECT IDSET(DestAirportID) FROM "
              + "mytable WHERE DaysSinceEpoch = 16430') = 1";
      String inQuery = "SELECT COUNT(*) FROM mytable WHERE DestAirportID IN (SELECT DestAirportID FROM mytable WHERE "
          + "DaysSinceEpoch = 16430)";
      testQuery(inPartitionedSubqueryQuery, inQuery);

      String notInPartitionedSubqueryQuery =
          "SELECT COUNT(*) FROM mytable WHERE INPARTITIONEDSUBQUERY(DestAirportID, 'SELECT IDSET(DestAirportID) FROM "
              + "mytable WHERE DaysSinceEpoch = 16430') = 0";
      String notInQuery =
          "SELECT COUNT(*) FROM mytable WHERE DestAirportID NOT IN (SELECT DestAirportID FROM mytable WHERE "
              + "DaysSinceEpoch = 16430)";
      testQuery(notInPartitionedSubqueryQuery, notInQuery);
    }
  }

  /**
   * Test to ensure that broker response contains expected stats
   *
   * @throws Exception
   */
  public void testBrokerResponseMetadata()
      throws Exception {
    String[] queries = new String[]{
        // matching query
        "SELECT count(*) FROM mytable",
        // query that does not match any row
        "SELECT count(*) FROM mytable where non_existing_column='non_existing_value'",
        // query a non existing table
        "SELECT count(*) FROM mytable_foo"
    };
    String[] statNames = new String[]{
        "totalDocs", "numServersQueried", "numServersResponded", "numSegmentsQueried", "numSegmentsProcessed",
        "numSegmentsMatched", "numDocsScanned", "totalDocs", "timeUsedMs", "numEntriesScannedInFilter",
        "numEntriesScannedPostFilter"
    };

    for (String query : queries) {
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
   * Test queries from the query file.
   */
  public void testQueriesFromQueryFile()
      throws Exception {
    InputStream inputStream =
        BaseClusterIntegrationTestSet.class.getClassLoader().getResourceAsStream(getQueryFileName());
    assertNotNull(inputStream);

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String queryString;
      while ((queryString = reader.readLine()) != null) {
        // Skip commented line and empty line.
        queryString = queryString.trim();
        if (queryString.startsWith("#") || queryString.isEmpty()) {
          continue;
        }

        JsonNode query = JsonUtils.stringToJsonNode(queryString);
        String pinotQuery = query.get("sql").asText();
        JsonNode hsqls = query.get("hsqls");
        String h2Query;
        if (hsqls == null || hsqls.isEmpty()) {
          h2Query = pinotQuery;
        } else {
          h2Query = hsqls.get(0).asText();
        }
        try {
          testQuery(pinotQuery, h2Query);
        } catch (Exception e) {
          e.printStackTrace();
          LOGGER.error("Failed to test Pinot query: {} with H2 query: {}.", pinotQuery, h2Query, e);
          throw e;
        }
      }
    }
  }

  /**
   * Test queries generated by query generator.
   *
   * @throws Exception
   */
  public void testGeneratedQueries()
      throws Exception {
    // default test with MV columns, without using multistage engine
    testGeneratedQueries(true, false);
  }

  protected void testGeneratedQueries(boolean withMultiValues, boolean useMultistageEngine)
      throws Exception {
    QueryGenerator queryGenerator = getQueryGenerator();
    queryGenerator.setSkipMultiValuePredicates(!withMultiValues);
    queryGenerator.setUseMultistageEngine(useMultistageEngine);
    int numQueriesToGenerate = getNumQueriesToGenerate();
    for (int i = 0; i < numQueriesToGenerate; i++) {
      QueryGenerator.Query query = queryGenerator.generateQuery();
      if (useMultistageEngine) {
        if (withMultiValues) {
          // For multistage query with MV columns, we need to use Pinot query string for testing.
          testQuery(query.generatePinotQuery().replace("`", "\""), query.generateH2Query());
        } else {
          // multistage engine follows standard SQL thus should use H2 query string for testing.
          testQuery(query.generateH2Query().replace("`", "\""), query.generateH2Query());
        }
      } else {
        testQuery(query.generatePinotQuery(), query.generateH2Query());
      }
    }
  }

  /**
   * Test invalid queries which should cause query exceptions.
   *
   * @throws Exception
   */
  public void testQueryExceptions()
      throws Exception {
    testQueryException("POTATO", QueryException.SQL_PARSING_ERROR_CODE);
    testQueryException("SELECT COUNT(*) FROM potato", QueryException.TABLE_DOES_NOT_EXIST_ERROR_CODE);
    testQueryException("SELECT POTATO(ArrTime) FROM mytable", QueryException.QUERY_EXECUTION_ERROR_CODE);
    testQueryException("SELECT COUNT(*) FROM mytable where ArrTime = 'potato'",
        QueryException.QUERY_EXECUTION_ERROR_CODE);
  }

  private void testQueryException(String query, int errorCode)
      throws Exception {
    JsonNode jsonObject = postQuery(query);
    assertEquals(jsonObject.get("exceptions").get(0).get("errorCode").asInt(), errorCode);
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
      if (!InstanceTypeUtils.isServer(instance)) {
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
          if ((routingTable.isEmpty()) != shouldBeEmpty) {
            return false;
          }
        }
        return true;
      } catch (Exception e) {
        return null;
      }
    }, 60_000L, errorMessage);
  }

  public void testReset(TableType tableType)
      throws Exception {
    String rawTableName = getTableName();

    // reset the table.
    resetTable(rawTableName, tableType, null);

    // wait for all live messages clear the queue.
    List<String> instances = _helixResourceManager.getServerInstancesForTable(rawTableName, tableType);
    PropertyKey.Builder keyBuilder = _helixDataAccessor.keyBuilder();
    TestUtils.waitForCondition(aVoid -> {
      int liveMessageCount = 0;
      for (String instanceName : instances) {
        List<Message> messages = _helixDataAccessor.getChildValues(keyBuilder.messages(instanceName), true);
        liveMessageCount += messages.size();
      }
      return liveMessageCount == 0;
    }, 30_000L, "Failed to wait for all segment reset messages clear helix state transition!");

    // Check that all segment states come back to ONLINE.
    TestUtils.waitForCondition(aVoid -> {
      // check external view and wait for everything to come back online
      ExternalView externalView = _helixAdmin.getResourceExternalView(getHelixClusterName(),
          TableNameBuilder.forType(tableType).tableNameWithType(rawTableName));
      for (Map<String, String> externalViewStateMap : externalView.getRecord().getMapFields().values()) {
        for (String state : externalViewStateMap.values()) {
          if (!CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(state)
              && !CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING.equals(state)) {
            return false;
          }
        }
      }
      return true;
    }, 30_000L, "Failed to wait for all segments come back online");
  }

  public String reloadTableAndValidateResponse(String tableName, TableType tableType, boolean forceDownload)
      throws IOException {
    String response =
        sendPostRequest(_controllerRequestURLBuilder.forTableReload(tableName, tableType, forceDownload), null);
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    JsonNode tableLevelDetails =
        JsonUtils.stringToJsonNode(StringEscapeUtils.unescapeJava(response.split(": ")[1])).get(tableNameWithType);
    String isZKWriteSuccess = tableLevelDetails.get("reloadJobMetaZKStorageStatus").asText();
    assertEquals(isZKWriteSuccess, "SUCCESS");
    String jobId = tableLevelDetails.get("reloadJobId").asText();
    String jobStatusResponse = sendGetRequest(_controllerRequestURLBuilder.forControllerJobStatus(jobId));
    JsonNode jobStatus = JsonUtils.stringToJsonNode(jobStatusResponse);

    // Validate all fields are present
    assertEquals(jobStatus.get("metadata").get("jobId").asText(), jobId);
    assertEquals(jobStatus.get("metadata").get("jobType").asText(), "RELOAD_SEGMENT");
    assertEquals(jobStatus.get("metadata").get("tableName").asText(), tableNameWithType);
    return jobId;
  }

  public boolean isReloadJobCompleted(String reloadJobId)
      throws Exception {
    String jobStatusResponse = sendGetRequest(_controllerRequestURLBuilder.forControllerJobStatus(reloadJobId));
    JsonNode jobStatus = JsonUtils.stringToJsonNode(jobStatusResponse);

    assertEquals(jobStatus.get("metadata").get("jobId").asText(), reloadJobId);
    assertEquals(jobStatus.get("metadata").get("jobType").asText(), "RELOAD_SEGMENT");
    return jobStatus.get("totalSegmentCount").asInt() == jobStatus.get("successCount").asInt();
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
    assertEquals(queryResponse.get("resultTable").get("dataSchema").get("columnNames").size(), schema.size());
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
        return testQueryResponse.get("resultTable").get("rows").get(0).get(0).asLong() == countStarResult;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate default values for new columns");

    // Select star query should return all the columns
    queryResponse = postQuery(selectStarQuery);
    assertEquals(queryResponse.get("exceptions").size(), 0);
    JsonNode resultTable = queryResponse.get("resultTable");
    assertEquals(resultTable.get("dataSchema").get("columnNames").size(), schema.size());
    assertEquals(resultTable.get("rows").size(), 10);

    // Test aggregation query to include querying all segemnts (including realtime)
    String aggregationQuery = "SELECT SUMMV(NewIntMVDimension) FROM " + rawTableName;
    queryResponse = postQuery(aggregationQuery);
    assertEquals(queryResponse.get("exceptions").size(), 0);

    // Test filter on all new added columns
    String countStarQuery = "SELECT COUNT(*) FROM " + rawTableName
        + " WHERE NewIntSVDimension < 0 AND NewLongSVDimension < 0 AND NewFloatSVDimension < 0 AND "
        + "NewDoubleSVDimension < 0 AND NewStringSVDimension = 'null' AND NewIntMVDimension < 0 AND "
        + "NewLongMVDimension < 0 AND NewFloatMVDimension < 0 AND NewDoubleMVDimension < 0 AND "
        + "NewStringMVDimension = 'null' AND NewIntMetric = 0 AND NewLongMetric = 0 AND NewFloatMetric = 0 "
        + "AND NewDoubleMetric = 0 AND NewBytesMetric = ''";
    queryResponse = postQuery(countStarQuery);
    assertEquals(queryResponse.get("exceptions").size(), 0);
    assertEquals(queryResponse.get("resultTable").get("rows").get(0).get(0).asLong(), countStarResult);
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
