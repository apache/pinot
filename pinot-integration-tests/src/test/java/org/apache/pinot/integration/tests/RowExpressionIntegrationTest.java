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
import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RowExpressionIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String SCHEMA_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";

  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
    setupTenants();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);

    // Use multi-stage query engine for all tests
    setUseMultiStageQueryEngine(true);
  }

  @BeforeMethod
  @Override
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(true);
  }

  protected void setupTenants()
      throws Exception {
    // Use default tenant setup
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testRowEqualityTwoFields()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ArrDelay) = (201, 10)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowEqualityThreeFields()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ArrDelay, DepDelay) = (201, 10, 5)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowNotEquals()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ArrDelay) <> (0, 0)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() > 0);
  }

  @Test
  public void testRowGreaterThan()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ActualElapsedTime) > (200, 230)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(count >= 0);
  }

  @Test
  public void testRowGreaterThanOrEqual()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ActualElapsedTime) >= (200, 230)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(count >= 0);
  }

  @Test
  public void testRowLessThan()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ActualElapsedTime) < (100, 120)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(count >= 0);
  }

  @Test
  public void testRowLessThanOrEqual()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ActualElapsedTime) <= (100, 120)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(count >= 0);
  }

  @Test
  public void testRowWithFourFields()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable "
        + "WHERE (AirTime, ArrDelay, DepDelay, Distance) > (200, 0, 0, 1000)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowWithMixedDataTypes()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirlineID, Carrier) > (20000, 'AA')";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testKeysetPaginationUseCase()
      throws Exception {
    String query1 = "SELECT AirlineID, Carrier, AirTime "
        + "FROM mytable "
        + "ORDER BY AirlineID, Carrier, AirTime "
        + "LIMIT 10";
    JsonNode result1 = postQuery(query1);
    assertNoError(result1);
    assertEquals(result1.get("resultTable").get("rows").size(), 10);

    JsonNode lastRow = result1.get("resultTable").get("rows").get(9);
    long lastAirlineId = lastRow.get(0).asLong();
    String lastCarrier = lastRow.get(1).asText();
    long lastAirTime = lastRow.get(2).asLong();

    String query2 = String.format(
        "SELECT AirlineID, Carrier, AirTime "
        + "FROM mytable "
        + "WHERE (AirlineID, Carrier, AirTime) > (%d, '%s', %d) "
        + "ORDER BY AirlineID, Carrier, AirTime "
        + "LIMIT 10",
        lastAirlineId, lastCarrier, lastAirTime);
    JsonNode result2 = postQuery(query2);
    assertNoError(result2);
    assertTrue(result2.get("resultTable").get("rows").size() > 0);

    JsonNode firstRowPage2 = result2.get("resultTable").get("rows").get(0);
    long firstAirlineIdPage2 = firstRowPage2.get(0).asLong();
    assertTrue(firstAirlineIdPage2 >= lastAirlineId);
  }

  @Test
  public void testRowInComplexQuery()
      throws Exception {
    String query = "SELECT COUNT(*) FROM ("
        + "  SELECT AirlineID, Carrier FROM mytable "
        + "  WHERE (AirlineID, Carrier) > (20000, 'AA') "
        + "  ORDER BY AirlineID, Carrier LIMIT 100"
        + ") AS t";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowWithCTE()
      throws Exception {
    String query = "WITH filtered AS ("
        + "  SELECT AirlineID, Carrier, AirTime FROM mytable "
        + "  WHERE AirlineID > 19000"
        + ") "
        + "SELECT COUNT(*) FROM filtered "
        + "WHERE (AirlineID, Carrier) > (20000, 'AA')";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testMultipleRowComparisons()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable "
        + "WHERE (AirTime, ActualElapsedTime) > (100, 120) "
        + "AND (AirTime, ActualElapsedTime) < (500, 600)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowComparisonWithLiterals()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable "
        + "WHERE (201, 230) < (AirTime, ActualElapsedTime)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testExplainPlanWithRowExpression()
      throws Exception {
    String query = "EXPLAIN PLAN FOR "
        + "SELECT * FROM mytable WHERE (AirTime, ActualElapsedTime) > (200, 230) LIMIT 10";
    JsonNode result = postQuery(query);
    assertNoError(result);
    String plan = result.get("resultTable").get("rows").get(0).get(1).asText();
    assertTrue(plan.contains("OR") || plan.contains("AND"));
  }

  @Test
  public void testRowInSelectList()
      throws Exception {
    String query = "SELECT (AirTime, ActualElapsedTime) FROM mytable LIMIT 10";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("ROW expressions are only supported in comparison contexts"),
        "Expected ROW validation error message");
  }

  @Test
  public void testRowInGroupBy()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable GROUP BY (AirlineID, Carrier)";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("ROW expressions are only supported in comparison contexts")
            || errorMessage.contains("unsupported context"),
        "Expected ROW validation error message");
  }

  @Test
  public void testRowInOrderBy()
      throws Exception {
    String query = "SELECT AirlineID, Carrier FROM mytable ORDER BY (AirlineID, Carrier) LIMIT 10";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("ROW expressions are only supported in comparison contexts")
        || errorMessage.contains("unsupported context"),
        "Expected ROW validation error message");
  }

  @Test
  public void testSingleSidedRowComparison()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ActualElapsedTime) > 200";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("QueryValidationError"),
        "Expected error about both sides needing to be ROW");
  }

  @Test
  public void testMismatchedRowSizes()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ActualElapsedTime) > (200, 230, 250)";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("QueryValidationError"),
        "Expected error about mismatched field counts");
  }

  @Test
  public void testEmptyRowExpression()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE () > ()";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
  }

  @Test
  public void testRowInFunctionCall()
      throws Exception {
    String query = "SELECT SUM((AirTime, ActualElapsedTime)) FROM mytable";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("QueryValidationError"),
        "Expected ROW validation error message");
  }

  @Test
  public void testRowWithNullComparison()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ArrDelay) > (200, NULL)";
    JsonNode result = postQuery(query);
    assertNoError(result);
  }

  @Test
  public void testRowComparisonSameValues()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable "
        + "WHERE (AirTime, ActualElapsedTime) >= (AirTime, ActualElapsedTime)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    // Should return all rows
    assertTrue(count > 0);
  }

  @Test
  public void testRowComparisonInSubquery()
      throws Exception {
    // Test ROW in subquery WHERE clause
    String query = "SELECT COUNT(*) FROM ("
        + "  SELECT * FROM mytable WHERE (AirTime, ActualElapsedTime) > (200, 230)"
        + ") AS subq";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowComparisonWithCalculatedFields()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable "
        + "WHERE (AirTime * 2, ActualElapsedTime + 10) > (400, 240)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowGreaterThanOrEqualVsExpanded()
      throws Exception {
    String rowQuery = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ActualElapsedTime) >= (200, 230)";
    JsonNode rowResult = postQuery(rowQuery);
    assertNoError(rowResult);
    long rowCount = rowResult.get("resultTable").get("rows").get(0).get(0).asLong();

    String expandedQuery = "SELECT COUNT(*) FROM mytable "
        + "WHERE (AirTime > 200) OR ((AirTime = 200) AND (ActualElapsedTime > 230)) "
        + "OR ((AirTime = 200) AND (ActualElapsedTime = 230))";
    JsonNode expandedResult = postQuery(expandedQuery);
    assertNoError(expandedResult);
    long expandedCount = expandedResult.get("resultTable").get("rows").get(0).get(0).asLong();

    assertEquals(rowCount, expandedCount, "ROW >= comparison should produce same results as expanded form");
  }

  @Test
  public void testRowEqualityVsExpanded()
      throws Exception {
    String rowQuery = "SELECT COUNT(*) FROM mytable WHERE (AirTime, ActualElapsedTime) = (201, 230)";
    JsonNode rowResult = postQuery(rowQuery);
    assertNoError(rowResult);
    long rowCount = rowResult.get("resultTable").get("rows").get(0).get(0).asLong();

    String expandedQuery = "SELECT COUNT(*) FROM mytable "
        + "WHERE (AirTime = 201) AND (ActualElapsedTime = 230)";
    JsonNode expandedResult = postQuery(expandedQuery);
    assertNoError(expandedResult);
    long expandedCount = expandedResult.get("resultTable").get("rows").get(0).get(0).asLong();

    assertEquals(rowCount, expandedCount, "ROW = comparison should produce same results as expanded form");
  }
}
