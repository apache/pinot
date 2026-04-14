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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class RowExpressionTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "RowExpressionTest";
  private static final String SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    try {
      Schema schema = createSchema(SCHEMA_FILE_NAME);
      schema.setSchemaName(getTableName());
      return schema;
    } catch (IOException e) {
      throw new RuntimeException("Failed to load schema: " + SCHEMA_FILE_NAME, e);
    }
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    return unpackAvroData(_tempDir);
  }

  @BeforeMethod
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(true);
  }

  @Test
  public void testRowEqualityTwoFields()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE (AirTime, ArrDelay) = (201, 10)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowEqualityThreeFields()
      throws Exception {
    String query =
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE (AirTime, ArrDelay, DepDelay) = (201, 10, 5)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowNotEquals()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE (AirTime, ArrDelay) <> (0, 0)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() > 0);
  }

  @Test
  public void testRowGreaterThan()
      throws Exception {
    String query =
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE (AirTime, ActualElapsedTime) > (200, 230)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(count >= 0);
  }

  @Test
  public void testRowGreaterThanOrEqual()
      throws Exception {
    String query =
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE (AirTime, ActualElapsedTime) >= (200, 230)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(count >= 0);
  }

  @Test
  public void testRowLessThan()
      throws Exception {
    String query =
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE (AirTime, ActualElapsedTime) < (100, 120)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(count >= 0);
  }

  @Test
  public void testRowLessThanOrEqual()
      throws Exception {
    String query =
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE (AirTime, ActualElapsedTime) <= (100, 120)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    long count = result.get("resultTable").get("rows").get(0).get(0).asLong();
    assertTrue(count >= 0);
  }

  @Test
  public void testRowWithFourFields()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName()
        + " WHERE (AirTime, ArrDelay, DepDelay, Distance) > (200, 0, 0, 1000)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowWithMixedDataTypes()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE (AirlineID, Carrier) > (20000, 'AA')";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testKeysetPaginationUseCase()
      throws Exception {
    String query1 = "SELECT AirlineID, Carrier, AirTime "
        + "FROM " + getTableName() + " "
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
        + "FROM " + getTableName() + " "
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
        + "  SELECT AirlineID, Carrier FROM " + getTableName() + " "
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
        + "  SELECT AirlineID, Carrier, AirTime FROM " + getTableName() + " "
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
    String query = "SELECT COUNT(*) FROM " + getTableName() + " "
        + "WHERE (AirTime, ActualElapsedTime) > (100, 120) "
        + "AND (AirTime, ActualElapsedTime) < (500, 600)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowComparisonWithLiterals()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName() + " "
        + "WHERE (201, 230) < (AirTime, ActualElapsedTime)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testExplainPlanWithRowExpression()
      throws Exception {
    String query = "EXPLAIN PLAN FOR "
        + "SELECT * FROM " + getTableName() + " WHERE (AirTime, ActualElapsedTime) > (200, 230) LIMIT 10";
    JsonNode result = postQuery(query);
    assertNoError(result);
    String plan = result.get("resultTable").get("rows").get(0).get(1).asText();
    assertTrue(plan.contains("OR") || plan.contains("AND"));
  }

  @Test
  public void testRowInSelectList()
      throws Exception {
    String query = "SELECT (AirTime, ActualElapsedTime) FROM " + getTableName() + " LIMIT 10";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("ROW expressions are only supported in comparison contexts"),
        "Expected ROW validation error message");
  }

  @Test
  public void testRowInGroupBy()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName() + " GROUP BY (AirlineID, Carrier)";
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
    String query = "SELECT AirlineID, Carrier FROM " + getTableName()
        + " ORDER BY (AirlineID, Carrier) LIMIT 10";
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
    String query = "SELECT COUNT(*) FROM " + getTableName()
        + " WHERE (AirTime, ActualElapsedTime) > 200";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("QueryValidationError"),
        "Expected error about both sides needing to be ROW");
  }

  @Test
  public void testMismatchedRowSizes()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName()
        + " WHERE (AirTime, ActualElapsedTime) > (200, 230, 250)";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("QueryValidationError"),
        "Expected error about mismatched field counts");
  }

  @Test
  public void testEmptyRowExpression()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE () > ()";
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
  }

  @Test
  public void testRowInFunctionCall()
      throws Exception {
    String query = "SELECT SUM((AirTime, ActualElapsedTime)) FROM " + getTableName();
    JsonNode result = postQuery(query);
    assertTrue(result.get("exceptions").size() > 0, "Expected validation error");
    String errorMessage = result.get("exceptions").get(0).get("message").asText();
    assertTrue(errorMessage.contains("QueryValidationError"),
        "Expected ROW validation error message");
  }

  @Test
  public void testRowWithNullComparison()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName()
        + " WHERE (AirTime, ArrDelay) > (200, NULL)";
    JsonNode result = postQuery(query);
    assertNoError(result);
  }

  @Test
  public void testRowComparisonSameValues()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName() + " "
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
        + "  SELECT * FROM " + getTableName() + " WHERE (AirTime, ActualElapsedTime) > (200, 230)"
        + ") AS subq";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowComparisonWithCalculatedFields()
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + getTableName() + " "
        + "WHERE (AirTime * 2, ActualElapsedTime + 10) > (400, 240)";
    JsonNode result = postQuery(query);
    assertNoError(result);
    assertTrue(result.get("numRowsResultSet").asInt() >= 0);
  }

  @Test
  public void testRowGreaterThanOrEqualVsExpanded()
      throws Exception {
    String rowQuery = "SELECT COUNT(*) FROM " + getTableName()
        + " WHERE (AirTime, ActualElapsedTime) >= (200, 230)";
    JsonNode rowResult = postQuery(rowQuery);
    assertNoError(rowResult);
    long rowCount = rowResult.get("resultTable").get("rows").get(0).get(0).asLong();

    String expandedQuery = "SELECT COUNT(*) FROM " + getTableName() + " "
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
    String rowQuery = "SELECT COUNT(*) FROM " + getTableName()
        + " WHERE (AirTime, ActualElapsedTime) = (201, 230)";
    JsonNode rowResult = postQuery(rowQuery);
    assertNoError(rowResult);
    long rowCount = rowResult.get("resultTable").get("rows").get(0).get(0).asLong();

    String expandedQuery = "SELECT COUNT(*) FROM " + getTableName() + " "
        + "WHERE (AirTime = 201) AND (ActualElapsedTime = 230)";
    JsonNode expandedResult = postQuery(expandedQuery);
    assertNoError(expandedResult);
    long expandedCount = expandedResult.get("resultTable").get("rows").get(0).get(0).asLong();

    assertEquals(rowCount, expandedCount, "ROW = comparison should produce same results as expanded form");
  }
}
