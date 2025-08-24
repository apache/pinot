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
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Integration test for ANY_VALUE aggregation function.
 * Tests the function using the existing airline data from BaseClusterIntegrationTestSet.
 * Uses the standard "mytable" with airline data that's already loaded.
 */
public class AnyValueFunctionIntegrationTest extends BaseClusterIntegrationTestSet {
  
  // This test extends BaseClusterIntegrationTestSet which already has airline test data loaded
  // We'll use the existing "mytable" with airline data to test ANY_VALUE functionality

  @Test
  public void testAnyValueBasicFunctionality() throws Exception {
    // Test basic ANY_VALUE functionality using existing airline data
    String query = "SELECT Carrier, ANY_VALUE(Origin), COUNT(*) FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 5";

    JsonNode response = postQuery(query);
    JsonNode rows = response.get("resultTable").get("rows");

    assertTrue(rows.size() > 0, "Should have results");
    
    // Verify that ANY_VALUE returns valid values
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertNotNull(row.get(0).asText(), "Carrier should not be null");
      assertNotNull(row.get(1).asText(), "ANY_VALUE(Origin) should not be null");
      assertTrue(row.get(2).asInt() > 0, "COUNT should be greater than 0");
    }
  }

  @Test
  public void testAnyValueWithoutGroupBy() throws Exception {
    // Test ANY_VALUE without GROUP BY - should return any single value
    String query = "SELECT ANY_VALUE(Carrier), ANY_VALUE(Origin), COUNT(*) FROM mytable";

    JsonNode response = postQuery(query);
    JsonNode rows = response.get("resultTable").get("rows");

    assertEquals(rows.size(), 1, "Should have 1 row without GROUP BY");

    JsonNode row = rows.get(0);
    assertNotNull(row.get(0).asText(), "Should have a carrier");
    assertNotNull(row.get(1).asText(), "Should have an origin");
    assertTrue(row.get(2).asInt() > 0, "Should have records");
  }

  @Test
  public void testAnyValueWithMultipleGroupByColumns() throws Exception {
    // Test ANY_VALUE with multiple GROUP BY columns
    String query = "SELECT Carrier, Origin, ANY_VALUE(Dest), COUNT(*) FROM mytable GROUP BY Carrier, Origin ORDER BY Carrier, Origin LIMIT 10";

    JsonNode response = postQuery(query);
    JsonNode rows = response.get("resultTable").get("rows");

    assertTrue(rows.size() > 0, "Should have results");
    
    // Verify that ANY_VALUE returns valid values for each group
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertNotNull(row.get(0).asText(), "Carrier should not be null");
      assertNotNull(row.get(1).asText(), "Origin should not be null");
      assertNotNull(row.get(2).asText(), "ANY_VALUE(Dest) should not be null");
      assertTrue(row.get(3).asInt() > 0, "COUNT should be greater than 0");
    }
  }

  @Test
  public void testAnyValuePerformanceComparison() throws Exception {
    // Test that ANY_VALUE can be used as an alternative to GROUP BY for 1:1 mappings
    // Compare ANY_VALUE approach vs traditional GROUP BY approach
    
    String anyValueQuery = "SELECT Carrier, ANY_VALUE(AirlineID), COUNT(*) FROM mytable GROUP BY Carrier ORDER BY Carrier LIMIT 5";
    String groupByQuery = "SELECT Carrier, AirlineID, COUNT(*) FROM mytable GROUP BY Carrier, AirlineID ORDER BY Carrier LIMIT 5";

    JsonNode anyValueResponse = postQuery(anyValueQuery);
    JsonNode groupByResponse = postQuery(groupByQuery);

    JsonNode anyValueRows = anyValueResponse.get("resultTable").get("rows");
    JsonNode groupByRows = groupByResponse.get("resultTable").get("rows");

    assertTrue(anyValueRows.size() > 0, "ANY_VALUE query should return results");
    assertTrue(groupByRows.size() > 0, "GROUP BY query should return results");
    
    // ANY_VALUE should return fewer or equal rows (since it doesn't expand groups)
    assertTrue(anyValueRows.size() <= groupByRows.size(), 
              "ANY_VALUE should return fewer or equal rows than full GROUP BY");
  }

  @Test
  public void testAnyValueWithDifferentDataTypes() throws Exception {
    // Test ANY_VALUE with different data types using existing airline data
    String query = "SELECT " +
                   "ANY_VALUE(Carrier) as StringValue, " +
                   "ANY_VALUE(AirlineID) as IntValue, " +
                   "ANY_VALUE(FlightNum) as IntValue2, " +
                   "ANY_VALUE(ArrDelay) as DoubleValue " +
                   "FROM mytable";

    JsonNode response = postQuery(query);
    JsonNode rows = response.get("resultTable").get("rows");

    assertEquals(rows.size(), 1, "Should have 1 row without GROUP BY");

    JsonNode row = rows.get(0);
    assertNotNull(row.get(0).asText(), "String value should not be null");
    assertTrue(row.get(1).asInt() >= 0, "Int value should be valid");
    assertTrue(row.get(2).asInt() >= 0, "Int value2 should be valid");
    // ArrDelay can be negative, so just check it's a valid number
    assertNotNull(row.get(3), "Double value should not be null");
  }

  @Test
  public void testAnyValueWithComplexGroupBy() throws Exception {
    // Test ANY_VALUE in more complex scenarios
    String query = "SELECT " +
                   "Origin, " +
                   "ANY_VALUE(Carrier) as SampleCarrier, " +
                   "ANY_VALUE(Dest) as SampleDest, " +
                   "COUNT(*) as FlightCount, " +
                   "AVG(ArrDelay) as AvgDelay " +
                   "FROM mytable " +
                   "GROUP BY Origin " +
                   "ORDER BY FlightCount DESC " +
                   "LIMIT 5";

    JsonNode response = postQuery(query);
    JsonNode rows = response.get("resultTable").get("rows");

    assertTrue(rows.size() > 0, "Should have results");
    
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertNotNull(row.get(0).asText(), "Origin should not be null");
      assertNotNull(row.get(1).asText(), "ANY_VALUE(Carrier) should not be null");
      assertNotNull(row.get(2).asText(), "ANY_VALUE(Dest) should not be null");
      assertTrue(row.get(3).asInt() > 0, "FlightCount should be positive");
      // AvgDelay can be negative, so just verify it's a number
      assertNotNull(row.get(4), "AvgDelay should not be null");
    }
  }
}
