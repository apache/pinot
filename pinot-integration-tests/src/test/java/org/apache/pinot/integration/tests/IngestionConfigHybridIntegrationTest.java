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
import java.util.Locale;
import org.apache.pinot.integration.tests.SharedHybridClusterIntegrationTestSuite.HybridScenarioLease;
import org.apache.pinot.integration.tests.SharedHybridClusterIntegrationTestSuite.SharedHybridSuiteLease;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests batch and stream ingestion configs on a hybrid table without repeating generic hybrid query coverage.
public class IngestionConfigHybridIntegrationTest {
  private static final String TABLE_NAME = "mytableIngestionConfig";
  private static final String TOPIC_NAME = "IngestionConfigHybridIntegrationTest";

  private SharedHybridSuiteLease _suiteLease;
  private SharedHybridClusterIntegrationTestSuite _owner;
  private HybridScenarioLease _scenario;

  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Throwable {
    Throwable primaryFailure = null;
    try {
      _suiteLease = SharedHybridClusterIntegrationTestSuite.acquireSharedSuite();
      _owner = _suiteLease.getOwner();
      _scenario = _owner.newScenario(TABLE_NAME, TOPIC_NAME);
      _owner.setUpIngestionConfigScenario(_scenario);
    } catch (Throwable t) {
      primaryFailure = t;
      throw t;
    } finally {
      if (primaryFailure != null) {
        if (_scenario != null) {
          try {
            _owner.closeScenario(_scenario, primaryFailure);
          } finally {
            _scenario = null;
          }
        }
        if (_suiteLease != null) {
          try {
            _suiteLease.close(primaryFailure);
          } finally {
            _suiteLease = null;
            _owner = null;
          }
        }
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    try {
      JsonNode response = _owner.queryScenario("SELECT COUNT(*) FROM " + TABLE_NAME, useMultiStageQueryEngine);
      assertNoQueryExceptions(response);
      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(),
          SharedHybridClusterIntegrationTestSuite.FILTERED_HYBRID_COUNT);

      response = _owner.queryScenario("SELECT millisSinceEpoch FROM " + TABLE_NAME, useMultiStageQueryEngine);
      assertNoQueryExceptions(response);
      assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(),
          "millisSinceEpoch");
      assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG");

      for (String tableName : new String[]{TABLE_NAME, TABLE_NAME + "_OFFLINE", TABLE_NAME + "_REALTIME"}) {
        assertAmPmTransform(_owner, tableName, useMultiStageQueryEngine);
        assertLowerCaseTransform(_owner, tableName, useMultiStageQueryEngine);
        assertFilteredRowsAbsent(_owner, tableName, useMultiStageQueryEngine);
      }
    } finally {
      _owner.resetQueryEngine();
    }
  }

  private static void assertAmPmTransform(SharedHybridClusterIntegrationTestSuite owner, String tableName,
      boolean useMultiStageQueryEngine)
      throws Exception {
    JsonNode response = owner.queryScenario("SELECT AmPm, DepTime FROM " + tableName, useMultiStageQueryEngine);
    assertNoQueryExceptions(response);
    JsonNode resultTable = response.get("resultTable");
    assertEquals(resultTable.get("dataSchema").get("columnNames").get(0).asText(), "AmPm");
    assertEquals(resultTable.get("dataSchema").get("columnNames").get(1).asText(), "DepTime");
    assertEquals(resultTable.get("dataSchema").get("columnDataTypes").get(0).asText(), "STRING");
    assertEquals(resultTable.get("dataSchema").get("columnDataTypes").get(1).asText(), "INT");
    assertFalse(resultTable.get("rows").isEmpty());
    for (JsonNode row : resultTable.get("rows")) {
      assertEquals(row.get(0).asText(), row.get(1).asInt() < 1200 ? "AM" : "PM");
    }
  }

  private static void assertLowerCaseTransform(SharedHybridClusterIntegrationTestSuite owner, String tableName,
      boolean useMultiStageQueryEngine)
      throws Exception {
    JsonNode response =
        owner.queryScenario("SELECT lowerCaseDestCityName FROM " + tableName, useMultiStageQueryEngine);
    assertNoQueryExceptions(response);
    JsonNode rows = response.get("resultTable").get("rows");
    assertFalse(rows.isEmpty());
    for (JsonNode row : rows) {
      String cityName = row.get(0).asText();
      assertEquals(cityName, cityName.toLowerCase(Locale.ROOT));
    }
  }

  private static void assertFilteredRowsAbsent(SharedHybridClusterIntegrationTestSuite owner, String tableName,
      boolean useMultiStageQueryEngine)
      throws Exception {
    JsonNode response = owner.queryScenario(
        "SELECT * FROM " + tableName + " WHERE AirlineID = 19393 OR ArrDelayMinutes <= 5",
        useMultiStageQueryEngine);
    assertNoQueryExceptions(response);
    assertTrue(response.get("resultTable").get("rows").isEmpty(), response.toString());
  }

  private static void assertNoQueryExceptions(JsonNode response) {
    assertNotNull(response);
    assertTrue(response.has("exceptions"), response.toString());
    assertTrue(response.get("exceptions").isEmpty(), response.toString());
  }

  @DataProvider(name = "useBothQueryEngines")
  public static Object[][] useBothQueryEngines() {
    return new Object[][]{{false}, {true}};
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Throwable {
    Throwable cleanupFailure = null;
    if (_scenario != null) {
      try {
        _owner.closeScenario(_scenario, null);
      } catch (Throwable t) {
        cleanupFailure = SharedHybridClusterIntegrationTestSuite.appendCleanupFailure(cleanupFailure, t);
      } finally {
        _scenario = null;
      }
    }
    if (_suiteLease != null) {
      try {
        _suiteLease.close(null);
      } catch (Throwable t) {
        cleanupFailure = SharedHybridClusterIntegrationTestSuite.appendCleanupFailure(cleanupFailure, t);
      } finally {
        _suiteLease = null;
        _owner = null;
      }
    }
    if (cleanupFailure != null) {
      throw cleanupFailure;
    }
  }
}
