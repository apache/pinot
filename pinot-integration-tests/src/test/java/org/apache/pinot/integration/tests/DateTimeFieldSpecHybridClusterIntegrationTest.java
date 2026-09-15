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
import org.apache.pinot.integration.tests.SharedHybridClusterIntegrationTestSuite.HybridScenarioLease;
import org.apache.pinot.integration.tests.SharedHybridClusterIntegrationTestSuite.SharedHybridSuiteLease;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Hybrid cluster integration test that uses one of the DateTimeFieldSpec as primary time column
public class DateTimeFieldSpecHybridClusterIntegrationTest {
  private static final String SCHEMA_WITH_DATETIME_FIELDSPEC_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_datetimefieldspecs.schema";
  private static final String TABLE_NAME = "mytableDateTimeFieldSpec";
  private static final String TOPIC_NAME = "DateTimeFieldSpecHybridClusterIntegrationTest";
  private static final String PRIMARY_TIME_COLUMN = "DaysSinceEpoch";

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
      _owner.setUpStandardScenario(_scenario, SCHEMA_WITH_DATETIME_FIELDSPEC_NAME,
          SharedHybridClusterIntegrationTestSuite.DEFAULT_HYBRID_COUNT);
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
  public void testDateTimeFieldSpecAsHybridTimeColumn(boolean useMultiStageQueryEngine)
      throws Exception {
    try {
      Schema schema = _owner.getScenarioSchema(TABLE_NAME);
      assertDateTimeField(schema, PRIMARY_TIME_COLUMN);
      assertDateTimeField(schema, "Year");
      assertDateTimeField(schema, "FlightDate");

      TableConfig offlineTableConfig = _owner.getScenarioTableConfig(TABLE_NAME, TableType.OFFLINE);
      TableConfig realtimeTableConfig = _owner.getScenarioTableConfig(TABLE_NAME, TableType.REALTIME);
      assertNotNull(offlineTableConfig);
      assertNotNull(realtimeTableConfig);
      assertEquals(offlineTableConfig.getValidationConfig().getTimeColumnName(), PRIMARY_TIME_COLUMN);
      assertEquals(realtimeTableConfig.getValidationConfig().getTimeColumnName(), PRIMARY_TIME_COLUMN);

      assertQueryCount(_owner, TABLE_NAME, SharedHybridClusterIntegrationTestSuite.DEFAULT_HYBRID_COUNT,
          useMultiStageQueryEngine);
      assertQueryCount(_owner, TABLE_NAME + "_OFFLINE", _scenario._offlineInputCount, useMultiStageQueryEngine);
      assertQueryCount(_owner, TABLE_NAME + "_REALTIME", _scenario._realtimeInputCount, useMultiStageQueryEngine);

      JsonNode dateTimeQueryResponse = _owner.queryScenario(
          "SELECT MIN(Year), MAX(Year), MIN(DaysSinceEpoch), MAX(DaysSinceEpoch) FROM " + TABLE_NAME,
          useMultiStageQueryEngine);
      assertNoQueryExceptions(dateTimeQueryResponse);
      JsonNode row = dateTimeQueryResponse.get("resultTable").get("rows").get(0);
      assertEquals(row.get(0).asInt(), 2014);
      assertEquals(row.get(1).asInt(), 2014);
      assertEquals(row.get(2).asInt(), 16071);
      assertEquals(row.get(3).asInt(), 16435);

      JsonNode flightDateResponse = _owner.queryScenario(
          "SELECT FlightDate FROM " + TABLE_NAME + " ORDER BY FlightDate LIMIT 1",
          useMultiStageQueryEngine);
      assertNoQueryExceptions(flightDateResponse);
      assertEquals(flightDateResponse.get("resultTable").get("rows").get(0).get(0).asText(), "2014-01-01");

      assertNotNull(_owner.getScenarioDebugInfo("debug/timeBoundary/" + TABLE_NAME));
    } finally {
      _owner.resetQueryEngine();
    }
  }

  private static void assertDateTimeField(Schema schema, String column) {
    assertNotNull(schema);
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    assertNotNull(fieldSpec);
    assertEquals(fieldSpec.getFieldType(), FieldSpec.FieldType.DATE_TIME);
  }

  private static void assertQueryCount(SharedHybridClusterIntegrationTestSuite owner, String tableName,
      long expectedCount, boolean useMultiStageQueryEngine)
      throws Exception {
    JsonNode response = owner.queryScenario("SELECT COUNT(*) FROM " + tableName, useMultiStageQueryEngine);
    assertNoQueryExceptions(response);
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(), expectedCount);
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
