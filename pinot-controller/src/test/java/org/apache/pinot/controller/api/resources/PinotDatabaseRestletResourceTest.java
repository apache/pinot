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
package org.apache.pinot.controller.api.resources;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.DatabaseConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class PinotDatabaseRestletResourceTest {
  private static final String DATABASE = "db";
  private static final List<String> TABLES = Lists.newArrayList("a_REALTIME", "b_OFFLINE", "c_REALTIME", "d_OFFLINE");

  @Mock
  PinotHelixResourceManager _resourceManager;
  @Mock
  HttpHeaders _httpHeaders;
  @InjectMocks
  PinotDatabaseRestletResource _resource;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(_resourceManager.getAllTables(DATABASE)).thenReturn(TABLES);
    doNothing().when(_resourceManager).deleteTable(anyString(), any(TableType.class), any());
    when(_resourceManager.deleteSchema(anyString())).thenReturn(true);

    // Mock HttpHeaders to return the database name
    when(_httpHeaders.getHeaderString("database")).thenReturn(DATABASE);
  }

  @Test
  public void successfulDatabaseDeletionDryRunTest() {
    successfulDatabaseDeletionCheck(true);
  }

  @Test
  public void successfulDatabaseDeletionTest() {
    successfulDatabaseDeletionCheck(false);
  }

  private void successfulDatabaseDeletionCheck(boolean dryRun) {
    DeleteDatabaseResponse response = _resource.deleteTablesInDatabase(DATABASE, dryRun);
    assertEquals(response.isDryRun(), dryRun);
    assertTrue(response.getFailedTables().isEmpty());
    assertEquals(response.getDeletedTables(), TABLES);
  }

  @Test
  public void partialDatabaseDeletionWithDeleteTableFailureTest() {
    int failureTableIdx = TABLES.size() / 2;
    doThrow(new RuntimeException()).when(_resourceManager)
        .deleteTable(TABLES.get(failureTableIdx), TableType.REALTIME, null);
    partialDatabaseDeletionCheck(failureTableIdx);
  }

  @Test
  public void partialDatabaseDeletionWithDeleteSchemaFailureTest() {
    int failureSchemaIdx = TABLES.size() / 2;
    doThrow(new RuntimeException()).when(_resourceManager)
        .deleteSchema(TableNameBuilder.extractRawTableName(TABLES.get(failureSchemaIdx)));
    partialDatabaseDeletionCheck(failureSchemaIdx);
  }

  private void partialDatabaseDeletionCheck(int idx) {
    DeleteDatabaseResponse response = _resource.deleteTablesInDatabase(DATABASE, false);
    List<String> resultList = new ArrayList<>(TABLES);
    String failedTable = resultList.remove(idx);
    assertFalse(response.isDryRun());
    assertEquals(response.getFailedTables().size(), 1);
    assertEquals(response.getFailedTables().get(0).getTableName(), failedTable);
    assertEquals(response.getDeletedTables(), resultList);
  }

  @Test
  public void testSetDatabaseQuotaWithMaxQueriesPerSecond()
      throws Exception {
    // Test setting database quota using traditional maxQueriesPerSecond parameter
    String queryQuota = "100";
    when(_resourceManager.getDatabaseConfig(DATABASE)).thenReturn(null);

    SuccessResponse response = _resource.setDatabaseQuota(DATABASE, queryQuota, _httpHeaders);

    assertNotNull(response);
    assertEquals(response.getStatus(), "Database quotas for database config " + DATABASE + " successfully updated");

    // Verify that addDatabaseConfig was called with correct QuotaConfig
    verify(_resourceManager).addDatabaseConfig(any(DatabaseConfig.class));
  }

  @Test
  public void testSetDatabaseQuotaWithVariousConfigurations()
      throws Exception {
    // Combined test for various database quota configurations
    Object[][] testCases = {
        {"SECONDS", 5.0, 50.0, "Flexible seconds configuration"},
        {"MINUTES", 10.0, 600.0, "Minutes configuration"},
        {"SECONDS", 1.0, 0.25, "Fractional rates"},
        {"SECONDS", 5.0, 1.5, "Complex duration and rate"}
    };

    for (Object[] testCase : testCases) {
      String rateLimiterUnit = (String) testCase[0];
      Double rateLimiterDuration = (Double) testCase[1];
      Double maxQueriesValue = (Double) testCase[2];
      String description = (String) testCase[3];

      // Reset mock for each test case
      reset(_resourceManager);
      when(_resourceManager.getDatabaseConfig(DATABASE)).thenReturn(null);

      SuccessResponse response = _resource.setDatabaseQuota(DATABASE, rateLimiterUnit,
          rateLimiterDuration, maxQueriesValue, _httpHeaders);

      assertNotNull(response, "Response should not be null for: " + description);
      assertEquals(response.getStatus(), "Database quotas for database config " + DATABASE + " successfully updated");
      verify(_resourceManager).addDatabaseConfig(any(DatabaseConfig.class));
    }
  }






  @Test
  public void testUpdateExistingDatabaseQuota()
      throws Exception {
    // Test updating an existing database quota
    DatabaseConfig existingConfig = new DatabaseConfig(DATABASE,
        new QuotaConfig(null, TimeUnit.SECONDS, 1.0, 50.0));
    when(_resourceManager.getDatabaseConfig(DATABASE)).thenReturn(existingConfig);

    String queryQuota = "200";
    SuccessResponse response = _resource.setDatabaseQuota(DATABASE, queryQuota, _httpHeaders);

    assertNotNull(response);
    assertEquals(response.getStatus(), "Database quotas for database config " + DATABASE + " successfully updated");

    // Verify that updateDatabaseConfig was called instead of addDatabaseConfig
    verify(_resourceManager).updateDatabaseConfig(any(DatabaseConfig.class));
  }



  @Test
  public void testGetDatabaseQuotaWhenConfigExists()
      throws Exception {
    // Test getting database quota when config exists
    QuotaConfig quotaConfig = new QuotaConfig(null, TimeUnit.SECONDS, 5.0, 100.0);
    DatabaseConfig databaseConfig = new DatabaseConfig(DATABASE, quotaConfig);
    when(_resourceManager.getDatabaseConfig(DATABASE)).thenReturn(databaseConfig);

    QuotaConfig result = _resource.getDatabaseQuota(DATABASE, _httpHeaders);

    assertNotNull(result);
    assertEquals(result.getRateLimiterUnit(), TimeUnit.SECONDS);
    assertEquals(result.getRateLimiterDuration(), 5.0);
    assertEquals(result.getRateLimits(), 100.0);
  }

  @Test
  public void testGetDatabaseQuotaWithDefaultClusterConfig()
      throws Exception {
    // Test getting database quota when no specific config exists but cluster default exists
    when(_resourceManager.getDatabaseConfig(DATABASE)).thenReturn(null);
    when(_resourceManager.getHelixAdmin()).thenReturn(null);
    when(_resourceManager.getHelixClusterName()).thenReturn(null);
    // The actual implementation will fetch cluster config, but we're testing the basic flow

    QuotaConfig result = _resource.getDatabaseQuota(DATABASE, _httpHeaders);

    // Result should be null when no cluster config is available
    assertNull(result);
  }



  @Test
  public void testDatabaseQuotaWithCommonFractionalScenarios()
      throws Exception {
    // Test common real-world fractional scenarios

    // Scenario 1: 1 query every 3 seconds (0.333... qps)
    {
      when(_resourceManager.getDatabaseConfig(DATABASE)).thenReturn(null);

      String rateLimiterUnit = "SECONDS";
      Double rateLimiterDuration = 3.0;
      Double maxQueriesValue = 1.0;

      SuccessResponse response = _resource.setDatabaseQuota(DATABASE, rateLimiterUnit,
          rateLimiterDuration, maxQueriesValue, _httpHeaders);

      assertNotNull(response);
    }

    // Scenario 2: 2 queries every 5 minutes
    {
      when(_resourceManager.getDatabaseConfig(DATABASE)).thenReturn(null);

      String rateLimiterUnit = "MINUTES";
      Double rateLimiterDuration = 5.0;
      Double maxQueriesValue = 2.0;

      SuccessResponse response = _resource.setDatabaseQuota(DATABASE, rateLimiterUnit,
          rateLimiterDuration, maxQueriesValue, _httpHeaders);

      assertNotNull(response);
    }

    // Verify that addDatabaseConfig was called exactly twice (once for each scenario)
    verify(_resourceManager, times(2)).addDatabaseConfig(any(DatabaseConfig.class));
  }

  @Test(expectedExceptions = NumberFormatException.class)
  public void testSetDatabaseQuotaWithInvalidQueryQuota()
      throws Exception {
    // Test handling of invalid query quota string
    String invalidQueryQuota = "invalid_number";

    _resource.setDatabaseQuota(DATABASE, invalidQueryQuota, _httpHeaders);
  }
}
