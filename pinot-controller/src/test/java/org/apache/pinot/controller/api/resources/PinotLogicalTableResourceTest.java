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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTable;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotLogicalTableResourceTest extends ControllerTest {

  private static final String LOGICAL_TABLE_NAME = "test_logical_table";
  protected ControllerRequestURLBuilder _controllerRequestURLBuilder;

  @BeforeClass
  public void setUpClass()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    _controllerRequestURLBuilder = getControllerRequestURLBuilder();
  }

  @AfterClass
  public void tearDownClass() {
    stopController();
    stopZk();
  }

  @AfterMethod
  public void tearDown() {
    // cleans up the physical tables after each testcase
    cleanup();
  }

  @DataProvider
  public Object[][] tableNamesProvider() {
    return new Object[][]{
        {"test_logical_table", List.of("test_table_1", "test_table_2"), List.of("test_table_3")},
        {"test_logical_table", List.of("test_table_1", "db.test_table_2"), List.of("test_table_3")},
        {"test_logical_table", List.of("test_table_1", "test_table_2"), List.of("db.test_table_3")},
        {"db.test_logical_table", List.of("test_table_1", "test_table_2"), List.of("test_table_3")},
        {"db.test_logical_table", List.of("test_table_1", "db.test_table_2"), List.of("test_table_3")},
        {"db.test_logical_table", List.of("test_table_1", "test_table_2"), List.of("db.test_table_3")},
        {"db.test_logical_table", List.of("db.test_table_1", "db.test_table_2"), List.of("db.test_table_3")},
    };
  }

  @Test(dataProvider = "tableNamesProvider")
  public void testCreateUpdateDeleteLogicalTables(String logicalTableName, List<String> physicalTableNames,
      List<String> physicalTablesToUpdate)
      throws IOException {
    // verify logical table does not exist
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    String getLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableGet(logicalTableName);
    String updateLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableUpdate(logicalTableName);
    String deleteLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableDelete(logicalTableName);

    // verify logical table does not exist
    verifyLogicalTableDoesNotExists(getLogicalTableUrl);

    // setup physical and logical tables
    List<String> physicalTableNamesWithType = createPhysicalTables(physicalTableNames);
    LogicalTable logicalTable = getLogicalTable(logicalTableName, physicalTableNamesWithType);

    // create logical table
    String resp =
        ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
    assertEquals(resp,
        "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTableName + " logical table successfully added.\"}");

    // verify logical table
    verifyLogicalTableExists(getLogicalTableUrl, logicalTable);

    // update logical table and setup new physical tables
    List<String> tableNameToUpdateWithType = createPhysicalTables(physicalTablesToUpdate);
    tableNameToUpdateWithType.addAll(physicalTableNamesWithType);
    logicalTable = getLogicalTable(logicalTableName, tableNameToUpdateWithType);

    String response =
        ControllerTest.sendPutRequest(updateLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
    assertEquals(response,
        "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTableName + " logical table successfully updated.\"}");

    // verify updated logical table
    verifyLogicalTableExists(getLogicalTableUrl, logicalTable);

    // delete logical table
    String deleteResponse = ControllerTest.sendDeleteRequest(deleteLogicalTableUrl, getHeaders());
    assertEquals(deleteResponse, "{\"status\":\"" + logicalTableName + " logical table successfully deleted.\"}");

    // verify logical table is deleted
    verifyLogicalTableDoesNotExists(getLogicalTableUrl);
  }

  @Test
  public void testLogicalTableValidationTests()
      throws IOException {
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();

    // create physical tables
    List<String> physicalTableNames = List.of("test_table_1", "test_table_2");
    List<String> physicalTableNamesWithType = createPhysicalTables(physicalTableNames);

    // Test logical table name with _OFFLINE and _REALTIME is not allowed
    LogicalTable logicalTable = getLogicalTable("testLogicalTable_OFFLINE", physicalTableNamesWithType);
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Reason: 'tableName' should not end with _OFFLINE or _REALTIME"),
          e.getMessage());
    }

    // Test logical table name can not be same as existing physical table name
    logicalTable =
        getLogicalTable("test_table_1", physicalTableNamesWithType);
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Table name: test_table_1 already exists"), e.getMessage());
    }

    // Test empty physical table names is not allowed
    logicalTable =
        getLogicalTable(LOGICAL_TABLE_NAME, List.of());
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("'physicalTableNames' should not be null or empty"), e.getMessage());
    }
  }

  @Test
  public void testLogicalTableWithSameNameNotAllowed()
      throws IOException {
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    String getLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableGet(LOGICAL_TABLE_NAME);
    List<String> physicalTableNamesWithType = createPhysicalTables(List.of("test_table_1", "test_table_2"));

    LogicalTable logicalTable = getLogicalTable(LOGICAL_TABLE_NAME, physicalTableNamesWithType);
    ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
    verifyLogicalTableExists(getLogicalTableUrl, logicalTable);
    try {
      // create the same logical table again
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table: test_logical_table already exists"), e.getMessage());
    }

    // clean up the logical table
    String deleteLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableDelete(LOGICAL_TABLE_NAME);
    ControllerTest.sendDeleteRequest(deleteLogicalTableUrl, getHeaders());
    verifyLogicalTableDoesNotExists(getLogicalTableUrl);
  }

  @DataProvider
  public Object[][] physicalTableShouldExistProvider() {
    return new Object[][]{
        {LOGICAL_TABLE_NAME, List.of("test_table_1"), "unknown_table_OFFLINE"},
        {LOGICAL_TABLE_NAME, List.of("test_table_1"), "unknown_table_REALTIME"},
        {LOGICAL_TABLE_NAME, List.of("test_table_1"), "db.test_table_1_OFFLINE"},
        {LOGICAL_TABLE_NAME, List.of("test_table_1"), "db.test_table_1_REALTIME"},
    };
  }

  @Test(dataProvider = "physicalTableShouldExistProvider")
  public void testPhysicalTableShouldExist(String logicalTableName, List<String> physicalTableNames,
      String unknownTableName)
      throws IOException {
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();

    // setup physical tables
    List<String> physicalTableNamesWithType = createPhysicalTables(physicalTableNames);
    physicalTableNamesWithType.add(unknownTableName);

    // Test physical table should exist
    LogicalTable logicalTable = getLogicalTable(logicalTableName, physicalTableNamesWithType);
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("'" + unknownTableName + "' should be one of the existing tables"),
          e.getMessage());
    }
  }

  @Test
  public void testGetLogicalTableNames()
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    String getLogicalTableNamesUrl = _controllerRequestURLBuilder.forLogicalTableNamesGet();
    String response = ControllerTest.sendGetRequest(getLogicalTableNamesUrl, getHeaders());
    assertEquals(response, objectMapper.writeValueAsString(List.of()));

    // setup physical tables and logical tables
    List<String> logicalTableNames = List.of("db.test_logical_table_1", "test_logical_table_2", "test_logical_table_3");
    List<String> physicalTableNames = List.of("test_table_1", "test_table_2", "db.test_table_3");
    List<String> physicalTableNamesWithType = createPhysicalTables(physicalTableNames);

    for (int i = 0; i < logicalTableNames.size(); i++) {
      LogicalTable logicalTable = getLogicalTable(logicalTableNames.get(i), List.of(
          physicalTableNamesWithType.get(2 * i), physicalTableNamesWithType.get(2 * i + 1)));

      ControllerTest.sendPostRequest(_controllerRequestURLBuilder.forLogicalTableCreate(),
          logicalTable.toSingleLineJsonString(), getHeaders());
    }

    // verify logical table names
    String getLogicalTableNamesResponse = ControllerTest.sendGetRequest(getLogicalTableNamesUrl, getHeaders());
    assertEquals(getLogicalTableNamesResponse, objectMapper.writeValueAsString(logicalTableNames));

    // cleanup: delete logical tables
    for (String logicalTableName : logicalTableNames) {
      String deleteLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableDelete(logicalTableName);
      String deleteResponse = ControllerTest.sendDeleteRequest(deleteLogicalTableUrl, getHeaders());
      assertEquals(deleteResponse, "{\"status\":\"" + logicalTableName + " logical table successfully deleted.\"}");
    }
  }

  private void verifyLogicalTableExists(String getLogicalTableUrl, LogicalTable logicalTable)
      throws IOException {
    LogicalTable remoteLogicalTable =
        LogicalTable.fromString(ControllerTest.sendGetRequest(getLogicalTableUrl, getHeaders()));
    assertEquals(remoteLogicalTable, logicalTable);
  }

  private void verifyLogicalTableDoesNotExists(String getLogicalTableUrl) {
    try {
      ControllerTest.sendGetRequest(getLogicalTableUrl, getHeaders());
      fail("Logical Table GET request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table not found"), e.getMessage());
    }
  }

  private List<String> createPhysicalTables(List<String> physicalTableNames)
      throws IOException {
    List<String> physicalTableNamesWithType = new ArrayList<>();
    for (String physicalTable : physicalTableNames) {
      addDummySchema(physicalTable);
      TableConfig offlineTable = getOfflineTable(physicalTable);
      TableConfig realtimeTable = getRealtimeTable(physicalTable);
      addTableConfig(offlineTable);
      addTableConfig(realtimeTable);
      physicalTableNamesWithType.add(offlineTable.getTableName());
      physicalTableNamesWithType.add(realtimeTable.getTableName());
    }
    return physicalTableNamesWithType;
  }

  protected Map<String, String> getHeaders() {
    return Map.of();
  }

  private static LogicalTable getLogicalTable(String tableName, List<String> physicalTableNames) {
    LogicalTable logicalTable = new LogicalTable();
    logicalTable.setTableName(tableName);
    logicalTable.setPhysicalTableNames(physicalTableNames);
    return logicalTable;
  }

  private TableConfig getRealtimeTable(String physicalTable) {
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(physicalTable)
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap())
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .build();
  }

  private static TableConfig getOfflineTable(String physicalTable) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(physicalTable)
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .build();
  }
}
