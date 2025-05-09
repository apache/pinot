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
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
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
  public static final String BROKER_TENANT = "DefaultTenant";
  protected ControllerRequestURLBuilder _controllerRequestURLBuilder;
  private String _addLogicalTableUrl;

  @BeforeClass
  public void setUpClass()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    _controllerRequestURLBuilder = getControllerRequestURLBuilder();
    _addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
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
        {"test_logical_table", List.of("db.test_table_1", "db.test_table_2"), List.of("db.test_table_3")},
        {"test_table", List.of("db1.test_table", "db2.test_table"), List.of("db3.test_table")},
        {"db0.test_table", List.of("db1.test_table", "db2.test_table"), List.of("db3.test_table")},
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
    String getLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableGet(logicalTableName);
    String updateLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableUpdate(logicalTableName);
    String deleteLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableDelete(logicalTableName);

    // verify logical table does not exist
    verifyLogicalTableDoesNotExists(getLogicalTableUrl);

    // setup physical and logical tables
    List<String> physicalTableNamesWithType = createHybridTables(physicalTableNames);
    LogicalTableConfig
        logicalTableConfig = getDummyLogicalTableConfig(logicalTableName, physicalTableNamesWithType, BROKER_TENANT);

    // create logical table
    String resp =
        ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
    assertEquals(resp,
        "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTableName + " logical table successfully added.\"}");

    // verify logical table
    verifyLogicalTableExists(getLogicalTableUrl, logicalTableConfig);

    // update logical table and setup new physical tables
    List<String> tableNameToUpdateWithType = createHybridTables(physicalTablesToUpdate);
    tableNameToUpdateWithType.addAll(physicalTableNamesWithType);
    logicalTableConfig = getDummyLogicalTableConfig(logicalTableName, tableNameToUpdateWithType, BROKER_TENANT);

    String response =
        ControllerTest.sendPutRequest(updateLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
    assertEquals(response,
        "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTableName + " logical table successfully updated.\"}");

    // verify updated logical table
    verifyLogicalTableExists(getLogicalTableUrl, logicalTableConfig);

    // delete logical table
    String deleteResponse = ControllerTest.sendDeleteRequest(deleteLogicalTableUrl, getHeaders());
    assertEquals(deleteResponse, "{\"status\":\"" + logicalTableName + " logical table successfully deleted.\"}");

    // verify logical table is deleted
    verifyLogicalTableDoesNotExists(getLogicalTableUrl);
  }

  @Test(expectedExceptions = IOException.class,
      expectedExceptionsMessageRegExp = ".*Reason: 'quota.storage' should not be set for logical table.*")
  public void testLogicalTableQuotaConfigValidation()
      throws IOException {
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_1"));
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
    logicalTableConfig.setQuotaConfig(new QuotaConfig("10G", "999"));
    ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
  }

  @Test
  public void testLogicalTableReferenceTableValidation()
      throws IOException {
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_1"));

    // Test ref offline table name is null validation
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
    logicalTableConfig.setRefOfflineTableName(null);
    try {
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage()
              .contains("Reason: 'refOfflineTableName' should not be null or empty when offline table exists"),
          e.getMessage());
    }

    // Test ref realtime table name is null validation
    logicalTableConfig = getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
    logicalTableConfig.setRefRealtimeTableName(null);
    try {
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage()
              .contains("Reason: 'refRealtimeTableName' should not be null or empty when realtime table exists"),
          e.getMessage());
    }

    // Test ref offline table is present in the offline tables validation
    logicalTableConfig = getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
    logicalTableConfig.setRefOfflineTableName("random_table_OFFLINE");
    try {
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Reason: 'refOfflineTableName' should be one of the provided offline tables"),
          e.getMessage());
    }

    // Test ref realtime table is present in the realtime tables validation
    logicalTableConfig = getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
    logicalTableConfig.setRefRealtimeTableName("random_table_REALTIME");
    try {
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(
          e.getMessage().contains("Reason: 'refRealtimeTableName' should be one of the provided realtime tables"),
          e.getMessage());
    }
  }

  @Test(expectedExceptions = IOException.class,
      expectedExceptionsMessageRegExp = ".*Reason: 'InvalidTenant' should be one of the existing broker tenants.*")
  public void testLogicalTableBrokerTenantValidation()
      throws IOException {
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_3"));
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, "InvalidTenant");

    ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
  }

  @Test
  public void testLogicalTablePhysicalTableConfigValidation() {
    // Test empty physical table names is not allowed
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, List.of(), BROKER_TENANT);
    try {
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("'physicalTableConfigMap' should not be null or empty"), e.getMessage());
    }

    // Test all table names are physical table names and none is hybrid table name
    List<String> physicalTableNames = List.of("test_table_1");
    logicalTableConfig = getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNames, BROKER_TENANT);
    try {
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Reason: 'test_table_1' should be one of the existing tables"),
          e.getMessage());
    }
  }

  @Test(expectedExceptions = IOException.class,
      expectedExceptionsMessageRegExp = ".*Table name: test_table_1 already exists.*")
  public void testLogicalTableNameCannotSameAsPhysicalTableNameValidation()
      throws IOException {
    String tableName = "test_table_1";
    List<String> physicalTableNamesWithType = createHybridTables(List.of(tableName));
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(tableName, physicalTableNamesWithType, BROKER_TENANT);
    ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
  }

  @Test
  public void testLogicalTableNameSuffixValidation()
      throws IOException {
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_4"));

    // Test logical table name with _OFFLINE and _REALTIME is not allowed
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig("testLogicalTable_OFFLINE", physicalTableNamesWithType, BROKER_TENANT);
    try {
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Reason: 'tableName' should not end with _OFFLINE or _REALTIME"),
          e.getMessage());
    }

    logicalTableConfig =
        getDummyLogicalTableConfig("testLogicalTable_REALTIME", physicalTableNamesWithType, BROKER_TENANT);
    try {
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Reason: 'tableName' should not end with _OFFLINE or _REALTIME"),
          e.getMessage());
    }
  }

  @DataProvider
  public Object[][] tableTypeProvider() {
    return new Object[][]{
        {TableType.OFFLINE},
        {TableType.REALTIME}
    };
  }

  @Test(dataProvider = "tableTypeProvider")
  public void testCreateLogicalTable(TableType tableType)
      throws IOException {
    // Test logical table with only realtime table
    String tableName = "test_table";
    addDummySchema(tableName);
    TableConfig tableConfig = createDummyTableConfig(tableName, tableType);
    addTableConfig(tableConfig);
    String getLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableGet(LOGICAL_TABLE_NAME);
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, List.of(tableConfig.getTableName()), BROKER_TENANT);
    ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
    verifyLogicalTableExists(getLogicalTableUrl, logicalTableConfig);
  }

  @Test
  public void testLogicalTableWithSameNameNotAllowed()
      throws IOException {
    String getLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableGet(LOGICAL_TABLE_NAME);
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_5"));

    LogicalTableConfig
        logicalTableConfig = getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
    ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
    verifyLogicalTableExists(getLogicalTableUrl, logicalTableConfig);
    try {
      // create the same logical table again
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
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
        {LOGICAL_TABLE_NAME, List.of("test_table_2"), "unknown_table_REALTIME"},
        {LOGICAL_TABLE_NAME, List.of("test_table_1"), "db.test_table_1_OFFLINE"},
        {LOGICAL_TABLE_NAME, List.of("test_table_2"), "db.test_table_2_REALTIME"},
    };
  }

  @Test(dataProvider = "physicalTableShouldExistProvider")
  public void testPhysicalTableShouldExist(String logicalTableName, List<String> physicalTableNames,
      String unknownTableName)
      throws IOException {
    // setup physical tables
    List<String> physicalTableNamesWithType = createHybridTables(physicalTableNames);
    physicalTableNamesWithType.add(unknownTableName);

    // Test physical table should exist
    LogicalTableConfig
        logicalTableConfig = getDummyLogicalTableConfig(logicalTableName, physicalTableNamesWithType, BROKER_TENANT);
    try {
      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
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
    List<String> physicalTableNamesWithType = createHybridTables(physicalTableNames);

    for (int i = 0; i < logicalTableNames.size(); i++) {
      LogicalTableConfig logicalTableConfig = getDummyLogicalTableConfig(logicalTableNames.get(i), List.of(
          physicalTableNamesWithType.get(2 * i), physicalTableNamesWithType.get(2 * i + 1)), BROKER_TENANT);

      ControllerTest.sendPostRequest(_addLogicalTableUrl, logicalTableConfig.toSingleLineJsonString(), getHeaders());
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

  private void verifyLogicalTableExists(String getLogicalTableUrl, LogicalTableConfig logicalTableConfig)
      throws IOException {
    LogicalTableConfig remoteLogicalTableConfig =
        LogicalTableConfig.fromString(ControllerTest.sendGetRequest(getLogicalTableUrl, getHeaders()));
    assertEquals(remoteLogicalTableConfig, logicalTableConfig);
  }

  private void verifyLogicalTableDoesNotExists(String getLogicalTableUrl) {
    try {
      ControllerTest.sendGetRequest(getLogicalTableUrl, getHeaders());
      fail("Logical Table GET request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table not found"), e.getMessage());
    }
  }

  protected Map<String, String> getHeaders() {
    return Map.of();
  }
}
