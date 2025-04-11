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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTable;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotLogicalTableResourceTest extends ControllerTest {

  private static final String LOGICAL_TABLE_NAME = "test_logical_table";
  public static final String BROKER_TENANT = "DefaultTenant";
  private static final List<String> PHYSICAL_TABLE_NAMES = List.of("test_table_1", "test_table_2");
  private static final List<String> PHYSICAL_TABLE_NAMES_WITH_TYPE = List.of(
      "test_table_1_OFFLINE",
      "test_table_1_REALTIME",
      "test_table_2_OFFLINE"
  );
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

  @BeforeMethod
  public void setUp()
      throws Exception {
    for (String physicalTable : PHYSICAL_TABLE_NAMES) {
      addDummySchema(physicalTable);
      addTableConfig(getOfflineTable(physicalTable));
      addTableConfig(getRealtimeTable(physicalTable));
    }
  }

  @AfterMethod
  public void tearDown() {
    cleanup();
    String deleteLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableDelete(LOGICAL_TABLE_NAME);
    try {
      ControllerTest.sendDeleteRequest(deleteLogicalTableUrl, getHeaders());
    } catch (IOException e) {
      // Ignore exception
    }
  }

  @Test
  public void testCreateUpdateLogicalTable()
      throws IOException {
    LogicalTable logicalTable = getLogicalTable(LOGICAL_TABLE_NAME, BROKER_TENANT, PHYSICAL_TABLE_NAMES_WITH_TYPE);

    // Add the logical table
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    String resp =
        ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
    assertEquals(resp,
        "{\"unrecognizedProperties\":{},\"status\":\"test_logical_table logical table successfully added.\"}");

    // Retry creating the same logical table
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table: test_logical_table already exists"));
    }

    // Get the logical table and verify table config
    String getLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableGet(logicalTable.getTableName());
    LogicalTable remoteLogicalTable =
        LogicalTable.fromString(ControllerTest.sendGetRequest(getLogicalTableUrl, getHeaders()));
    assertEquals(remoteLogicalTable, logicalTable);

    // Update physical table names and verify
    logicalTable.setPhysicalTableNames(List.of(
        "test_table_1_OFFLINE",
        "test_table_1_REALTIME",
        "test_table_2_OFFLINE",
        "test_table_2_REALTIME"
    ));
    String updateLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableUpdate(logicalTable.getTableName());
    String response =
        ControllerTest.sendPutRequest(updateLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
    assertEquals(response,
        "{\"unrecognizedProperties\":{},\"status\":\"test_logical_table logical table successfully updated.\"}");

    // Get the logical table and verify updated table config
    remoteLogicalTable = LogicalTable.fromString(ControllerTest.sendGetRequest(getLogicalTableUrl, getHeaders()));
    assertEquals(remoteLogicalTable, logicalTable);

    // Update logical table with invalid physical table names and verify
    logicalTable.setPhysicalTableNames(List.of(
        "test_table_1_OFFLINE",
        "test_table_2_OFFLINE",
        "test_invalid_table_REALTIME"
    ));
    try {
      ControllerTest.sendPutRequest(updateLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("'test_invalid_table_REALTIME' should be one of the existing tables"));
    }
  }

  @Test
  public void testCreateDeleteLogicalTable()
      throws IOException {
    LogicalTable logicalTable = getLogicalTable(LOGICAL_TABLE_NAME, BROKER_TENANT, PHYSICAL_TABLE_NAMES_WITH_TYPE);

    // Add the logical table
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    String resp =
        ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
    assertEquals(resp,
        "{\"unrecognizedProperties\":{},\"status\":\"test_logical_table logical table successfully added.\"}");

    // Delete the logical table
    String deleteLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableDelete(logicalTable.getTableName());
    String response = ControllerTest.sendDeleteRequest(deleteLogicalTableUrl, getHeaders());
    assertEquals(response, "{\"status\":\"test_logical_table logical table successfully deleted.\"}");

    // Verify that the logical table is deleted
    String getLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableGet(logicalTable.getTableName());
    try {
      ControllerTest.sendGetRequest(getLogicalTableUrl, getHeaders());
      fail("Logical Table GET request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table not found"));
    }
  }

  @Test
  public void testLogicalTableValidationTests() {
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    String getLogicalTableNamesUrl = _controllerRequestURLBuilder.forLogicalTableNamesGet();

    // Test logical table name with _OFFLINE and _REALTIME is not allowed
    LogicalTable logicalTable =
        getLogicalTable("testLogicalTable_OFFLINE", BROKER_TENANT, PHYSICAL_TABLE_NAMES_WITH_TYPE);
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Reason: 'tableName' should not end with _OFFLINE or _REALTIME"));
    }

    // Test logical table name can not be same as existing physical table name
    logicalTable =
        getLogicalTable("test_table_1", BROKER_TENANT, PHYSICAL_TABLE_NAMES_WITH_TYPE);
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Table name: test_table_1 already exists"));
    }

    // Test empty physical table names is not allowed
    logicalTable =
        getLogicalTable(LOGICAL_TABLE_NAME, BROKER_TENANT, List.of());
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("'physicalTableNames' should not be null or empty"));
    }

    // Test cannot create logical table with same name twice
    logicalTable = getLogicalTable(LOGICAL_TABLE_NAME, BROKER_TENANT, PHYSICAL_TABLE_NAMES_WITH_TYPE);
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      String tableNames = ControllerTest.sendGetRequest(getLogicalTableNamesUrl, getHeaders());
      assertEquals(tableNames, "[\"test_logical_table\"]");
      // create the same logical table again
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table: test_logical_table already exists"));
    }
  }

  @Test
  public void testLogicalTableFromDiffDatabases()
      throws IOException {
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    LogicalTable logicalTable = new LogicalTable();
    logicalTable.setTableName(LOGICAL_TABLE_NAME);
    logicalTable.setBrokerTenant(BROKER_TENANT);

    // Verify physical table from unknown database fails
    logicalTable.setPhysicalTableNames(List.of(
        "testDB.test_table_3_OFFLINE",
        "testDB.test_table_3_REALTIME",
        "test_table_1_OFFLINE",
        "test_table_1_REALTIME")
    );
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("'testDB.test_table_3_OFFLINE' should be one of the existing tables"));
    }

    // create physical tables in testDB database
    Map<String, String> testDBHeaders = new HashMap<>(getHeaders());
    testDBHeaders.put(CommonConstants.DATABASE, "testDB");
    ControllerRequestClient controllerRequestClient = new ControllerRequestClient(
        _controllerRequestURLBuilder,
        HttpClient.getInstance(),
        testDBHeaders
    );
    controllerRequestClient.addSchema(ControllerTest.createDummySchema("test_table_3"));
    controllerRequestClient.addTableConfig(getOfflineTable("test_table_3"));
    controllerRequestClient.addTableConfig(getRealtimeTable("test_table_3"));

    // Verify physical table from unknown database passes
    logicalTable.setPhysicalTableNames(
        List.of("testDB.test_table_3_OFFLINE", "testDB.test_table_3_REALTIME", "test_table_1_OFFLINE",
            "test_table_1_REALTIME")
    );
    String resp =
        ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
    assertEquals(resp,
        "{\"unrecognizedProperties\":{},\"status\":\"test_logical_table logical table successfully added.\"}");
  }

  protected Map<String, String> getHeaders() {
    return Map.of();
  }

  private static LogicalTable getLogicalTable(String tableName, String brokerTenant,
      List<String> physicalTableNames) {
    LogicalTable logicalTable = new LogicalTable();
    logicalTable.setTableName(tableName);
    logicalTable.setBrokerTenant(brokerTenant);
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
