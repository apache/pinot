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
import java.util.List;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTable;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotLogicalTableResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final List<String> PHYSICAL_TABLE_NAMES = List.of("test_table_1", "test_table_2", "test_table_3");
  private static final List<String> PHYSICAL_TABLE_NAMES_WITH_TYPE = List.of(
      "test_table_1_OFFLINE",
      "test_table_2_OFFLINE"
  );
  private static final String LOGICAL_TABLE_STRING = "{"
          + "  \"tableName\" : \"test_logical_table\","
          + "  \"physicalTableNames\" : [ "
          + "    \"test_table_1_OFFLINE\","
          + "    \"test_table_2_OFFLINE\"]}";

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
    for (String physicalTable : PHYSICAL_TABLE_NAMES) {
      TEST_INSTANCE.addDummySchema(physicalTable);
      TEST_INSTANCE.addTableConfig(
          new TableConfigBuilder(TableType.OFFLINE)
              .setTableName(physicalTable)
              .build()
      );
    }
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }

  @Test
  public void testCreateLogicalTableUsingJson()
      throws IOException {

    // Verify that the logical table is not created
    String logicalTableNamesGet = TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableNamesGet();
    String tableNames = ControllerTest.sendGetRequest(logicalTableNamesGet);
    assertEquals(tableNames, List.of().toString());

    try {
      final String response =
          ControllerTest.sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableCreate(),
              LOGICAL_TABLE_STRING);
      assertEquals(response,
          "{\"unrecognizedProperties\":{},\"status\":\"test_logical_table logical table successfully added.\"}");
    } catch (IOException e) {
      fail("Shouldn't have caught an exception: " + e.getMessage());
    }

    // Verify all logical tables names
    tableNames = ControllerTest.sendGetRequest(logicalTableNamesGet);
    assertEquals(tableNames, "[\"test_logical_table\"]");
  }

  @Test
  public void testCreateUpdateLogicalTable()
      throws IOException {
    LogicalTable logicalTable = getLogicalTable("testLogicalTable", "DefaultTenant",
        PHYSICAL_TABLE_NAMES_WITH_TYPE);

    // Add the logical table
    String addLogicalTableUrl = TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableCreate();
    String resp = ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString());
    assertEquals(resp,
        "{\"unrecognizedProperties\":{},\"status\":\"testLogicalTable logical table successfully added.\"}");

    // Retry creating the same logical table
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table: testLogicalTable already exists"));
    }

    // Get the logical table and verify table config
    String getLogicalTableUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableGet(logicalTable.getTableName());
    LogicalTable remoteLogicalTable = LogicalTable.fromString(ControllerTest.sendGetRequest(getLogicalTableUrl));
    assertEquals(remoteLogicalTable, logicalTable);

    // Update physical table names and verify
    logicalTable.setPhysicalTableNames(List.of(
        "test_table_1_OFFLINE",
        "test_table_2_OFFLINE",
        "test_table_3_OFFLINE"
    ));
    String updateLogicalTableUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableUpdate(logicalTable.getTableName());
    String response = ControllerTest.sendPutRequest(updateLogicalTableUrl, logicalTable.toSingleLineJsonString());
    assertEquals(response,
        "{\"unrecognizedProperties\":{},\"status\":\"testLogicalTable logical table successfully updated.\"}");

    // Get the logical table and verify updated table config
    remoteLogicalTable = LogicalTable.fromString(ControllerTest.sendGetRequest(getLogicalTableUrl));
    assertEquals(remoteLogicalTable, logicalTable);

    // Update logical table with invalid physical table names and verify
    logicalTable.setPhysicalTableNames(List.of(
        "test_table_1_OFFLINE",
        "test_table_2_OFFLINE",
        "test_table_3_REALTIME"
    ));
    try {
      ControllerTest.sendPutRequest(updateLogicalTableUrl, logicalTable.toSingleLineJsonString());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("'test_table_3_REALTIME' should be one of the existing tables"));
    }
  }

  @Test
  public void testCreateDeleteLogicalTable()
      throws IOException {
    LogicalTable logicalTable = getLogicalTable("testDeleteTable", "DefaultTenant",
        PHYSICAL_TABLE_NAMES_WITH_TYPE);

    // Add the logical table
    String addLogicalTableUrl = TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableCreate();
    String resp = ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString());
    assertEquals(resp,
        "{\"unrecognizedProperties\":{},\"status\":\"testDeleteTable logical table successfully added.\"}");

    // Delete the logical table
    String deleteLogicalTableUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableDelete(logicalTable.getTableName());
    String response = ControllerTest.sendDeleteRequest(deleteLogicalTableUrl);
    assertEquals(response, "{\"status\":\"testDeleteTable logical table successfully deleted.\"}");

    // Verify that the logical table is deleted
    String getLogicalTableUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableGet(logicalTable.getTableName());
    try {
      ControllerTest.sendGetRequest(getLogicalTableUrl);
      fail("Logical Table GET request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table not found"));
    }
  }

  @Test
  public void testCreateTableWithoutPhysicalTables() {
    LogicalTable logicalTable =
        getLogicalTable("testLogicalTableWithoutPhysicalTables", "DefaultTenant_BROKER", List.of());

    // Add the logical table
    String addLogicalTableUrl = TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableCreate();
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("'physicalTableNames' should not be null or empty"));
    }
  }

  private static LogicalTable getLogicalTable(String tableName, String brokerTenant,
      List<String> physicalTableNames) {
    LogicalTable logicalTable = new LogicalTable();
    logicalTable.setTableName(tableName);
    logicalTable.setBrokerTenant(brokerTenant);
    logicalTable.setPhysicalTableNames(physicalTableNames);
    return logicalTable;
  }
}
