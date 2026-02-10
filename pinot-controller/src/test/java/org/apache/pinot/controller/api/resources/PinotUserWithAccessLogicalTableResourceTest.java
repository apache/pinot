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
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotUserWithAccessLogicalTableResourceTest extends ControllerTest {

  public static final String AUTH_TOKEN = "Basic YWRtaW46dmVyeXNlY3JldA=====";
  public static final String AUTH_TOKEN_USER = "Basic dXNlcjpzZWNyZXQ==";
  public static final Map<String, String> AUTH_HEADER = Map.of("Authorization", AUTH_TOKEN);
  public static final Map<String, String> AUTH_HEADER_USER = Map.of("Authorization", AUTH_TOKEN_USER);
  public static final String LOGICAL_TABLE_NAME = "test_logical_table";

  private Map<String, Object> getControllerConf(Object permissions) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("controller.admin.access.control.factory.class",
        "org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory");
    properties.put("controller.admin.access.control.principals", "admin,user");
    properties.put("controller.admin.access.control.principals.admin.password", "verysecret");
    properties.put("controller.admin.access.control.principals.user.password", "secret");
    properties.put("controller.admin.access.control.principals.user.permissions", permissions);
    return properties;
  }

  protected Map<String, String> getHeaders() {
    return AUTH_HEADER_USER;
  }

  @Override
  protected Map<String, String> getControllerRequestClientHeaders() {
    return AUTH_HEADER;
  }

  private void setup(Map<String, Object> properties)
      throws Exception {
    startZk();
    Map<String, Object> configuration = getDefaultControllerConfiguration();
    configuration.putAll(properties);
    startController(configuration);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    // create schema for logical table
    addDummySchema(LOGICAL_TABLE_NAME);
  }

  @AfterMethod
  private void tearDown() {
    cleanup();
    try {
      deleteLogicalTable(LOGICAL_TABLE_NAME, AUTH_HEADER);
    } catch (Exception e) {
      // ignore
    }
    stopFakeInstances();
    stopController();
    stopZk();
  }

  @DataProvider
  public Object[][] permissionsProvider() {
    return new Object[][]{
        {"read,create"},
        {"read,create,update"},
        {"read,create,update,delete"}
    };
  }

  @Test(dataProvider = "permissionsProvider")
  public void testUserWithCreateAccess(String permissions)
      throws Exception {
    Map<String, Object> properties = getControllerConf(permissions);

    setup(properties);

    List<String> physicalTableNames = List.of("test_table_1");
    List<String> physicalTablesWithType = createHybridTables(physicalTableNames);
    LogicalTableConfig logicalTableConfig;

    // create logical table
    try {
      logicalTableConfig = getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTablesWithType, "DefaultTenant");
      String resp = createLogicalTable(logicalTableConfig, getHeaders());
      if (permissions.contains("create")) {
        assertEquals(resp,
            "{\"unrecognizedProperties\":{},\"status\":\"" + LOGICAL_TABLE_NAME
                + " logical table successfully added.\"}");
        verifyLogicalTableExists(LOGICAL_TABLE_NAME, logicalTableConfig);
      } else {
        fail("Logical Table POST request should have failed");
      }
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Permission is denied for CREATE"), e.getMessage());
    }

    // update logical table
    try {
      physicalTablesWithType.addAll(createHybridTables(List.of("test_table_2")));
      logicalTableConfig = getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTablesWithType, "DefaultTenant");
      String respUpdate = updateLogicalTable(logicalTableConfig, getHeaders());
      if (permissions.contains("update")) {
        assertEquals(respUpdate,
            "{\"unrecognizedProperties\":{},\"status\":\"" + LOGICAL_TABLE_NAME
                + " logical table successfully updated.\"}");
        verifyLogicalTableExists(LOGICAL_TABLE_NAME, logicalTableConfig);
      } else {
        fail("Logical Table POST request should have failed");
      }
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Permission is denied for UPDATE"), e.getMessage());
    }

    // delete logical table
    try {
      String respDelete = deleteLogicalTable(LOGICAL_TABLE_NAME, getHeaders());
      if (permissions.contains("delete")) {
        assertEquals(respDelete, "{\"status\":\"" + LOGICAL_TABLE_NAME + " logical table successfully deleted.\"}");
      } else {
        fail("Logical Table DELETE request should have failed");
      }
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Permission is denied for DELETE"), e.getMessage());
    }
  }

  private String createLogicalTable(LogicalTableConfig logicalTableConfig, Map<String, String> headers)
      throws IOException {
    return executeControllerRest("POST", "/logicalTables", logicalTableConfig.toSingleLineJsonString(), null, headers);
  }

  private String updateLogicalTable(LogicalTableConfig logicalTableConfig, Map<String, String> headers)
      throws IOException {
    return executeControllerRest("PUT", "/logicalTables/" + logicalTableConfig.getTableName(),
        logicalTableConfig.toSingleLineJsonString(), null, headers);
  }

  private String deleteLogicalTable(String logicalTableName, Map<String, String> headers)
      throws IOException {
    return executeControllerRest("DELETE", "/logicalTables/" + logicalTableName, null, null, headers);
  }

  private String getLogicalTable(String logicalTableName, Map<String, String> headers)
      throws IOException {
    return executeControllerRest("GET", "/logicalTables/" + logicalTableName, null, null, headers);
  }

  private void verifyLogicalTableExists(String logicalTableName, LogicalTableConfig logicalTableConfig)
      throws IOException {
    String respGet = getLogicalTable(logicalTableName, getHeaders());
    LogicalTableConfig remoteTable = LogicalTableConfig.fromString(respGet);
    assertEquals(remoteTable, logicalTableConfig);
  }
}
