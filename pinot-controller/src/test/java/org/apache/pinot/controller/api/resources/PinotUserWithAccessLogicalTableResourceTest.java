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
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.data.LogicalTable;
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
  public ControllerRequestClient getControllerRequestClient() {
    if (_controllerRequestClient == null) {
      _controllerRequestClient =
          new ControllerRequestClient(_controllerRequestURLBuilder, getHttpClient(), AUTH_HEADER);
    }
    return _controllerRequestClient;
  }

  private void setup(Map<String, Object> properties)
      throws Exception {
    startZk();
    Map<String, Object> configuration = getDefaultControllerConfiguration();
    configuration.putAll(properties);
    startController(configuration);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    _controllerRequestURLBuilder = getControllerRequestURLBuilder();
  }

  @AfterMethod
  private void tearDown() {
    cleanup();
    String deleteLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableDelete(LOGICAL_TABLE_NAME);
    try {
      ControllerTest.sendDeleteRequest(deleteLogicalTableUrl, AUTH_HEADER);
    } catch (Exception e) {
      // ignore
    }
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

    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    String getLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableGet(LOGICAL_TABLE_NAME);
    String updateLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableUpdate(LOGICAL_TABLE_NAME);
    String deleteLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableDelete(LOGICAL_TABLE_NAME);

    List<String> physicalTableNames = List.of("test_table_1");
    List<String> physicalTablesWithType = createHybridTables(physicalTableNames);
    LogicalTable logicalTable;

    // create logical table
    try {
      logicalTable = getDummyLogicalTable(LOGICAL_TABLE_NAME, physicalTablesWithType, "DefaultTenant");
      String resp =
          ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
      if (permissions.contains("create")) {
        assertEquals(resp,
            "{\"unrecognizedProperties\":{},\"status\":\"" + LOGICAL_TABLE_NAME
                + " logical table successfully added.\"}");
        verifyLogicalTableExists(getLogicalTableUrl, logicalTable);
      } else {
        fail("Logical Table POST request should have failed");
      }
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Permission is denied for CREATE"), e.getMessage());
    }

    // update logical table
    try {
      physicalTablesWithType.addAll(createHybridTables(List.of("test_table_2")));
      logicalTable = getDummyLogicalTable(LOGICAL_TABLE_NAME, physicalTablesWithType, "DefaultTenant");
      String respUpdate = ControllerTest.sendPutRequest(updateLogicalTableUrl, logicalTable.toSingleLineJsonString(),
          getHeaders());
      if (permissions.contains("update")) {
        assertEquals(respUpdate,
            "{\"unrecognizedProperties\":{},\"status\":\"" + LOGICAL_TABLE_NAME
                + " logical table successfully updated.\"}");
        verifyLogicalTableExists(getLogicalTableUrl, logicalTable);
      } else {
        fail("Logical Table POST request should have failed");
      }
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Permission is denied for UPDATE"), e.getMessage());
    }

    // delete logical table
    try {
      String respDelete = ControllerTest.sendDeleteRequest(deleteLogicalTableUrl, getHeaders());
      if (permissions.contains("delete")) {
        assertEquals(respDelete, "{\"status\":\"" + LOGICAL_TABLE_NAME + " logical table successfully deleted.\"}");
      } else {
        fail("Logical Table DELETE request should have failed");
      }
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Permission is denied for DELETE"), e.getMessage());
    }
  }

  private void verifyLogicalTableExists(String logicalTableNamesGet, LogicalTable logicalTable)
      throws IOException {
    String respGet = ControllerTest.sendGetRequest(logicalTableNamesGet, getHeaders());
    LogicalTable remoteTable = LogicalTable.fromString(respGet);
    assertEquals(remoteTable, logicalTable);
  }
}
