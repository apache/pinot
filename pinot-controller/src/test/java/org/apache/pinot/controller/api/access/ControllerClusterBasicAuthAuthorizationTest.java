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
package org.apache.pinot.controller.api.access;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Exercises controller HTTP authorization for cluster-scoped requests with both BasicAuth implementations.
public class ControllerClusterBasicAuthAuthorizationTest extends ControllerTest {
  private static final String ADMIN_USER = "clusterAdmin";
  private static final String ADMIN_PASSWORD = "clusterAdminPassword";
  private static final String RESTRICTED_USER = "clusterReader";
  private static final String RESTRICTED_PASSWORD = "clusterReaderPassword";
  private static final String CREATE_USER = "clusterCreator";
  private static final String CREATE_PASSWORD = "clusterCreatorPassword";
  private static final String UPDATE_USER = "clusterUpdater";
  private static final String UPDATE_PASSWORD = "clusterUpdaterPassword";
  private static final String DELETE_USER = "clusterDeleter";
  private static final String DELETE_PASSWORD = "clusterDeleterPassword";
  private static final String TABLE_UPDATE_USER = "tableUpdater";
  private static final String TABLE_UPDATE_PASSWORD = "tableUpdaterPassword";
  private static final String ALLOWED_TABLE = "allowedTable";
  private static final String DISALLOWED_TABLE = "disallowedTable";

  @DataProvider(name = "accessControlFactories")
  public Object[][] accessControlFactories() {
    return new Object[][]{
        {BasicAuthAccessControlFactory.class.getName(), false, "static"},
        {ZkBasicAuthAccessControlFactory.class.getName(), true, "zk"}
    };
  }

  @Test(dataProvider = "accessControlFactories")
  public void testClusterAuthorization(String factoryClass, boolean zkBacked, String configKeySuffix)
      throws Exception {
    boolean controllerStarted = false;
    startZk();
    try {
      Map<String, Object> controllerConfiguration = getDefaultControllerConfiguration();
      controllerConfiguration.put(ControllerConf.ACCESS_CONTROL_FACTORY_CLASS, factoryClass);
      configurePrincipals(controllerConfiguration, zkBacked);
      startController(controllerConfiguration);
      controllerStarted = true;

      Map<String, String> adminHeaders = authHeaders(ADMIN_USER, ADMIN_PASSWORD);
      Map<String, String> restrictedHeaders = authHeaders(RESTRICTED_USER, RESTRICTED_PASSWORD);
      Map<String, String> createHeaders = authHeaders(CREATE_USER, CREATE_PASSWORD);
      Map<String, String> updateHeaders = authHeaders(UPDATE_USER, UPDATE_PASSWORD);
      Map<String, String> deleteHeaders = authHeaders(DELETE_USER, DELETE_PASSWORD);
      Map<String, String> tableUpdateHeaders = authHeaders(TABLE_UPDATE_USER, TABLE_UPDATE_PASSWORD);
      String clusterConfigsUrl = _controllerBaseApiUrl + "/cluster/configs";
      if (zkBacked) {
        addZkUsers();
        TestUtils.waitForCondition(aVoid -> getStatus(clusterConfigsUrl, restrictedHeaders) == 200
                && getStatus(clusterConfigsUrl, createHeaders) == 403
                && getStatus(clusterConfigsUrl, updateHeaders) == 403
                && getStatus(clusterConfigsUrl, deleteHeaders) == 403
                && getStatus(clusterConfigsUrl, tableUpdateHeaders) == 403, TIMEOUT_MS,
            "Controller users were not loaded from ZooKeeper");
      }

      assertGetStatus(clusterConfigsUrl, authHeaders("unknown", "incorrect"), 401);
      assertGetStatus(clusterConfigsUrl, restrictedHeaders, 200);
      assertTrue(JsonUtils.stringToJsonNode(
          sendPostRequest(_controllerBaseApiUrl + "/query/tableNames", "[]", restrictedHeaders)).isEmpty());
      assertTrue(JsonUtils.stringToJsonNode(
          sendPostRequest(_controllerBaseApiUrl + "/instances/updateTags/validate", "[]", restrictedHeaders))
          .isEmpty());

      assertTableScope(restrictedHeaders, tableUpdateHeaders, authHeaders("unknown", "incorrect"));

      String configKey = "pinot.controller.auth.test." + configKeySuffix;
      String zkPath = "/controller-auth-test-" + configKeySuffix;
      String zkCreateUrl = _controllerBaseApiUrl + "/zk/create?path=" + zkPath;
      String zkDeleteUrl = _controllerBaseApiUrl + "/zk/delete?path=" + zkPath;
      assertHttpError(403, () -> sendPostRequest(zkCreateUrl, "{not-json", restrictedHeaders));
      assertHttpError(403, () -> sendPostRequest(clusterConfigsUrl, "{not-json", restrictedHeaders));
      assertHttpError(403, () -> sendDeleteRequest(clusterConfigsUrl + "/" + configKey, restrictedHeaders));

      assertHttpError(403, () -> sendPostRequest(zkCreateUrl, "{not-json", updateHeaders));
      assertHttpError(403, () -> sendPostRequest(clusterConfigsUrl, "{not-json", createHeaders));
      assertHttpError(403, () -> sendDeleteRequest(clusterConfigsUrl + "/" + configKey, updateHeaders));

      boolean zkCleanupRequired = false;
      try {
        sendPostRequest(zkCreateUrl, JsonUtils.objectToString(new ZNRecord("controllerAuthTest")), createHeaders);
        zkCleanupRequired = true;
        sendDeleteRequest(zkDeleteUrl, deleteHeaders);
        zkCleanupRequired = false;
      } finally {
        if (zkCleanupRequired) {
          sendDeleteRequest(zkDeleteUrl, adminHeaders);
        }
      }

      String configValue = "authorized";
      boolean cleanupRequired = false;
      try {
        sendPostRequest(clusterConfigsUrl, JsonUtils.objectToString(Map.of(configKey, configValue)), updateHeaders);
        cleanupRequired = true;
        assertEquals(
            JsonUtils.stringToJsonNode(sendGetRequest(clusterConfigsUrl, adminHeaders)).get(configKey).asText(),
            configValue);

        sendDeleteRequest(clusterConfigsUrl + "/" + configKey, deleteHeaders);
        cleanupRequired = false;
        assertFalse(JsonUtils.stringToJsonNode(sendGetRequest(clusterConfigsUrl, adminHeaders)).has(configKey));
      } finally {
        if (cleanupRequired) {
          sendDeleteRequest(clusterConfigsUrl + "/" + configKey, adminHeaders);
        }
      }
    } finally {
      if (controllerStarted) {
        stopController();
      }
      stopZk();
    }
  }

  private static void configurePrincipals(Map<String, Object> controllerConfiguration, boolean zkBacked) {
    if (zkBacked) {
      controllerConfiguration.put(ControllerConf.ACCESS_CONTROL_USERNAME, ADMIN_USER);
      controllerConfiguration.put(ControllerConf.ACCESS_CONTROL_PASSWORD, ADMIN_PASSWORD);
      return;
    }

    controllerConfiguration.put("controller.admin.access.control.principals",
        String.join(",", ADMIN_USER, RESTRICTED_USER, CREATE_USER, UPDATE_USER, DELETE_USER, TABLE_UPDATE_USER));
    controllerConfiguration.put("controller.admin.access.control.principals." + ADMIN_USER + ".password",
        ADMIN_PASSWORD);
    controllerConfiguration.put("controller.admin.access.control.principals." + RESTRICTED_USER + ".password",
        RESTRICTED_PASSWORD);
    controllerConfiguration.put("controller.admin.access.control.principals." + RESTRICTED_USER + ".tables",
        ALLOWED_TABLE);
    controllerConfiguration.put("controller.admin.access.control.principals." + RESTRICTED_USER + ".permissions",
        "read");
    addStaticPrincipal(controllerConfiguration, CREATE_USER, CREATE_PASSWORD, "create");
    addStaticPrincipal(controllerConfiguration, UPDATE_USER, UPDATE_PASSWORD, "update");
    addStaticPrincipal(controllerConfiguration, DELETE_USER, DELETE_PASSWORD, "delete");
    addStaticPrincipal(controllerConfiguration, TABLE_UPDATE_USER, TABLE_UPDATE_PASSWORD, "update");
    controllerConfiguration.put("controller.admin.access.control.principals." + TABLE_UPDATE_USER + ".tables",
        ALLOWED_TABLE);
  }

  private static void addStaticPrincipal(Map<String, Object> controllerConfiguration, String username,
      String password, String permission) {
    controllerConfiguration.put("controller.admin.access.control.principals." + username + ".password", password);
    controllerConfiguration.put("controller.admin.access.control.principals." + username + ".permissions",
        permission);
  }

  private void addZkUsers()
      throws IOException {
    addZkUser(RESTRICTED_USER, RESTRICTED_PASSWORD, List.of(ALLOWED_TABLE),
        org.apache.pinot.spi.config.user.AccessType.READ);
    addZkUser(CREATE_USER, CREATE_PASSWORD, null, org.apache.pinot.spi.config.user.AccessType.CREATE);
    addZkUser(UPDATE_USER, UPDATE_PASSWORD, null, org.apache.pinot.spi.config.user.AccessType.UPDATE);
    addZkUser(DELETE_USER, DELETE_PASSWORD, null, org.apache.pinot.spi.config.user.AccessType.DELETE);
    addZkUser(TABLE_UPDATE_USER, TABLE_UPDATE_PASSWORD, List.of(ALLOWED_TABLE),
        org.apache.pinot.spi.config.user.AccessType.UPDATE);
  }

  private void assertTableScope(Map<String, String> readHeaders, Map<String, String> updateHeaders,
      Map<String, String> invalidHeaders)
      throws IOException {
    String tableConfigUrl = _controllerBaseApiUrl + "/tables/";
    assertGetStatus(tableConfigUrl + ALLOWED_TABLE, invalidHeaders, 401);
    assertGetStatus(tableConfigUrl + ALLOWED_TABLE, readHeaders, 404);
    assertGetStatus(tableConfigUrl + DISALLOWED_TABLE, readHeaders, 403);

    String periodicTaskUrl = _controllerBaseApiUrl + "/periodictask/run?taskname=missing&tableName=";
    assertGetStatus(periodicTaskUrl + ALLOWED_TABLE, updateHeaders, 404);
    assertGetStatus(periodicTaskUrl + DISALLOWED_TABLE, updateHeaders, 403);
  }

  private void addZkUser(String username, String password, List<String> tables,
      org.apache.pinot.spi.config.user.AccessType permission)
      throws IOException {
    _helixResourceManager.addUser(new UserConfig(username, password, ComponentType.CONTROLLER.name(),
        RoleType.USER.name(), tables, null, List.of(permission)));
  }

  private static Map<String, String> authHeaders(String username, String password) {
    return Map.of(HttpHeaders.AUTHORIZATION, BasicAuthTokenUtils.toBasicAuthToken(username, password));
  }

  private static int getStatus(String url, Map<String, String> headers) {
    try {
      return sendGetRequestWithStatusCode(url, headers).getLeft();
    } catch (IOException e) {
      return -1;
    }
  }

  private static void assertGetStatus(String url, Map<String, String> headers, int expectedStatus)
      throws IOException {
    Pair<Integer, String> response = sendGetRequestWithStatusCode(url, headers);
    assertEquals(response.getLeft().intValue(), expectedStatus);
  }

  private static void assertHttpError(int expectedStatus, IoRequest request) {
    try {
      request.send();
      fail("Expected HTTP status " + expectedStatus);
    } catch (IOException e) {
      assertTrue(e.getCause() instanceof HttpErrorStatusException);
      assertEquals(((HttpErrorStatusException) e.getCause()).getStatusCode(), expectedStatus);
    }
  }

  @FunctionalInterface
  private interface IoRequest {
    void send()
        throws IOException;
  }
}
