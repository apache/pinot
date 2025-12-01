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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Row Level Security integration test using ZkAuth (ZooKeeper-based authentication).
 * Users and permissions are created dynamically via REST API calls to the controller.
 * This approach allows for dynamic user management without requiring cluster restarts.
 */
public class RowLevelSecurityZkAuthIntegrationTest extends RowLevelSecurityIntegrationTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(RowLevelSecurityZkAuthIntegrationTest.class);

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put("controller.segment.fetcher.auth.token", AUTH_TOKEN);
    properties.put("controller.admin.access.control.factory.class",
        "org.apache.pinot.controller.api.access.ZkBasicAuthAccessControlFactory");
    properties.put("access.control.init.username", ADMIN_USER);
    properties.put("access.control.init.password", ADMIN_PASSWORD);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty("pinot.broker.enable.row.column.level.auth", "true");
    brokerConf.setProperty("pinot.broker.access.control.class",
        "org.apache.pinot.broker.broker.ZkBasicAuthAccessControlFactory");
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty("pinot.server.segment.fetcher.auth.token", AUTH_TOKEN);
    serverConf.setProperty("pinot.server.segment.uploader.auth.token", AUTH_TOKEN);
    serverConf.setProperty("pinot.server.instance.auth.token", AUTH_TOKEN);
  }

  /**
   * Initialize users via REST API after the cluster has started.
   * This creates the same users with the same permissions and RLS filters as BasicAuth test.
   */
  @Override
  protected void initializeUsers()
      throws Exception {
    LOGGER.info("Initializing users via REST API for ZkAuth test");

    // Wait for controller to be ready
    waitForControllerReady();

    // Create users for BROKER component with RLS filters
    createBrokerUsers();

    // Create users for CONTROLLER component
    createControllerUsers();

    // Create users for SERVER component
    createServerUsers();

    LOGGER.info("Successfully initialized all users for ZkAuth test");
  }

  private void waitForControllerReady()
      throws Exception {
    int maxRetries = 30;
    int retryCount = 0;
    String healthUrl = "http://localhost:" + getControllerPort() + "/health";
    while (retryCount < maxRetries) {
      try {
        String response = sendGetRequest(healthUrl);
        if (response != null && !response.isEmpty()) {
          LOGGER.info("Controller is ready");
          return;
        }
      } catch (Exception e) {
        // Controller not ready yet
      }
      retryCount++;
      Thread.sleep(2000);
    }
    throw new Exception("Controller failed to become ready after " + maxRetries + " attempts");
  }

  private void createBrokerUsers()
      throws Exception {
    LOGGER.info("Creating BROKER users");

    // User with READ permission on mytable, mytable2, mytable3 with RLS filter on mytable
    createUser("user", "secret", "BROKER", "USER",
        new String[]{"mytable", "mytable2", "mytable3"},
        new String[]{"READ"},
        null,
        Map.of("mytable", new String[]{"AirlineID='19805'"},
            "mytable3", new String[]{"AirlineID='20409' OR AirTime>'300'", "DestStateName='Florida'"}));

    // User2 with READ permission on mytable, mytable2, mytable3 with RLS filters
    createUser("user2", "notSoSecret", "BROKER", "USER",
        new String[]{"mytable", "mytable2", "mytable3"},
        new String[]{"READ"},
        null,
        Map.of("mytable", new String[]{"AirlineID='19805'", "DestStateName='California'"},
            "mytable2", new String[]{"AirlineID='20409'", "DestStateName='Florida'"},
            "mytable3", new String[]{"AirlineID='20409' OR DestStateName='California'", "DestStateName='Florida'"}));
  }

  private void createControllerUsers()
      throws Exception {
    LOGGER.info("Creating CONTROLLER users");

    // User with READ permission on mytable, mytable2, mytable3
    createUser("user", "secret", "CONTROLLER", "USER",
        new String[]{"mytable", "mytable2", "mytable3"},
        new String[]{"READ"},
        null, null);

    // User2 with READ permission on mytable, mytable2, mytable3
    createUser("user2", "notSoSecret", "CONTROLLER", "USER",
        new String[]{"mytable", "mytable2", "mytable3"},
        new String[]{"READ"},
        null, null);
  }

  private void createServerUsers() {
    // Nothing to do for SERVER users in this test
    LOGGER.info("No SERVER users to create for this test");
  }

  /**
   * Create a user via REST API call to /users endpoint.
   *
   * @param username Username
   * @param password Password
   * @param component Component (BROKER, CONTROLLER, SERVER)
   * @param role Role (ADMIN, USER)
   * @param tables Tables the user has access to (null for all tables)
   * @param permissions Permissions (READ, CREATE, UPDATE, DELETE)
   * @param excludeTables Tables to exclude (null if not applicable)
   * @param rlsFilters Row-level security filters per table (null if not applicable)
   */
  private void createUser(String username, String password, String component, String role,
      String[] tables, String[] permissions, String[] excludeTables, Map<String, String[]> rlsFilters)
      throws Exception {

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode userJson = mapper.createObjectNode();

    userJson.put("username", username);
    userJson.put("password", password);
    userJson.put("component", component);
    userJson.put("role", role);

    if (tables != null) {
      userJson.set("tables", mapper.valueToTree(tables));
    }

    if (permissions != null) {
      userJson.set("permissions", mapper.valueToTree(permissions));
    }

    if (excludeTables != null) {
      userJson.set("excludeTables", mapper.valueToTree(excludeTables));
    }

    if (rlsFilters != null) {
      ObjectNode rlsNode = mapper.createObjectNode();
      for (Map.Entry<String, String[]> entry : rlsFilters.entrySet()) {
        rlsNode.set(entry.getKey(), mapper.valueToTree(entry.getValue()));
      }
      userJson.set("rlsFilters", rlsNode);
    }

    String jsonPayload = userJson.toString();

    try {
      String usersUrl = "http://localhost:" + getControllerPort() + "/users";
      String response = sendPostRequest(usersUrl, jsonPayload, AUTH_HEADER);

      LOGGER.info("Created user: {}_{} - Response: {}", username, component, response);
    } catch (Exception e) {
      // Check if user already exists (409 conflict)
      if (e.getMessage() != null && e.getMessage().contains("409")) {
        LOGGER.info("User {}_{} already exists, skipping", username, component);
      } else {
        throw e;
      }
    }
  }
}
