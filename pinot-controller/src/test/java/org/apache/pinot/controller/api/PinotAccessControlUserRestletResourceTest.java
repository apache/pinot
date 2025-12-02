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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.UserConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotAccessControlUserRestletResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();

  private String _createUserUrl;
  private final UserConfigBuilder _userConfigBuilder = new UserConfigBuilder();

  @BeforeClass
  public void setup()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();

    _createUserUrl = TEST_INSTANCE.getControllerRequestURLBuilder().forUserCreate();
    _userConfigBuilder.setUsername("testUser").setPassword("123456").setComponentType(ComponentType.CONTROLLER)
        .setRoleType(RoleType.USER);
  }

  @Test
  public void testAddUser()
      throws Exception {
    String userConfigString = _userConfigBuilder.setUsername("bad.user.with.dot").build().toJsonString();
    try {
      ControllerTest.sendPostRequest(_createUserUrl, userConfigString);
      fail("Adding a user with dot in username does not fail");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Username: bad.user.with.dot containing '.' or space is not allowed"));
    }

    // Creating a user with a valid username should succeed
    userConfigString = _userConfigBuilder.setUsername("valid_table_name").build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);

    // Create a user that already exists should fail
    try {
      ControllerTest.sendPostRequest(_createUserUrl, userConfigString);
      fail("Creation of an existing user does not fail");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("User valid_table_name_CONTROLLER already exists"));
    }
  }

  private UserConfig getUserConfig(String username, String componentType)
      throws Exception {
    String userConfigString = ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forUserGet(username, componentType));
    String usernameWithType = username + "_" + componentType;
    return JsonUtils.jsonNodeToObject(JsonUtils.stringToJsonNode(userConfigString).get(usernameWithType),
        UserConfig.class);
  }

  @Test
  public void testUpdateUserConfig()
      throws Exception {
    String username = "updateTC";
    String userConfigString =
        _userConfigBuilder.setUsername(username).setComponentType(ComponentType.CONTROLLER).build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);
    // user creation should succeed
    UserConfig userConfig = getUserConfig(username, "CONTROLLER");
    assertEquals(userConfig.getRoleType().toString(), RoleType.USER.toString());
    assertTrue(BcryptUtils.checkpw("123456", userConfig.getPassword()));
    userConfig.setRole("ADMIN");
    userConfig.setPassword("654321");

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(ControllerTest.sendPutRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forUpdateUserConfig(username, "CONTROLLER", true),
        userConfig.toString()));
    assertTrue(jsonResponse.has("status"));

    UserConfig modifiedConfig = getUserConfig(username, "CONTROLLER");
    assertEquals(modifiedConfig.getRoleType().toString(), "ADMIN");
    assertTrue(BcryptUtils.checkpw("654321", modifiedConfig.getPassword()));
  }

  @Test
  public void testDeleteUser()
      throws Exception {
    // Case 1: Create a CONTORLLER user and delete it directly w/o using query param.
    UserConfig controllerUserConfig =
        _userConfigBuilder.setUsername("user1").setComponentType(ComponentType.CONTROLLER).build();
    String creationResponse = ControllerTest.sendPostRequest(_createUserUrl, controllerUserConfig.toJsonString());
    assertEquals(creationResponse, "{\"status\":\"User user1_CONTROLLER has been successfully added!\"}");

    // Delete controller user using CONTROLLER suffix
    String deleteResponse = ControllerTest.sendDeleteRequest(
        StringUtil.join("/", TEST_INSTANCE.getControllerBaseApiUrl(), "users", "user1?component=CONTROLLER"));
    assertEquals(deleteResponse, "{\"status\":\"User: user1_CONTROLLER has been successfully deleted\"}");

    // Case 2: Create a BROKER user and delete it directly w/o using query param.
    UserConfig brokerUserConfig =
        _userConfigBuilder.setUsername("user1").setComponentType(ComponentType.BROKER).build();
    creationResponse = ControllerTest.sendPostRequest(_createUserUrl, brokerUserConfig.toJsonString());
    assertEquals(creationResponse, "{\"status\":\"User user1_BROKER has been successfully added!\"}");

    // Delete controller user using BROKER suffix
    deleteResponse = ControllerTest.sendDeleteRequest(
        StringUtil.join("/", TEST_INSTANCE.getControllerBaseApiUrl(), "users", "user1?component=BROKER"));
    assertEquals(deleteResponse, "{\"status\":\"User: user1_BROKER has been successfully deleted\"}");
  }

  @Test
  public void testAddUserWithRlsFilters()
      throws Exception {
    String username = "rlsUser1";
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("country='US'"));
    rlsFilters.put("table2", Arrays.asList("department='Engineering'", "level='Senior'"));

    String userConfigString = _userConfigBuilder.setUsername(username)
        .setComponentType(ComponentType.BROKER)
        .setRlsFilters(rlsFilters)
        .build().toJsonString();

    String creationResponse = ControllerTest.sendPostRequest(_createUserUrl, userConfigString);
    assertEquals(creationResponse, "{\"status\":\"User " + username + "_BROKER has been successfully added!\"}");

    // Verify the user was created with RLS filters
    UserConfig userConfig = getUserConfig(username, "BROKER");
    assertNotNull(userConfig.getRlsFilters());
    assertEquals(userConfig.getRlsFilters().size(), 2);
    assertTrue(userConfig.getRlsFilters().containsKey("table1"));
    assertTrue(userConfig.getRlsFilters().containsKey("table2"));
    assertEquals(userConfig.getRlsFilters().get("table1").size(), 1);
    assertEquals(userConfig.getRlsFilters().get("table2").size(), 2);
    // Verify actual filter values
    assertEquals(userConfig.getRlsFilters().get("table1").get(0), "country='US'");
    assertEquals(userConfig.getRlsFilters().get("table2").get(0), "department='Engineering'");
    assertEquals(userConfig.getRlsFilters().get("table2").get(1), "level='Senior'");
  }

  @Test
  public void testAddUserWithComplexRlsFilters()
      throws Exception {
    String username = "rlsUser2";
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("salesTable", Arrays.asList("region='APAC', status='active'"));
    rlsFilters.put("userTable", Arrays.asList("(role='admin' OR role='manager'), active=true"));

    String userConfigString = _userConfigBuilder.setUsername(username)
        .setComponentType(ComponentType.BROKER)
        .setRlsFilters(rlsFilters)
        .build().toJsonString();

    String creationResponse = ControllerTest.sendPostRequest(_createUserUrl, userConfigString);
    assertEquals(creationResponse, "{\"status\":\"User " + username + "_BROKER has been successfully added!\"}");

    // Verify complex filters are stored correctly
    UserConfig userConfig = getUserConfig(username, "BROKER");
    assertNotNull(userConfig.getRlsFilters());
    assertEquals(userConfig.getRlsFilters().get("salesTable").get(0), "region='APAC', status='active'");
    assertEquals(userConfig.getRlsFilters().get("userTable").get(0),
        "(role='admin' OR role='manager'), active=true");
  }

  @Test
  public void testUpdateUserWithRlsFilters()
      throws Exception {
    String username = "rlsUpdateUser";

    // Create user without RLS filters - reset the builder first
    String userConfigString = new UserConfigBuilder()
        .setUsername(username)
        .setPassword("123456")
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);

    // Verify no RLS filters initially
    UserConfig userConfig = getUserConfig(username, "BROKER");
    if (userConfig.getRlsFilters() != null) {
      assertEquals(userConfig.getRlsFilters().size(), 0);
    }

    // Update user to add RLS filters
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("orders", Arrays.asList("customer_id='12345'"));
    rlsFilters.put("products", Arrays.asList("category='electronics'"));

    userConfig.setPassword("newPassword");
    UserConfigBuilder builder = new UserConfigBuilder()
        .setUsername(username)
        .setPassword(userConfig.getPassword())
        .setComponentType(ComponentType.BROKER)
        .setRoleType(userConfig.getRoleType())
        .setRlsFilters(rlsFilters);

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(ControllerTest.sendPutRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forUpdateUserConfig(username, "BROKER", false),
        builder.build().toString()));
    assertTrue(jsonResponse.has("status"));

    // Verify RLS filters were added
    UserConfig modifiedConfig = getUserConfig(username, "BROKER");
    assertNotNull(modifiedConfig.getRlsFilters());
    assertEquals(modifiedConfig.getRlsFilters().size(), 2);
    assertTrue(modifiedConfig.getRlsFilters().containsKey("orders"));
    assertTrue(modifiedConfig.getRlsFilters().containsKey("products"));
    // Verify actual filter values
    assertEquals(modifiedConfig.getRlsFilters().get("orders").get(0), "customer_id='12345'");
    assertEquals(modifiedConfig.getRlsFilters().get("products").get(0), "category='electronics'");
  }

  @Test
  public void testGetUserWithRlsFilters()
      throws Exception {
    String username = "rlsGetUser";
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("transactions", Arrays.asList("amount > 1000", "status='completed'"));

    String userConfigString = _userConfigBuilder.setUsername(username)
        .setComponentType(ComponentType.CONTROLLER)
        .setRlsFilters(rlsFilters)
        .build().toJsonString();

    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);

    // Get user and verify RLS filters are returned
    String userConfigResponse = ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forUserGet(username, "CONTROLLER"));

    JsonNode responseNode = JsonUtils.stringToJsonNode(userConfigResponse);
    JsonNode userNode = responseNode.get(username + "_CONTROLLER");

    assertNotNull(userNode);
    assertTrue(userNode.has("rlsFilters"));
    JsonNode rlsFiltersNode = userNode.get("rlsFilters");
    assertTrue(rlsFiltersNode.has("transactions"));
    assertEquals(rlsFiltersNode.get("transactions").size(), 2);
    // Verify actual filter values
    assertEquals(rlsFiltersNode.get("transactions").get(0).asText(), "amount > 1000");
    assertEquals(rlsFiltersNode.get("transactions").get(1).asText(), "status='completed'");
  }

  @Test
  public void testListAllUsers()
      throws Exception {
    // Create multiple users with different configurations
    String user1 = "listUser1";
    String user2 = "listUser2";

    String userConfig1 = _userConfigBuilder.setUsername(user1)
        .setComponentType(ComponentType.BROKER)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfig1);

    String userConfig2 = _userConfigBuilder.setUsername(user2)
        .setComponentType(ComponentType.CONTROLLER)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfig2);

    // List all users
    String listResponse = ControllerTest.sendGetRequest(
        StringUtil.join("/", TEST_INSTANCE.getControllerBaseApiUrl(), "users"));

    JsonNode responseNode = JsonUtils.stringToJsonNode(listResponse);
    assertTrue(responseNode.has("users"));
    JsonNode usersNode = responseNode.get("users");

    // Verify our created users are in the list
    assertTrue(usersNode.has(user1 + "_BROKER"));
    assertTrue(usersNode.has(user2 + "_CONTROLLER"));
  }

  @Test
  public void testListUsersWithRlsFilters()
      throws Exception {
    String username = "rlsListUser";
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("inventory", Arrays.asList("warehouse='west'"));

    String userConfigString = _userConfigBuilder.setUsername(username)
        .setComponentType(ComponentType.BROKER)
        .setRlsFilters(rlsFilters)
        .build().toJsonString();

    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);

    // List all users and verify RLS filters are included
    String listResponse = ControllerTest.sendGetRequest(
        StringUtil.join("/", TEST_INSTANCE.getControllerBaseApiUrl(), "users"));

    JsonNode responseNode = JsonUtils.stringToJsonNode(listResponse);
    JsonNode usersNode = responseNode.get("users");
    JsonNode userNode = usersNode.get(username + "_BROKER");

    assertNotNull(userNode);
    assertTrue(userNode.has("rlsFilters"));
    JsonNode rlsFiltersNode = userNode.get("rlsFilters");
    assertTrue(rlsFiltersNode.has("inventory"));
    // Verify actual filter value
    assertEquals(rlsFiltersNode.get("inventory").get(0).asText(), "warehouse='west'");
  }

  @Test
  public void testAddUserWithTablesAndPermissions()
      throws Exception {
    String username = "tablesPermUser";
    List<String> tables = Arrays.asList("table1", "table2", "table3");
    List<String> excludeTables = Arrays.asList("sensitiveTable");
    List<AccessType> permissions = Arrays.asList(AccessType.READ, AccessType.UPDATE);

    UserConfigBuilder builder = new UserConfigBuilder()
        .setUsername(username)
        .setPassword("password123")
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .setTableList(tables)
        .setExcludeTableList(excludeTables)
        .setPermissionList(permissions);

    String userConfigString = builder.build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);

    // Verify user was created with tables and permissions
    UserConfig userConfig = getUserConfig(username, "BROKER");
    assertNotNull(userConfig.getTables());
    assertEquals(userConfig.getTables().size(), 3);
    assertNotNull(userConfig.getExcludeTables());
    assertEquals(userConfig.getExcludeTables().size(), 1);
    assertNotNull(userConfig.getPermissios());
    assertEquals(userConfig.getPermissios().size(), 2);
    assertTrue(userConfig.getPermissios().contains(AccessType.READ));
    assertTrue(userConfig.getPermissios().contains(AccessType.UPDATE));
  }

  @Test
  public void testUpdateUserWithTablesAndPermissions()
      throws Exception {
    String username = "updateTablesUser";

    // Create user with initial configuration
    String userConfigString = _userConfigBuilder.setUsername(username)
        .setComponentType(ComponentType.BROKER)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);

    // Update user with tables and permissions
    List<String> tables = Arrays.asList("newTable1", "newTable2");
    List<AccessType> permissions = Arrays.asList(AccessType.READ, AccessType.CREATE, AccessType.DELETE);

    UserConfig userConfig = getUserConfig(username, "BROKER");
    UserConfigBuilder builder = new UserConfigBuilder()
        .setUsername(username)
        .setPassword(userConfig.getPassword())
        .setComponentType(ComponentType.BROKER)
        .setRoleType(userConfig.getRoleType())
        .setTableList(tables)
        .setPermissionList(permissions);

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(ControllerTest.sendPutRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forUpdateUserConfig(username, "BROKER", false),
        builder.build().toString()));
    assertTrue(jsonResponse.has("status"));

    // Verify updates
    UserConfig modifiedConfig = getUserConfig(username, "BROKER");
    assertNotNull(modifiedConfig.getTables());
    assertEquals(modifiedConfig.getTables().size(), 2);
    assertNotNull(modifiedConfig.getPermissios());
    assertEquals(modifiedConfig.getPermissios().size(), 3);
  }

  @Test
  public void testAddUserWithDifferentComponentTypes()
      throws Exception {
    ComponentType[] componentTypes = {
        ComponentType.CONTROLLER, ComponentType.BROKER, ComponentType.SERVER
    };

    for (ComponentType componentType : componentTypes) {
      String username = "user_" + componentType.name().toLowerCase();
      String userConfigString = _userConfigBuilder.setUsername(username)
          .setComponentType(componentType)
          .build().toJsonString();

      String creationResponse = ControllerTest.sendPostRequest(_createUserUrl, userConfigString);
      assertEquals(creationResponse,
          "{\"status\":\"User " + username + "_" + componentType.name() + " has been successfully added!\"}");

      // Verify user was created with correct component type
      UserConfig userConfig = getUserConfig(username, componentType.name());
      assertEquals(userConfig.getComponentType(), componentType);
    }
  }

  @Test
  public void testAddUserWithDifferentRoleTypes()
      throws Exception {
    // Test ADMIN role
    String adminUser = "adminRoleUser";
    String adminConfigString = _userConfigBuilder.setUsername(adminUser)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.ADMIN)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, adminConfigString);

    UserConfig adminConfig = getUserConfig(adminUser, "BROKER");
    assertEquals(adminConfig.getRoleType(), RoleType.ADMIN);

    // Test USER role
    String regularUser = "regularRoleUser";
    String userConfigString = _userConfigBuilder.setUsername(regularUser)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);

    UserConfig regularConfig = getUserConfig(regularUser, "BROKER");
    assertEquals(regularConfig.getRoleType(), RoleType.USER);
  }

  @Test
  public void testGetNonExistentUser()
      throws Exception {
    String nonExistentUser = "nonExistentUser_" + System.currentTimeMillis();
    String response = ControllerTest.sendGetRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forUserGet(nonExistentUser, "BROKER"));

    // Verify response indicates user doesn't exist
    JsonNode responseNode = JsonUtils.stringToJsonNode(response);
    JsonNode userNode = responseNode.get(nonExistentUser + "_BROKER");
    assertTrue(userNode == null || userNode.isNull());
  }

  @Test
  public void testDeleteNonExistentUser()
      throws Exception {
    String nonExistentUser = "nonExistentDeleteUser";
    try {
      ControllerTest.sendDeleteRequest(
          StringUtil.join("/", TEST_INSTANCE.getControllerBaseApiUrl(), "users",
              nonExistentUser + "?component=BROKER"));
      fail("Deleting a non-existent user should fail");
    } catch (IOException e) {
      // Expected exception
      assertTrue(e.getMessage().contains("does not exist") || e.getMessage().contains("not found")
          || e.getMessage().contains("404"));
    }
  }

  @Test
  public void testUpdateUserPasswordWithBcrypt()
      throws Exception {
    String username = "bcryptUser";
    String initialPassword = "initialPass123";

    // Create user
    String userConfigString = _userConfigBuilder.setUsername(username)
        .setPassword(initialPassword)
        .setComponentType(ComponentType.BROKER)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);

    // Update password with passwordChanged=true
    String newPassword = "newSecurePass456";
    UserConfig userConfig = getUserConfig(username, "BROKER");
    userConfig.setPassword(newPassword);

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(ControllerTest.sendPutRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forUpdateUserConfig(username, "BROKER", true),
        userConfig.toString()));
    assertTrue(jsonResponse.has("status"));

    // Verify password was encrypted
    UserConfig modifiedConfig = getUserConfig(username, "BROKER");
    assertFalse(modifiedConfig.getPassword().equals(newPassword)); // Should be encrypted
    assertTrue(BcryptUtils.checkpw(newPassword, modifiedConfig.getPassword())); // Should match when checked
  }

  @Test
  public void testAddUserWithAllFieldsIncludingRls()
      throws Exception {
    String username = "completeUser";
    List<String> tables = Arrays.asList("orders", "customers");
    List<String> excludeTables = Arrays.asList("internal_logs");
    List<AccessType> permissions = Arrays.asList(AccessType.READ, AccessType.UPDATE);
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("orders", Arrays.asList("customer_id='123'", "status='active'"));
    rlsFilters.put("customers", Arrays.asList("region='US'"));

    UserConfigBuilder builder = new UserConfigBuilder()
        .setUsername(username)
        .setPassword("completePass")
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.ADMIN)
        .setTableList(tables)
        .setExcludeTableList(excludeTables)
        .setPermissionList(permissions)
        .setRlsFilters(rlsFilters);

    String userConfigString = builder.build().toJsonString();
    String creationResponse = ControllerTest.sendPostRequest(_createUserUrl, userConfigString);
    assertEquals(creationResponse, "{\"status\":\"User " + username + "_BROKER has been successfully added!\"}");

    // Verify all fields
    UserConfig userConfig = getUserConfig(username, "BROKER");
    assertEquals(userConfig.getUserName(), username);
    assertEquals(userConfig.getRoleType(), RoleType.ADMIN);
    assertEquals(userConfig.getComponentType(), ComponentType.BROKER);
    assertNotNull(userConfig.getTables());
    assertEquals(userConfig.getTables().size(), 2);
    assertNotNull(userConfig.getExcludeTables());
    assertEquals(userConfig.getExcludeTables().size(), 1);
    assertNotNull(userConfig.getPermissios());
    assertEquals(userConfig.getPermissios().size(), 2);
    assertNotNull(userConfig.getRlsFilters());
    assertEquals(userConfig.getRlsFilters().size(), 2);
    assertEquals(userConfig.getRlsFilters().get("orders").size(), 2);
    assertEquals(userConfig.getRlsFilters().get("customers").size(), 1);
    // Verify actual filter values
    assertEquals(userConfig.getRlsFilters().get("orders").get(0), "customer_id='123'");
    assertEquals(userConfig.getRlsFilters().get("orders").get(1), "status='active'");
    assertEquals(userConfig.getRlsFilters().get("customers").get(0), "region='US'");
  }

  @Test
  public void testUpdateUserModifyingRlsFilters()
      throws Exception {
    String username = "modifyRlsUser";

    // Create user with initial RLS filters
    Map<String, List<String>> initialRlsFilters = new HashMap<>();
    initialRlsFilters.put("table1", Arrays.asList("filter1"));

    String userConfigString = _userConfigBuilder.setUsername(username)
        .setComponentType(ComponentType.BROKER)
        .setRlsFilters(initialRlsFilters)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, userConfigString);

    // Update with modified RLS filters
    Map<String, List<String>> modifiedRlsFilters = new HashMap<>();
    modifiedRlsFilters.put("table1", Arrays.asList("filter1_updated", "filter2"));
    modifiedRlsFilters.put("table2", Arrays.asList("newFilter"));

    UserConfig userConfig = getUserConfig(username, "BROKER");
    UserConfigBuilder builder = new UserConfigBuilder()
        .setUsername(username)
        .setPassword(userConfig.getPassword())
        .setComponentType(ComponentType.BROKER)
        .setRoleType(userConfig.getRoleType())
        .setRlsFilters(modifiedRlsFilters);

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(ControllerTest.sendPutRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forUpdateUserConfig(username, "BROKER", false),
        builder.build().toString()));
    assertTrue(jsonResponse.has("status"));

    // Verify RLS filters were modified
    UserConfig modifiedConfig = getUserConfig(username, "BROKER");
    assertNotNull(modifiedConfig.getRlsFilters());
    assertEquals(modifiedConfig.getRlsFilters().size(), 2);
    assertEquals(modifiedConfig.getRlsFilters().get("table1").size(), 2);
    assertTrue(modifiedConfig.getRlsFilters().containsKey("table2"));
    // Verify actual filter values
    assertEquals(modifiedConfig.getRlsFilters().get("table1").get(0), "filter1_updated");
    assertEquals(modifiedConfig.getRlsFilters().get("table1").get(1), "filter2");
    assertEquals(modifiedConfig.getRlsFilters().get("table2").get(0), "newFilter");
  }

  @Test
  public void testGetUserByUsernameAndComponent()
      throws Exception {
    String username = "getUserTest";

    // Create users with same username but different components
    String brokerConfigString = _userConfigBuilder.setUsername(username)
        .setComponentType(ComponentType.BROKER)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, brokerConfigString);

    String controllerConfigString = _userConfigBuilder.setUsername(username)
        .setComponentType(ComponentType.CONTROLLER)
        .build().toJsonString();
    ControllerTest.sendPostRequest(_createUserUrl, controllerConfigString);

    // Get BROKER user
    UserConfig brokerConfig = getUserConfig(username, "BROKER");
    assertEquals(brokerConfig.getComponentType(), ComponentType.BROKER);

    // Get CONTROLLER user
    UserConfig controllerConfig = getUserConfig(username, "CONTROLLER");
    assertEquals(controllerConfig.getComponentType(), ComponentType.CONTROLLER);
  }
}
