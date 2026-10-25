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
import io.swagger.annotations.ApiParam;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.controller.api.resources.PinotAccessControlUserRestletResource;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.UserConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PinotAccessControlUserRestletResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();

  private final UserConfigBuilder _userConfigBuilder = new UserConfigBuilder();

  @BeforeClass
  public void setup()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
    _userConfigBuilder.setUsername("testUser").setPassword("123456").setComponentType(ComponentType.CONTROLLER)
        .setRoleType(RoleType.USER);
  }

  private UserConfig getUserConfig(String username, String componentType)
      throws Exception {
    String userConfigString =
        TEST_INSTANCE.getOrCreateAdminClient().getUserClient().getUser(username, componentType);
    String usernameWithType = username + "_" + componentType;
    return JsonUtils.jsonNodeToObject(JsonUtils.stringToJsonNode(userConfigString).get(usernameWithType),
        UserConfig.class);
  }

  private static ApiParam getApiParam(Method method, int parameterIndex) {
    for (Annotation annotation : method.getParameterAnnotations()[parameterIndex]) {
      if (annotation instanceof ApiParam) {
        return (ApiParam) annotation;
      }
    }
    return null;
  }

  @Test
  public void testGetUserWithoutComponentReturnsClearBadRequest()
      throws Exception {
    Pair<Integer, String> response = ControllerTest.sendGetRequestWithStatusCode(
        TEST_INSTANCE.getControllerBaseApiUrl() + "/users/testUser", null);
    assertEquals(response.getLeft().intValue(), Response.Status.BAD_REQUEST.getStatusCode());
    assertTrue(response.getRight().contains("Component type is required"));
    assertFalse(response.getRight().contains("Cannot invoke"));
  }

  @Test
  public void testUserComponentQueryParamIsRequiredInSwagger()
      throws Exception {
    Method getUser = PinotAccessControlUserRestletResource.class.getDeclaredMethod("getUser", String.class,
        String.class);
    Method deleteUser = PinotAccessControlUserRestletResource.class.getDeclaredMethod("deleteUser", String.class,
        String.class);
    Method updateUserConfig = PinotAccessControlUserRestletResource.class.getDeclaredMethod("updateUserConfig",
        String.class, String.class, boolean.class, String.class);

    ApiParam getUserComponent = getApiParam(getUser, 1);
    ApiParam deleteUserComponent = getApiParam(deleteUser, 1);
    ApiParam updateUserComponent = getApiParam(updateUserConfig, 1);

    assertNotNull(getUserComponent);
    assertNotNull(deleteUserComponent);
    assertNotNull(updateUserComponent);
    assertTrue(getUserComponent.required());
    assertTrue(deleteUserComponent.required());
    assertTrue(updateUserComponent.required());
  }

  @Test
  public void testUpdateUserConfig()
      throws Exception {
    String username = "updateTC";
    String userConfigString =
        _userConfigBuilder.setUsername(username).setComponentType(ComponentType.CONTROLLER).build().toJsonString();
    TEST_INSTANCE.getOrCreateAdminClient().getUserClient().createUser(userConfigString);
    // user creation should succeed
    UserConfig userConfig = getUserConfig(username, "CONTROLLER");
    assertEquals(userConfig.getRoleType().toString(), RoleType.USER.toString());
    assertTrue(BcryptUtils.checkpw("123456", userConfig.getPassword()));
    userConfig.setRole("ADMIN");
    userConfig.setPassword("654321");

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(
        TEST_INSTANCE.getOrCreateAdminClient().getUserClient().updateUser(username, "CONTROLLER", true,
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
    String creationResponse =
        TEST_INSTANCE.getOrCreateAdminClient().getUserClient().createUser(controllerUserConfig.toJsonString());
    assertEquals(creationResponse, "{\"status\":\"User user1_CONTROLLER has been successfully added!\"}");

    // Delete controller user using CONTROLLER suffix
    String deleteResponse =
        TEST_INSTANCE.getOrCreateAdminClient().getUserClient().deleteUser("user1", "CONTROLLER");
    assertEquals(deleteResponse, "{\"status\":\"User: user1_CONTROLLER has been successfully deleted\"}");

    // Case 2: Create a BROKER user and delete it directly w/o using query param.
    UserConfig brokerUserConfig =
        _userConfigBuilder.setUsername("user1").setComponentType(ComponentType.BROKER).build();
    creationResponse =
        TEST_INSTANCE.getOrCreateAdminClient().getUserClient().createUser(brokerUserConfig.toJsonString());
    assertEquals(creationResponse, "{\"status\":\"User user1_BROKER has been successfully added!\"}");

    // Delete controller user using BROKER suffix
    deleteResponse = TEST_INSTANCE.getOrCreateAdminClient().getUserClient().deleteUser("user1", "BROKER");
    assertEquals(deleteResponse, "{\"status\":\"User: user1_BROKER has been successfully deleted\"}");
  }
}
