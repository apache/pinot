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
package org.apache.pinot.spi.config.user;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class UserConfigTest {

  @Test
  public void testJsonSerializationExcludesDeprecatedMethods()
      throws IOException {
    // Create a UserConfig instance
    List<String> tables = Arrays.asList("table1", "table2");
    List<String> excludeTables = Arrays.asList("excludeTable1");
    List<AccessType> permissions = Arrays.asList(AccessType.READ, AccessType.UPDATE);

    UserConfig userConfig = new UserConfig(
        "testUser",
        "testPassword",
        "BROKER",
        "ADMIN",
        tables,
        excludeTables,
        permissions
    );

    // Serialize to JSON
    String jsonString = userConfig.toJsonString();
    JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);

    // Verify standard fields are present
    assertTrue(jsonNode.has("username"));
    assertEquals(jsonNode.get("username").asText(), "testUser");
    assertTrue(jsonNode.has("password"));
    assertEquals(jsonNode.get("password").asText(), "testPassword");
    assertTrue(jsonNode.has("component"));
    assertEquals(jsonNode.get("component").asText(), "BROKER");
    assertTrue(jsonNode.has("role"));
    assertEquals(jsonNode.get("role").asText(), "ADMIN");
    assertTrue(jsonNode.has("tables"));
    assertTrue(jsonNode.has("excludeTables"));
    assertTrue(jsonNode.has("permissions"));
    assertEquals(jsonNode.get("permissions").size(), 2);

    // Verify deprecated method with typo is NOT in JSON (should be excluded by @JsonIgnore)
    assertFalse(jsonNode.has("permissios"),
        "Deprecated method 'getPermissios()' should not be serialized to JSON");

    // Verify correct permissions field is present (not the typo'd one)
    assertTrue(jsonNode.has("permissions"),
        "Correct 'getPermissions()' method should be serialized to JSON");

    // Verify utility method is NOT in JSON (should be excluded by @JsonIgnore)
    assertFalse(jsonNode.has("usernameWithComponent"),
        "Utility method 'getUsernameWithComponent()' should not be serialized to JSON");
  }

  @Test
  public void testJsonDeserializationWithAllFields()
      throws IOException {
    String jsonString = "{"
        + "\"username\":\"admin\","
        + "\"password\":\"secret\","
        + "\"component\":\"CONTROLLER\","
        + "\"role\":\"ADMIN\","
        + "\"tables\":[\"users\",\"orders\"],"
        + "\"excludeTables\":[\"temp\"],"
        + "\"permissions\":[\"CREATE\",\"READ\",\"UPDATE\",\"DELETE\"]"
        + "}";

    UserConfig userConfig = JsonUtils.stringToObject(jsonString, UserConfig.class);

    assertNotNull(userConfig);
    assertEquals(userConfig.getUserName(), "admin");
    assertEquals(userConfig.getPassword(), "secret");
    assertEquals(userConfig.getComponentType(), ComponentType.CONTROLLER);
    assertEquals(userConfig.getRoleType(), RoleType.ADMIN);
    assertEquals(userConfig.getTables().size(), 2);
    assertTrue(userConfig.getTables().contains("users"));
    assertTrue(userConfig.getTables().contains("orders"));
    assertEquals(userConfig.getExcludeTables().size(), 1);
    assertTrue(userConfig.getExcludeTables().contains("temp"));
    assertEquals(userConfig.getPermissions().size(), 4);

    // Verify deprecated method still works for backward compatibility in Java API
    assertEquals(userConfig.getPermissios(), userConfig.getPermissions(),
        "Deprecated method should return same value as correct method");
  }

  @Test
  public void testJsonDeserializationWithMinimalFields()
      throws IOException {
    String jsonString = "{"
        + "\"username\":\"user1\","
        + "\"password\":\"pass123\","
        + "\"component\":\"BROKER\","
        + "\"role\":\"USER\""
        + "}";

    UserConfig userConfig = JsonUtils.stringToObject(jsonString, UserConfig.class);

    assertNotNull(userConfig);
    assertEquals(userConfig.getUserName(), "user1");
    assertEquals(userConfig.getPassword(), "pass123");
    assertEquals(userConfig.getComponentType(), ComponentType.BROKER);
    assertEquals(userConfig.getRoleType(), RoleType.USER);
    assertNull(userConfig.getTables());
    assertNull(userConfig.getExcludeTables());
    assertNull(userConfig.getPermissions());
    assertNull(userConfig.getPermissios()); // Deprecated method should also return null
  }

  @Test
  public void testSerializationRoundTrip()
      throws IOException {
    // Create a UserConfig with all fields
    List<AccessType> permissions = Arrays.asList(AccessType.READ, AccessType.UPDATE);
    UserConfig original = new UserConfig(
        "testuser",
        "testpass",
        "server",  // Test case-insensitive conversion
        "user",    // Test case-insensitive conversion
        Arrays.asList("table1"),
        Arrays.asList("exclude1"),
        permissions
    );

    // Serialize and deserialize
    String jsonString = original.toJsonString();
    UserConfig deserialized = JsonUtils.stringToObject(jsonString, UserConfig.class);

    // Verify all fields match
    assertEquals(deserialized.getUserName(), original.getUserName());
    assertEquals(deserialized.getPassword(), original.getPassword());
    assertEquals(deserialized.getComponentType(), original.getComponentType());
    assertEquals(deserialized.getRoleType(), original.getRoleType());
    assertEquals(deserialized.getTables(), original.getTables());
    assertEquals(deserialized.getExcludeTables(), original.getExcludeTables());
    assertEquals(deserialized.getPermissions(), original.getPermissions());

    // Verify the serialized JSON doesn't contain deprecated/utility methods
    JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);
    assertFalse(jsonNode.has("permissios"),
        "Deprecated method 'getPermissios()' should not be serialized");
    assertFalse(jsonNode.has("usernameWithComponent"),
        "Utility method 'getUsernameWithComponent()' should not be serialized");
  }
}
