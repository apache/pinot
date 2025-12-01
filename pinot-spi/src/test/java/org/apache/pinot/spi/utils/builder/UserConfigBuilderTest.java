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

package org.apache.pinot.spi.utils.builder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link UserConfigBuilder}
 */
public class UserConfigBuilderTest {

  private static final String TEST_USERNAME = "testUser";
  private static final String TEST_PASSWORD = "testPassword";

  @Test
  public void testBasicUserConfigBuild() {
    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .build();

    Assert.assertNotNull(userConfig, "UserConfig should not be null");
    Assert.assertEquals(userConfig.getUserName(), TEST_USERNAME, "Username should match");
    Assert.assertEquals(userConfig.getPassword(), TEST_PASSWORD, "Password should match");
    Assert.assertEquals(userConfig.getComponentType(), ComponentType.BROKER, "Component type should match");
    Assert.assertEquals(userConfig.getRoleType(), RoleType.USER, "Role type should match");
    Assert.assertNull(userConfig.getTables(), "Tables should be null when not set");
    Assert.assertNull(userConfig.getExcludeTables(), "Exclude tables should be null when not set");
    Assert.assertNull(userConfig.getPermissios(), "Permissions should be null when not set");
    Assert.assertNull(userConfig.getRlsFilters(), "RLS filters should be null when not set");
  }

  @Test
  public void testUserConfigWithTableList() {
    List<String> tableList = Arrays.asList("table1", "table2", "table3");

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.CONTROLLER)
        .setRoleType(RoleType.ADMIN)
        .setTableList(tableList)
        .build();

    Assert.assertNotNull(userConfig.getTables(), "Tables should not be null");
    Assert.assertEquals(userConfig.getTables(), tableList, "Table list should match");
    Assert.assertEquals(userConfig.getTables().size(), 3, "Table list size should be 3");
  }

  @Test
  public void testUserConfigWithExcludeTableList() {
    List<String> excludeTableList = Arrays.asList("excludeTable1", "excludeTable2");

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.SERVER)
        .setRoleType(RoleType.USER)
        .setExcludeTableList(excludeTableList)
        .build();

    Assert.assertNotNull(userConfig.getExcludeTables(), "Exclude tables should not be null");
    Assert.assertEquals(userConfig.getExcludeTables(), excludeTableList, "Exclude table list should match");
    Assert.assertEquals(userConfig.getExcludeTables().size(), 2, "Exclude table list size should be 2");
  }

  @Test
  public void testUserConfigWithPermissionList() {
    List<AccessType> permissionList = Arrays.asList(AccessType.READ, AccessType.CREATE, AccessType.UPDATE);

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.MINION)
        .setRoleType(RoleType.ADMIN)
        .setPermissionList(permissionList)
        .build();

    Assert.assertNotNull(userConfig.getPermissios(), "Permissions should not be null");
    Assert.assertEquals(userConfig.getPermissios(), permissionList, "Permission list should match");
    Assert.assertEquals(userConfig.getPermissios().size(), 3, "Permission list size should be 3");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.READ), "Should contain READ permission");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.CREATE), "Should contain CREATE permission");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.UPDATE), "Should contain UPDATE permission");
  }

  @Test
  public void testUserConfigWithRlsFilters() {
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("filter1", "filter2"));
    rlsFilters.put("table2", Arrays.asList("filter3"));

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.PROXY)
        .setRoleType(RoleType.USER)
        .setRlsFilters(rlsFilters)
        .build();

    Assert.assertNotNull(userConfig.getRlsFilters(), "RLS filters should not be null");
    Assert.assertEquals(userConfig.getRlsFilters(), rlsFilters, "RLS filters should match");
    Assert.assertEquals(userConfig.getRlsFilters().size(), 2, "RLS filters map size should be 2");
    Assert.assertTrue(userConfig.getRlsFilters().containsKey("table1"), "Should contain table1 filters");
    Assert.assertTrue(userConfig.getRlsFilters().containsKey("table2"), "Should contain table2 filters");
    Assert.assertEquals(userConfig.getRlsFilters().get("table1").size(), 2, "table1 should have 2 filters");
    Assert.assertEquals(userConfig.getRlsFilters().get("table2").size(), 1, "table2 should have 1 filter");
  }

  @Test
  public void testUserConfigWithAllFields() {
    List<String> tableList = Arrays.asList("table1", "table2");
    List<String> excludeTableList = Arrays.asList("excludeTable1");
    List<AccessType> permissionList = Arrays.asList(AccessType.READ, AccessType.UPDATE, AccessType.DELETE);
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("country='US'", "department='Engineering'"));

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername("adminUser")
        .setPassword("adminPass123")
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.ADMIN)
        .setTableList(tableList)
        .setExcludeTableList(excludeTableList)
        .setPermissionList(permissionList)
        .setRlsFilters(rlsFilters)
        .build();

    Assert.assertNotNull(userConfig, "UserConfig should not be null");
    Assert.assertEquals(userConfig.getUserName(), "adminUser", "Username should match");
    Assert.assertEquals(userConfig.getPassword(), "adminPass123", "Password should match");
    Assert.assertEquals(userConfig.getComponentType(), ComponentType.BROKER, "Component type should match");
    Assert.assertEquals(userConfig.getRoleType(), RoleType.ADMIN, "Role type should match");
    Assert.assertEquals(userConfig.getTables(), tableList, "Table list should match");
    Assert.assertEquals(userConfig.getExcludeTables(), excludeTableList, "Exclude table list should match");
    Assert.assertEquals(userConfig.getPermissios(), permissionList, "Permission list should match");
    Assert.assertEquals(userConfig.getRlsFilters(), rlsFilters, "RLS filters should match");
  }

  @Test
  public void testBuilderChaining() {
    UserConfigBuilder builder = new UserConfigBuilder();

    // Verify that each setter returns the builder instance for chaining
    UserConfigBuilder result1 = builder.setUsername(TEST_USERNAME);
    Assert.assertSame(result1, builder, "setUsername should return the builder instance");

    UserConfigBuilder result2 = builder.setPassword(TEST_PASSWORD);
    Assert.assertSame(result2, builder, "setPassword should return the builder instance");

    UserConfigBuilder result3 = builder.setComponentType(ComponentType.BROKER);
    Assert.assertSame(result3, builder, "setComponentType should return the builder instance");

    UserConfigBuilder result4 = builder.setRoleType(RoleType.USER);
    Assert.assertSame(result4, builder, "setRoleType should return the builder instance");

    UserConfigBuilder result5 = builder.setTableList(Arrays.asList("table1"));
    Assert.assertSame(result5, builder, "setTableList should return the builder instance");

    UserConfigBuilder result6 = builder.setExcludeTableList(Arrays.asList("excludeTable1"));
    Assert.assertSame(result6, builder, "setExcludeTableList should return the builder instance");

    UserConfigBuilder result7 = builder.setPermissionList(Arrays.asList(AccessType.READ));
    Assert.assertSame(result7, builder, "setPermissionList should return the builder instance");

    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("filter1"));
    UserConfigBuilder result8 = builder.setRlsFilters(rlsFilters);
    Assert.assertSame(result8, builder, "setRlsFilters should return the builder instance");
  }

  @Test
  public void testMultipleComponentTypes() {
    // Test with different component types
    ComponentType[] componentTypes = {
        ComponentType.CONTROLLER, ComponentType.BROKER, ComponentType.SERVER,
        ComponentType.MINION, ComponentType.PROXY
    };

    for (ComponentType componentType : componentTypes) {
      UserConfig userConfig = new UserConfigBuilder()
          .setUsername(TEST_USERNAME)
          .setPassword(TEST_PASSWORD)
          .setComponentType(componentType)
          .setRoleType(RoleType.USER)
          .build();

      Assert.assertEquals(userConfig.getComponentType(), componentType,
          "Component type should match for " + componentType);
    }
  }

  @Test
  public void testMultipleRoleTypes() {
    // Test with different role types
    RoleType[] roleTypes = {RoleType.ADMIN, RoleType.USER};

    for (RoleType roleType : roleTypes) {
      UserConfig userConfig = new UserConfigBuilder()
          .setUsername(TEST_USERNAME)
          .setPassword(TEST_PASSWORD)
          .setComponentType(ComponentType.BROKER)
          .setRoleType(roleType)
          .build();

      Assert.assertEquals(userConfig.getRoleType(), roleType,
          "Role type should match for " + roleType);
    }
  }

  @Test
  public void testAllAccessTypes() {
    // Test with all access types
    List<AccessType> allPermissions = Arrays.asList(
        AccessType.CREATE, AccessType.READ, AccessType.UPDATE, AccessType.DELETE
    );

    UserConfig userConfig = new UserConfigBuilder()
        .setUsername(TEST_USERNAME)
        .setPassword(TEST_PASSWORD)
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.ADMIN)
        .setPermissionList(allPermissions)
        .build();

    Assert.assertEquals(userConfig.getPermissios().size(), 4, "Should have all 4 access types");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.CREATE), "Should contain CREATE");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.READ), "Should contain READ");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.UPDATE), "Should contain UPDATE");
    Assert.assertTrue(userConfig.getPermissios().contains(AccessType.DELETE), "Should contain DELETE");
  }
}
