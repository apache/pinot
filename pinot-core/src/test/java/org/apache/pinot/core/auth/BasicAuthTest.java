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
package org.apache.pinot.core.auth;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.UserConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BasicAuthTest {

  @Test
  public void testBasicAuthPrincipal() {
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("READ"))
        .hasTable("myTable"));
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable", "myTable1"),
        Collections.emptySet(), ImmutableSet.of("Read"))
        .hasTable("myTable1"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("read"))
        .hasTable("myTable1"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable", "myTable1"),
        Collections.emptySet(), ImmutableSet.of("read"))
        .hasTable("myTable2"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), ImmutableSet.of("myTable"),
        ImmutableSet.of("read"))
        .hasTable("myTable"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", Collections.emptySet(), ImmutableSet.of("myTable"),
        ImmutableSet.of("read"))
        .hasTable("myTable"));
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), ImmutableSet.of("myTable1"),
        ImmutableSet.of("read"))
        .hasTable("myTable"));

    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("READ"))
        .hasPermission("read"));
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("Read"))
        .hasPermission("READ"));
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("read"))
        .hasPermission("Read"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("read"))
        .hasPermission("write"));

    Assert.assertEquals(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("read"), Map.of("myTable", List.of("cityID > 100")))
        .getRLSFilters("myTable"), Optional.of(List.of("cityID > 100")));
  }

  @Test
  public void testExtractBasicAuthPrincipals() {
    // Test basic configuration with multiple principals
    Map<String, Object> config = new HashMap<>();
    config.put("principals", "admin,user");
    config.put("principals.admin.password", "verysecret");
    config.put("principals.user.password", "secret");
    config.put("principals.user.tables", "lessImportantStuff,lesserImportantStuff,leastImportantStuff");
    config.put("principals.user.excludeTables", "excludedTable");
    config.put("principals.user.permissions", "read,write");
    config.put("principals.user.lessImportantStuff.rls", "cityID > 100,status = 'active'");
    config.put("principals.user.lesserImportantStuff.rls", "region = 'US'");

    PinotConfiguration configuration = new PinotConfiguration(config);
    List<BasicAuthPrincipal> principals = BasicAuthUtils.extractBasicAuthPrincipals(configuration, "principals");

    Assert.assertEquals(principals.size(), 2);

    // Verify admin principal (should have no table restrictions)
    BasicAuthPrincipal adminPrincipal = principals.stream()
        .filter(p -> p.getName().equals("admin"))
        .findFirst()
        .orElse(null);
    Assert.assertNotNull(adminPrincipal);
    Assert.assertEquals(adminPrincipal.getName(), "admin");

    // Verify user principal
    BasicAuthPrincipal userPrincipal = principals.stream()
        .filter(p -> p.getName().equals("user"))
        .findFirst()
        .orElse(null);
    Assert.assertNotNull(userPrincipal);
    Assert.assertEquals(userPrincipal.getName(), "user");

    Set<String> expectedTables = ImmutableSet.of("lessImportantStuff", "lesserImportantStuff", "leastImportantStuff");
    expectedTables.forEach(tableName -> {
      Assert.assertTrue(userPrincipal.hasTable(tableName));
    });

    Set<String> expectedExcludeTables = ImmutableSet.of("excludedTable");
    expectedExcludeTables.forEach(tableName -> {
      Assert.assertFalse(userPrincipal.hasTable(tableName));
    });

    Set<String> expectedPermissions = ImmutableSet.of("read", "write");
    expectedPermissions.forEach(permission -> {
      Assert.assertTrue(userPrincipal.hasPermission(permission));
    });

    // Verify RLS filters

    List<String> lessImportantStuffFilters = userPrincipal.getRLSFilters("lessImportantStuff").get();
    Assert.assertNotNull(lessImportantStuffFilters);
    Assert.assertEquals(lessImportantStuffFilters.size(), 2);
    Assert.assertTrue(lessImportantStuffFilters.contains("cityID > 100"));
    Assert.assertTrue(lessImportantStuffFilters.contains("status = 'active'"));

    List<String> lesserImportantStuffFilters = userPrincipal.getRLSFilters("lesserImportantStuff").get();
    Assert.assertNotNull(lesserImportantStuffFilters);
    Assert.assertEquals(lesserImportantStuffFilters.size(), 1);
    Assert.assertTrue(lesserImportantStuffFilters.contains("region = 'US'"));

    // Verify no RLS filters for leastImportantStuff (not configured)
    Assert.assertTrue(userPrincipal.getRLSFilters("leastImportantStuff").isEmpty());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "must provide "
      + "principals")
  public void testExtractBasicAuthPrincipalsNoPrincipals() {
    Map<String, Object> config = new HashMap<>();
    PinotConfiguration configuration = new PinotConfiguration(config);
    BasicAuthUtils.extractBasicAuthPrincipals(configuration, "principals");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "must provide a "
      + "password for.*")
  public void testExtractBasicAuthPrincipalsNoPassword() {
    Map<String, Object> config = new HashMap<>();
    config.put("principals", "admin");
    PinotConfiguration configuration = new PinotConfiguration(config);
    BasicAuthUtils.extractBasicAuthPrincipals(configuration, "principals");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".* is not a valid name")
  public void testExtractBasicAuthPrincipalsBlankName() {
    Map<String, Object> config = new HashMap<>();
    config.put("principals", "admin, ,user");
    config.put("principals.admin.password", "secret");
    config.put("principals.user.password", "secret");
    PinotConfiguration configuration = new PinotConfiguration(config);
    BasicAuthUtils.extractBasicAuthPrincipals(configuration, "principals");
  }

  @Test
  public void testExtractBasicAuthPrincipalsFromUserConfig() {
    // Test with multiple UserConfig objects with various configurations
    List<UserConfig> userConfigList = new ArrayList<>();

    // User 1: Admin with minimal configuration
    UserConfig adminUser = new UserConfigBuilder()
        .setUsername("admin")
        .setPassword("adminpass")
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.ADMIN)
        .build();
    userConfigList.add(adminUser);

    // User 2: Regular user with tables, excludeTables, and permissions
    UserConfig regularUser = new UserConfigBuilder()
        .setUsername("regularUser")
        .setPassword("userpass")
        .setComponentType(ComponentType.CONTROLLER)
        .setRoleType(RoleType.USER)
        .setTableList(Arrays.asList("table1", "table2", "table3"))
        .setExcludeTableList(Arrays.asList("excludedTable1"))
        .setPermissionList(Arrays.asList(AccessType.READ, AccessType.UPDATE))
        .build();
    userConfigList.add(regularUser);

    // User 3: User with RLS filters
    Map<String, List<String>> rlsFilters = new HashMap<>();
    rlsFilters.put("table1", Arrays.asList("cityID > 100", "status = 'active'"));
    rlsFilters.put("table2", Arrays.asList("region = 'US'"));

    UserConfig userWithRls = new UserConfigBuilder()
        .setUsername("rlsUser")
        .setPassword("rlspass")
        .setComponentType(ComponentType.SERVER)
        .setRoleType(RoleType.USER)
        .setTableList(Arrays.asList("table1", "table2"))
        .setPermissionList(Arrays.asList(AccessType.READ))
        .setRlsFilters(rlsFilters)
        .build();
    userConfigList.add(userWithRls);

    // User 4: User with all fields populated
    Map<String, List<String>> fullRlsFilters = new HashMap<>();
    fullRlsFilters.put("fullTable", Arrays.asList("department = 'Engineering'"));

    UserConfig fullUser = new UserConfigBuilder()
        .setUsername("fullUser")
        .setPassword("fullpass")
        .setComponentType(ComponentType.MINION)
        .setRoleType(RoleType.ADMIN)
        .setTableList(Arrays.asList("fullTable", "anotherTable"))
        .setExcludeTableList(Arrays.asList("excludedTable2", "excludedTable3"))
        .setPermissionList(Arrays.asList(AccessType.CREATE, AccessType.READ, AccessType.UPDATE, AccessType.DELETE))
        .setRlsFilters(fullRlsFilters)
        .build();
    userConfigList.add(fullUser);

    // Extract principals
    List<ZkBasicAuthPrincipal> principals = BasicAuthUtils.extractBasicAuthPrincipals(userConfigList);

    // Verify the size
    Assert.assertEquals(principals.size(), 4, "Should have 4 principals");

    // Verify admin user
    ZkBasicAuthPrincipal adminPrincipal = principals.stream()
        .filter(p -> p.getName().equals("admin"))
        .findFirst()
        .orElse(null);
    Assert.assertNotNull(adminPrincipal, "Admin principal should exist");
    Assert.assertEquals(adminPrincipal.getName(), "admin");
    Assert.assertEquals(adminPrincipal.getPassword(), "adminpass");
    Assert.assertEquals(adminPrincipal.getComponent(), "BROKER");
    Assert.assertEquals(adminPrincipal.getRole(), "ADMIN");
    Assert.assertTrue(adminPrincipal.hasPermission(RoleType.ADMIN, ComponentType.BROKER));

    // Verify regular user
    ZkBasicAuthPrincipal regularPrincipal = principals.stream()
        .filter(p -> p.getName().equals("regularUser"))
        .findFirst()
        .orElse(null);
    Assert.assertNotNull(regularPrincipal, "Regular user principal should exist");
    Assert.assertEquals(regularPrincipal.getName(), "regularUser");
    Assert.assertEquals(regularPrincipal.getPassword(), "userpass");
    Assert.assertEquals(regularPrincipal.getComponent(), "CONTROLLER");
    Assert.assertEquals(regularPrincipal.getRole(), "USER");
    Assert.assertTrue(regularPrincipal.hasTable("table1"));
    Assert.assertTrue(regularPrincipal.hasTable("table2"));
    Assert.assertTrue(regularPrincipal.hasTable("table3"));
    Assert.assertFalse(regularPrincipal.hasTable("excludedTable1"));
    Assert.assertTrue(regularPrincipal.hasPermission("READ"));
    Assert.assertTrue(regularPrincipal.hasPermission("UPDATE"));
    Assert.assertFalse(regularPrincipal.hasPermission("DELETE"));

    // Verify user with RLS filters
    ZkBasicAuthPrincipal rlsPrincipal = principals.stream()
        .filter(p -> p.getName().equals("rlsUser"))
        .findFirst()
        .orElse(null);
    Assert.assertNotNull(rlsPrincipal, "RLS user principal should exist");
    Assert.assertEquals(rlsPrincipal.getName(), "rlsUser");
    Assert.assertEquals(rlsPrincipal.getPassword(), "rlspass");
    Assert.assertEquals(rlsPrincipal.getComponent(), "SERVER");
    Assert.assertEquals(rlsPrincipal.getRole(), "USER");
    Assert.assertTrue(rlsPrincipal.hasTable("table1"));
    Assert.assertTrue(rlsPrincipal.hasTable("table2"));

    // Verify RLS filters for table1
    Optional<List<String>> table1Filters = rlsPrincipal.getRLSFilters("table1");
    Assert.assertTrue(table1Filters.isPresent(), "RLS filters for table1 should exist");
    Assert.assertEquals(table1Filters.get().size(), 2);
    Assert.assertTrue(table1Filters.get().contains("cityID > 100"));
    Assert.assertTrue(table1Filters.get().contains("status = 'active'"));

    // Verify RLS filters for table2
    Optional<List<String>> table2Filters = rlsPrincipal.getRLSFilters("table2");
    Assert.assertTrue(table2Filters.isPresent(), "RLS filters for table2 should exist");
    Assert.assertEquals(table2Filters.get().size(), 1);
    Assert.assertTrue(table2Filters.get().contains("region = 'US'"));

    // Verify full user with all fields
    ZkBasicAuthPrincipal fullPrincipal = principals.stream()
        .filter(p -> p.getName().equals("fullUser"))
        .findFirst()
        .orElse(null);
    Assert.assertNotNull(fullPrincipal, "Full user principal should exist");
    Assert.assertEquals(fullPrincipal.getName(), "fullUser");
    Assert.assertEquals(fullPrincipal.getPassword(), "fullpass");
    Assert.assertEquals(fullPrincipal.getComponent(), "MINION");
    Assert.assertEquals(fullPrincipal.getRole(), "ADMIN");
    Assert.assertTrue(fullPrincipal.hasTable("fullTable"));
    Assert.assertTrue(fullPrincipal.hasTable("anotherTable"));
    Assert.assertFalse(fullPrincipal.hasTable("excludedTable2"));
    Assert.assertFalse(fullPrincipal.hasTable("excludedTable3"));
    Assert.assertTrue(fullPrincipal.hasPermission("CREATE"));
    Assert.assertTrue(fullPrincipal.hasPermission("READ"));
    Assert.assertTrue(fullPrincipal.hasPermission("UPDATE"));
    Assert.assertTrue(fullPrincipal.hasPermission("DELETE"));

    // Verify RLS filters for fullTable
    Optional<List<String>> fullTableFilters = fullPrincipal.getRLSFilters("fullTable");
    Assert.assertTrue(fullTableFilters.isPresent(), "RLS filters for fullTable should exist");
    Assert.assertEquals(fullTableFilters.get().size(), 1);
    Assert.assertTrue(fullTableFilters.get().contains("department = 'Engineering'"));

    // Verify no RLS filters for anotherTable
    Optional<List<String>> anotherTableFilters = fullPrincipal.getRLSFilters("anotherTable");
    Assert.assertFalse(anotherTableFilters.isPresent(), "RLS filters for anotherTable should not exist");
  }

  @Test
  public void testExtractBasicAuthPrincipalsFromUserConfigEmptyList() {
    // Test with empty list
    List<UserConfig> emptyList = new ArrayList<>();
    List<ZkBasicAuthPrincipal> principals = BasicAuthUtils.extractBasicAuthPrincipals(emptyList);
    Assert.assertNotNull(principals, "Principals list should not be null");
    Assert.assertEquals(principals.size(), 0, "Principals list should be empty");
  }

  @Test
  public void testExtractBasicAuthPrincipalsFromUserConfigNullOptionalFields() {
    // Test with user having null optional fields
    List<UserConfig> userConfigList = new ArrayList<>();

    UserConfig userWithNulls = new UserConfigBuilder()
        .setUsername("userWithNulls")
        .setPassword("password")
        .setComponentType(ComponentType.PROXY)
        .setRoleType(RoleType.USER)
        .build();
    userConfigList.add(userWithNulls);

    List<ZkBasicAuthPrincipal> principals = BasicAuthUtils.extractBasicAuthPrincipals(userConfigList);

    Assert.assertEquals(principals.size(), 1);
    ZkBasicAuthPrincipal principal = principals.get(0);
    Assert.assertEquals(principal.getName(), "userWithNulls");
    Assert.assertEquals(principal.getPassword(), "password");
    Assert.assertEquals(principal.getComponent(), "PROXY");
    Assert.assertEquals(principal.getRole(), "USER");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".* is not a valid "
      + "username")
  public void testExtractBasicAuthPrincipalsFromUserConfigBlankUsername() {
    // Test with blank username
    List<UserConfig> userConfigList = new ArrayList<>();
    UserConfig userWithBlankName = new UserConfigBuilder()
        .setUsername("  ")
        .setPassword("password")
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .build();
    userConfigList.add(userWithBlankName);

    BasicAuthUtils.extractBasicAuthPrincipals(userConfigList);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "must provide a "
      + "password for.*")
  public void testExtractBasicAuthPrincipalsFromUserConfigBlankPassword() {
    // Test with blank password
    List<UserConfig> userConfigList = new ArrayList<>();
    UserConfig userWithBlankPassword = new UserConfigBuilder()
        .setUsername("user")
        .setPassword("  ")
        .setComponentType(ComponentType.BROKER)
        .setRoleType(RoleType.USER)
        .build();
    userConfigList.add(userWithBlankPassword);

    BasicAuthUtils.extractBasicAuthPrincipals(userConfigList);
  }
}
