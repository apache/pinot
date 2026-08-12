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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class BasicAuthTest {

  @Test
  public void testBasicAuthPrincipal() {
    assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(),
        ImmutableSet.of("READ"))
        .hasTable("myTable"));
    assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable", "myTable1"),
        Set.of(), ImmutableSet.of("Read"))
        .hasTable("myTable1"));
    assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(),
        ImmutableSet.of("read"))
        .hasTable("myTable1"));
    assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable", "myTable1"),
        Set.of(), ImmutableSet.of("read"))
        .hasTable("myTable2"));
    assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), ImmutableSet.of("myTable"),
        ImmutableSet.of("read"))
        .hasTable("myTable"));
    assertFalse(new BasicAuthPrincipal("name", "token", Set.of(), ImmutableSet.of("myTable"),
        ImmutableSet.of("read"))
        .hasTable("myTable"));
    assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), ImmutableSet.of("myTable1"),
        ImmutableSet.of("read"))
        .hasTable("myTable"));
    assertTrue(new BasicAuthPrincipal("name", "token", Set.of(), Set.of(), Set.of("read"))
        .hasUnrestrictedTableAccess());
    assertFalse(new BasicAuthPrincipal("name", "token", Set.of("myTable"), Set.of(), Set.of("read"))
        .hasUnrestrictedTableAccess());
    assertFalse(new BasicAuthPrincipal("name", "token", Set.of(), Set.of("myTable"), Set.of("read"))
        .hasUnrestrictedTableAccess());

    assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(),
        ImmutableSet.of("READ"))
        .hasPermission("read"));
    assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(),
        ImmutableSet.of("Read"))
        .hasPermission("READ"));
    assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(),
        ImmutableSet.of("read"))
        .hasPermission("Read"));
    assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(),
        ImmutableSet.of("read"))
        .hasPermission("write"));
    assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(), Set.of())
        .hasExplicitPermission(Actions.Cluster.GET_ZNODE));
    assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(),
        ImmutableSet.of(Actions.Cluster.GET_ZNODE))
        .hasExplicitPermission(Actions.Cluster.GET_ZNODE));

    assertEquals(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(),
        ImmutableSet.of("read"), Map.of("myTable", List.of("cityID > 100")))
        .getRLSFilters("myTable"), Optional.of(List.of("cityID > 100")));
  }

  @Test
  public void testBasicAuthPrincipalWithNullTableEntry() {
    Set<String> tables = new HashSet<>(Set.of("myTable"));
    tables.add(null);
    BasicAuthPrincipal included = new BasicAuthPrincipal("name", "token", tables, Set.of(), Set.of("read"));
    assertTrue(included.hasTable("myTable"));
    assertFalse(included.hasTable("otherTable"));
    assertFalse(included.hasUnrestrictedTableAccess());

    BasicAuthPrincipal excluded = new BasicAuthPrincipal("name", "token", Set.of(), tables, Set.of("read"));
    assertFalse(excluded.hasTable("myTable"));
    assertTrue(excluded.hasTable("otherTable"));
    assertFalse(excluded.hasUnrestrictedTableAccess());
  }

  @Test
  public void testNoExcludeTables() {
    // A principal without exclusions grants raw and typed names alike, through the allow-list and without one
    BasicAuthPrincipal unrestricted = new BasicAuthPrincipal("name", "token", Set.of(), Set.of(), Set.of("read"));
    assertTrue(unrestricted.hasTable("t"));
    assertTrue(unrestricted.hasTable("t_OFFLINE"));
    assertTrue(unrestricted.hasTable("t_REALTIME"));
    assertTrue(unrestricted.hasUnrestrictedTableAccess());

    BasicAuthPrincipal allowListed =
        new BasicAuthPrincipal("name", "token", Set.of("t", "t_OFFLINE"), Set.of(), Set.of("read"));
    assertTrue(allowListed.hasTable("t"));
    assertTrue(allowListed.hasTable("t_OFFLINE"));
    assertFalse(allowListed.hasTable("t_REALTIME"));
    assertFalse(allowListed.hasTable("other"));
  }

  @Test
  public void testExcludeTablesAlsoMatchTheRawName() {
    // excludeTables lists raw names: a type suffix must not bypass an excluded table
    BasicAuthPrincipal principal = new BasicAuthPrincipal("name", "token", Set.of(), Set.of("t"), Set.of("read"));
    assertFalse(principal.hasTable("t"));
    assertFalse(principal.hasTable("t_OFFLINE"));
    assertFalse(principal.hasTable("t_REALTIME"));
    assertTrue(principal.hasTable("other"));
    assertTrue(principal.hasTable("other_OFFLINE"));

    // excludeTables is documented as raw names: an entry written with a type suffix is a misconfiguration that the
    // principal does not widen. It matches only that spelling as given, so the raw name and the other type stay
    // accessible.
    BasicAuthPrincipal typedExclusion =
        new BasicAuthPrincipal("name", "token", Set.of(), Set.of("t_OFFLINE"), Set.of("read"));
    assertFalse(typedExclusion.hasTable("t_OFFLINE"));
    assertTrue(typedExclusion.hasTable("t"));
    assertTrue(typedExclusion.hasTable("t_REALTIME"));
  }

  @Test
  public void testAuthorizeDeleteRows() {
    BasicAuthPrincipal principal =
        new BasicAuthPrincipal("alice", "token", Set.of("myTable"), Set.of(), Set.of("read", "delete"));

    AuthorizationResult noPrincipal = BasicAuthPrincipalUtils.authorizeDeleteRows(null, "myTable", true);
    assertFalse(noPrincipal.hasAccess());
    assertEquals(noPrincipal.getFailureMessage(), "Missing or invalid credentials");

    AuthorizationResult noTable = BasicAuthPrincipalUtils.authorizeDeleteRows(principal, "myTable_OFFLINE", false);
    assertFalse(noTable.hasAccess());
    assertEquals(noTable.getFailureMessage(), "Principal: alice does not have access to table: myTable_OFFLINE");

    // The DELETE permission must be granted explicitly: a principal without a permission list, which hasPermission
    // grants everything, may not delete rows
    BasicAuthPrincipal noPermissionList =
        new BasicAuthPrincipal("bob", "token", Set.of("myTable"), Set.of(), Set.of());
    assertTrue(noPermissionList.hasPermission("delete"));
    AuthorizationResult noPermission = BasicAuthPrincipalUtils.authorizeDeleteRows(noPermissionList, "myTable", true);
    assertFalse(noPermission.hasAccess());
    assertEquals(noPermission.getFailureMessage(), "Principal: bob is not granted the DELETE permission");
    BasicAuthPrincipal readOnly =
        new BasicAuthPrincipal("carol", "token", Set.of("myTable"), Set.of(), Set.of("read"));
    assertEquals(BasicAuthPrincipalUtils.authorizeDeleteRows(readOnly, "myTable", true).getFailureMessage(),
        "Principal: carol is not granted the DELETE permission");

    AuthorizationResult granted = BasicAuthPrincipalUtils.authorizeDeleteRows(principal, "myTable", true);
    assertTrue(granted.hasAccess());
    assertEquals(granted.getFailureMessage(), "");
  }

  @Test
  public void testExtractBasicAuthPrincipals() {
    // Test basic configuration with multiple principals
    Map<String, Object> config = new HashMap<>();
    config.put("principals", "admin,user");
    config.put("principals.admin.password", "verysecret");
    config.put("principals.admin.permissions", Actions.Cluster.GET_ZNODE);
    config.put("principals.user.password", "secret");
    config.put("principals.user.tables", "lessImportantStuff,lesserImportantStuff,leastImportantStuff");
    config.put("principals.user.excludeTables", "excludedTable");
    config.put("principals.user.permissions", "read,write");
    config.put("principals.user.lessImportantStuff.rls", "cityID > 100,status = 'active'");
    config.put("principals.user.lesserImportantStuff.rls", "region = 'US'");

    PinotConfiguration configuration = new PinotConfiguration(config);
    List<BasicAuthPrincipal> principals = BasicAuthPrincipalUtils
        .extractBasicAuthPrincipals(configuration, "principals");

    assertEquals(principals.size(), 2);

    // Verify admin principal (should have no table restrictions)
    BasicAuthPrincipal adminPrincipal = principals.stream()
        .filter(p -> p.getName().equals("admin"))
        .findFirst()
        .orElse(null);
    assertNotNull(adminPrincipal);
    assertEquals(adminPrincipal.getName(), "admin");
    assertTrue(adminPrincipal.hasExplicitPermission(Actions.Cluster.GET_ZNODE));

    // Verify user principal
    BasicAuthPrincipal userPrincipal = principals.stream()
        .filter(p -> p.getName().equals("user"))
        .findFirst()
        .orElse(null);
    assertNotNull(userPrincipal);
    assertEquals(userPrincipal.getName(), "user");

    Set<String> expectedTables = ImmutableSet.of("lessImportantStuff", "lesserImportantStuff", "leastImportantStuff");
    expectedTables.forEach(tableName -> {
      assertTrue(userPrincipal.hasTable(tableName));
    });

    Set<String> expectedExcludeTables = ImmutableSet.of("excludedTable");
    expectedExcludeTables.forEach(tableName -> {
      assertFalse(userPrincipal.hasTable(tableName));
    });

    Set<String> expectedPermissions = ImmutableSet.of("read", "write");
    expectedPermissions.forEach(permission -> {
      assertTrue(userPrincipal.hasPermission(permission));
    });

    // Verify RLS filters

    List<String> lessImportantStuffFilters = userPrincipal.getRLSFilters("lessImportantStuff").get();
    assertNotNull(lessImportantStuffFilters);
    assertEquals(lessImportantStuffFilters.size(), 2);
    assertTrue(lessImportantStuffFilters.contains("cityID > 100"));
    assertTrue(lessImportantStuffFilters.contains("status = 'active'"));

    List<String> lesserImportantStuffFilters = userPrincipal.getRLSFilters("lesserImportantStuff").get();
    assertNotNull(lesserImportantStuffFilters);
    assertEquals(lesserImportantStuffFilters.size(), 1);
    assertTrue(lesserImportantStuffFilters.contains("region = 'US'"));

    // Verify no RLS filters for leastImportantStuff (not configured)
    assertTrue(userPrincipal.getRLSFilters("leastImportantStuff").isEmpty());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "must provide "
      + "principals")
  public void testExtractBasicAuthPrincipalsNoPrincipals() {
    Map<String, Object> config = new HashMap<>();
    PinotConfiguration configuration = new PinotConfiguration(config);
    BasicAuthPrincipalUtils.extractBasicAuthPrincipals(configuration, "principals");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "must provide a "
      + "password for.*")
  public void testExtractBasicAuthPrincipalsNoPassword() {
    Map<String, Object> config = new HashMap<>();
    config.put("principals", "admin");
    PinotConfiguration configuration = new PinotConfiguration(config);
    BasicAuthPrincipalUtils.extractBasicAuthPrincipals(configuration, "principals");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".* is not a valid name")
  public void testExtractBasicAuthPrincipalsBlankName() {
    Map<String, Object> config = new HashMap<>();
    config.put("principals", "admin, ,user");
    config.put("principals.admin.password", "secret");
    config.put("principals.user.password", "secret");
    PinotConfiguration configuration = new PinotConfiguration(config);
    BasicAuthPrincipalUtils.extractBasicAuthPrincipals(configuration, "principals");
  }
}
