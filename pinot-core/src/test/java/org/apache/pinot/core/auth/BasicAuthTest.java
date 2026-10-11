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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    assertEquals(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Set.of(),
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
