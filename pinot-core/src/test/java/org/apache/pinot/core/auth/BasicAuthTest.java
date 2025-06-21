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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pinot.spi.env.PinotConfiguration;
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
        ImmutableSet.of("read"), Map.of("myTable", ImmutableList.of("cityID > 100")))
        .getRLSFilters("myTable"), Optional.of(ImmutableList.of("cityID > 100")));
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
}
