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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ZkBasicAuthTest {

  @Test
  public void testFineGrainedPermissionsAreAddedToZkPrincipal() {
    UserConfig legacyUser = new UserConfig("legacy", "password", ComponentType.CONTROLLER.name(),
        RoleType.ADMIN.name(), null, null, List.of(AccessType.READ));
    ZkBasicAuthPrincipal legacyPrincipal = BasicAuthPrincipalUtils.extractBasicAuthPrincipals(List.of(legacyUser))
        .get(0);
    Assert.assertTrue(legacyPrincipal.hasPermission("read"));
    Assert.assertTrue(legacyPrincipal.hasExplicitPermission("read"));
    Assert.assertFalse(legacyPrincipal.hasExplicitPermission(Actions.Cluster.GET_ZNODE));

    UserConfig fineGrainedUser = new UserConfig("fineGrained", "password", ComponentType.CONTROLLER.name(),
        RoleType.ADMIN.name(), null, null, null, List.of(Actions.Cluster.GET_ZNODE));
    ZkBasicAuthPrincipal fineGrainedPrincipal =
        BasicAuthPrincipalUtils.extractBasicAuthPrincipals(List.of(fineGrainedUser)).get(0);
    // The separate fine-grained permission must not turn the legacy empty-set wildcard into a table restriction.
    Assert.assertTrue(fineGrainedPrincipal.hasPermission("read"));
    Assert.assertTrue(fineGrainedPrincipal.hasExplicitPermission(Actions.Cluster.GET_ZNODE));
  }

  @Test
  public void testBasicAuthPrincipal() {
    Assert.assertTrue(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable"),
        Set.of(), ImmutableSet.of("READ")).hasTable("myTable"));
    Assert.assertTrue(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable", "myTable1"),
        Set.of(), ImmutableSet.of("Read")).hasTable("myTable1"));
    Assert.assertFalse(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable"),
        Set.of(), ImmutableSet.of("read")).hasTable("myTable1"));
    Assert.assertFalse(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable", "myTable1"),
        Set.of(), ImmutableSet.of("read")).hasTable("myTable2"));
    Assert.assertFalse(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable", "myTable1"),
        ImmutableSet.of("myTable3"), ImmutableSet.of("Read")).hasTable("myTable3"));
    Assert.assertTrue(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable", "myTable1"),
        ImmutableSet.of("myTable"), ImmutableSet.of("read")).hasTable("myTable1"));
    Assert.assertFalse(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), Set.of(),
        ImmutableSet.of("myTable"), ImmutableSet.of("read")).hasTable("myTable"));

    Assert.assertTrue(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable"),
        Set.of(), ImmutableSet.of("READ")).hasPermission("read"));
    Assert.assertTrue(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable"),
        Set.of(), ImmutableSet.of("Read")).hasPermission("READ"));
    Assert.assertTrue(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable"),
        Set.of(), ImmutableSet.of("read")).hasPermission("Read"));
    Assert.assertFalse(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable"),
        Set.of(), ImmutableSet.of("read")).hasPermission("write"));

    Assert.assertEquals(new ZkBasicAuthPrincipal("name", "token", "password",
        ComponentType.CONTROLLER.name(), RoleType.ADMIN.name(), ImmutableSet.of("myTable"),
        Set.of(), ImmutableSet.of("read"), Map.of("myTable", List.of("cityID > 100"))).getRLSFilters(
        "myTable").get(), List.of("cityID > 100"));
  }
}
