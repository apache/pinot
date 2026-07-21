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
package org.apache.pinot.controller.api.access;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests controller BasicAuth endpoint-level access checks for static and ZooKeeper-backed principal sources.
 */
public class BasicAuthAccessControlFactoryTest {
  private static final String USER_CONFIG_PARENT_PATH = "/CONFIGS/USER";
  private static final String USER_CONFIG_PATH_PREFIX = USER_CONFIG_PARENT_PATH + "/";

  @Test
  public void testBasicAuthRequiresFullTableScopeForNonTableUpdate() {
    Map<String, Object> config = new HashMap<>();
    config.put("controller.admin.access.control.principals", "admin,tableUser,reader,excludedUser");
    config.put("controller.admin.access.control.principals.admin.password", "verysecret");
    config.put("controller.admin.access.control.principals.tableUser.password", "secret");
    config.put("controller.admin.access.control.principals.tableUser.tables", "allowedTable");
    config.put("controller.admin.access.control.principals.tableUser.permissions", "update");
    config.put("controller.admin.access.control.principals.reader.password", "readsecret");
    config.put("controller.admin.access.control.principals.reader.permissions", "read");
    config.put("controller.admin.access.control.principals.excludedUser.password", "excludedsecret");
    config.put("controller.admin.access.control.principals.excludedUser.excludeTables", "blockedTable");
    config.put("controller.admin.access.control.principals.excludedUser.permissions", "update");

    BasicAuthAccessControlFactory factory = new BasicAuthAccessControlFactory();
    factory.init(new PinotConfiguration(config));

    AccessControl accessControl = factory.create();
    HttpHeaders tableUserHeaders = headersFor("tableUser", "secret");

    assertTrue(accessControl.hasAccess("allowedTable", AccessType.UPDATE, tableUserHeaders, "/tables/allowedTable"));
    assertFalse(accessControl.hasAccess(AccessType.UPDATE, tableUserHeaders, "/cluster/configs"));
    assertFalse(accessControl.hasAccess(AccessType.UPDATE, headersFor("reader", "readsecret"), "/cluster/configs"));
    assertFalse(
        accessControl.hasAccess(AccessType.UPDATE, headersFor("excludedUser", "excludedsecret"), "/cluster/configs"));
    assertTrue(accessControl.hasAccess(AccessType.UPDATE, headersFor("admin", "verysecret"), "/cluster/configs"));
    assertTrue(accessControl.hasAccess(tableUserHeaders, TargetType.TABLE, "allowedTable_OFFLINE",
        Actions.Table.UPDATE_TABLE_CONFIG));
    assertFalse(accessControl.hasAccess(tableUserHeaders, TargetType.TABLE, "blockedTable_OFFLINE",
        Actions.Table.UPDATE_TABLE_CONFIG));
    assertFalse(accessControl.hasAccess(tableUserHeaders, TargetType.CLUSTER, null,
        Actions.Cluster.UPDATE_CLUSTER_CONFIG));
    assertFalse(accessControl.hasAccess(headersFor("reader", "readsecret"), TargetType.CLUSTER, null,
        Actions.Cluster.UPDATE_CLUSTER_CONFIG));
    assertFalse(accessControl.hasAccess(tableUserHeaders, TargetType.CLUSTER));
    assertFalse(accessControl.hasAccess(headersFor("excludedUser", "excludedsecret"), TargetType.CLUSTER));
    assertTrue(accessControl.hasAccess(headersFor("reader", "readsecret"), TargetType.CLUSTER));
    assertTrue(accessControl.hasAccess(headersFor("admin", "verysecret"), TargetType.CLUSTER, null,
        Actions.Cluster.UPDATE_CLUSTER_CONFIG));
  }

  @Test
  public void testZkBasicAuthRequiresFullTableScopeForNonTableUpdate()
      throws Exception {
    AccessControl accessControl = createZkAccessControl(List.of(
        userRecord("admin", "verysecret", null, null),
        userRecord("tableUser", "secret", List.of("allowedTable"),
            List.of(org.apache.pinot.spi.config.user.AccessType.UPDATE)),
        userRecord("reader", "readsecret", null, List.of(org.apache.pinot.spi.config.user.AccessType.READ)),
        userRecord("excludedUser", "excludedsecret", null, List.of("blockedTable"),
            List.of(org.apache.pinot.spi.config.user.AccessType.UPDATE))));
    HttpHeaders tableUserHeaders = headersFor("tableUser", "secret");

    assertTrue(accessControl.hasAccess("allowedTable", AccessType.UPDATE, tableUserHeaders, "/tables/allowedTable"));
    assertFalse(accessControl.hasAccess(AccessType.UPDATE, tableUserHeaders, "/cluster/configs"));
    assertFalse(accessControl.hasAccess(AccessType.UPDATE, headersFor("reader", "readsecret"), "/cluster/configs"));
    assertFalse(
        accessControl.hasAccess(AccessType.UPDATE, headersFor("excludedUser", "excludedsecret"), "/cluster/configs"));
    assertTrue(accessControl.hasAccess(AccessType.UPDATE, headersFor("admin", "verysecret"), "/cluster/configs"));
    assertTrue(accessControl.hasAccess(tableUserHeaders, TargetType.TABLE, "allowedTable_OFFLINE",
        Actions.Table.UPDATE_TABLE_CONFIG));
    assertFalse(accessControl.hasAccess(tableUserHeaders, TargetType.TABLE, "blockedTable_OFFLINE",
        Actions.Table.UPDATE_TABLE_CONFIG));
    assertFalse(accessControl.hasAccess(tableUserHeaders, TargetType.CLUSTER, null,
        Actions.Cluster.UPDATE_CLUSTER_CONFIG));
    assertFalse(accessControl.hasAccess(headersFor("reader", "readsecret"), TargetType.CLUSTER, null,
        Actions.Cluster.UPDATE_CLUSTER_CONFIG));
    assertFalse(accessControl.hasAccess(tableUserHeaders, TargetType.CLUSTER));
    assertFalse(accessControl.hasAccess(headersFor("excludedUser", "excludedsecret"), TargetType.CLUSTER));
    assertTrue(accessControl.hasAccess(headersFor("reader", "readsecret"), TargetType.CLUSTER));
    assertTrue(accessControl.hasAccess(headersFor("admin", "verysecret"), TargetType.CLUSTER, null,
        Actions.Cluster.UPDATE_CLUSTER_CONFIG));
  }

  private static HttpHeaders headersFor(String username, String password) {
    HttpHeaders httpHeaders = Mockito.mock(HttpHeaders.class);
    Mockito.when(httpHeaders.getRequestHeader("Authorization"))
        .thenReturn(List.of(BasicAuthTokenUtils.toBasicAuthToken(username, password)));
    return httpHeaders;
  }

  private static ZNRecord userRecord(String username, String password, List<String> tables,
      List<org.apache.pinot.spi.config.user.AccessType> permissions)
      throws Exception {
    return userRecord(username, password, tables, null, permissions);
  }

  private static ZNRecord userRecord(String username, String password, List<String> tables, List<String> excludeTables,
      List<org.apache.pinot.spi.config.user.AccessType> permissions)
      throws Exception {
    UserConfig userConfig = new UserConfig(username, BcryptUtils.encrypt(password, 4), ComponentType.CONTROLLER.name(),
        RoleType.USER.name(), tables, excludeTables, permissions);
    return AccessControlUserConfigUtils.toZNRecord(userConfig);
  }

  @SuppressWarnings("unchecked")
  private static AccessControl createZkAccessControl(List<ZNRecord> userRecords)
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    List<String> userNames = userRecords.stream()
        .map(record -> record.getSimpleField(UserConfig.USERNAME_KEY) + "_" + ComponentType.CONTROLLER)
        .toList();
    List<String> userPaths = userNames.stream().map(userName -> USER_CONFIG_PATH_PREFIX + userName).toList();
    Mockito.when(propertyStore.getChildNames(USER_CONFIG_PARENT_PATH, AccessOption.PERSISTENT)).thenReturn(userNames);
    Mockito.when(propertyStore.get(Mockito.eq(userPaths), Mockito.isNull(), Mockito.eq(AccessOption.PERSISTENT),
        Mockito.eq(false))).thenReturn(userRecords);

    PinotHelixResourceManager pinotHelixResourceManager = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(pinotHelixResourceManager.getPropertyStore()).thenReturn(propertyStore);

    ZkBasicAuthAccessControlFactory factory = new ZkBasicAuthAccessControlFactory();
    factory.init(new ControllerConf(), pinotHelixResourceManager);
    return factory.create();
  }
}
