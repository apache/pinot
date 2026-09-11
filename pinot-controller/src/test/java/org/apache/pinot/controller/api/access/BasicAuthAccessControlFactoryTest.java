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

import java.util.List;
import java.util.Map;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies the shared authorization contract for static and ZooKeeper-backed controller BasicAuth.
public class BasicAuthAccessControlFactoryTest {
  private static final String ALLOWED_TABLE = "allowedTable";
  private static final String RESTRICTED_USER = "restricted";
  private static final String RESTRICTED_PASSWORD = "restrictedPassword";
  private static final String FULL_USER = "full";
  private static final String FULL_PASSWORD = "fullPassword";
  private static final String WILDCARD_USER = "wildcard";
  private static final String WILDCARD_PASSWORD = "wildcardPassword";
  private static final String SCOPED_ADMIN_USER = "scopedAdmin";
  private static final String SCOPED_ADMIN_PASSWORD = "scopedAdminPassword";
  private static final String EXCLUDED_USER = "excluded";
  private static final String EXCLUDED_PASSWORD = "excludedPassword";
  private static final String CLUSTER_READER_USER = "clusterReader";
  private static final String CLUSTER_READER_PASSWORD = "clusterReaderPassword";
  private static final String CORRUPT_USER = "corrupt";
  private static final String ENDPOINT_URL = "/cluster/configs";

  @DataProvider(name = "accessControls")
  public Object[][] accessControls()
      throws Exception {
    return new Object[][]{{createStaticAccessControl()}, {createZkAccessControl()}};
  }

  @Test(dataProvider = "accessControls")
  public void testClusterPermissions(AccessControl accessControl) {
    // A principal confined to a subset of tables holds no cluster-wide access, whichever permissions it was granted.
    for (Map.Entry<String, String> tableScopedUser : Map.of(RESTRICTED_USER, RESTRICTED_PASSWORD, SCOPED_ADMIN_USER,
        SCOPED_ADMIN_PASSWORD, EXCLUDED_USER, EXCLUDED_PASSWORD).entrySet()) {
      HttpHeaders tableScopedHeaders = headers(tableScopedUser.getKey(), tableScopedUser.getValue());
      for (AccessType accessType : AccessType.values()) {
        assertFalse(accessControl.hasAccess(accessType, tableScopedHeaders, ENDPOINT_URL),
            tableScopedUser.getKey() + " must not hold cluster " + accessType);
      }
    }

    // Table scope is necessary but not sufficient: an unrestricted principal is still held to its permissions.
    HttpHeaders clusterReaderHeaders = headers(CLUSTER_READER_USER, CLUSTER_READER_PASSWORD);
    assertTrue(accessControl.hasAccess(AccessType.READ, clusterReaderHeaders, ENDPOINT_URL));
    for (AccessType accessType : List.of(AccessType.CREATE, AccessType.UPDATE, AccessType.DELETE)) {
      assertFalse(accessControl.hasAccess(accessType, clusterReaderHeaders, ENDPOINT_URL),
          CLUSTER_READER_USER + " holds only READ, so it must not hold cluster " + accessType);
    }

    HttpHeaders fullHeaders = headers(FULL_USER, FULL_PASSWORD);
    for (AccessType accessType : AccessType.values()) {
      assertTrue(accessControl.hasAccess(accessType, fullHeaders, ENDPOINT_URL));
    }

    // Missing permissions have historically meant unrestricted access. Preserve that behavior for compatibility.
    HttpHeaders wildcardHeaders = headers(WILDCARD_USER, WILDCARD_PASSWORD);
    for (AccessType accessType : AccessType.values()) {
      assertTrue(accessControl.hasAccess(accessType, wildcardHeaders, ENDPOINT_URL));
    }
  }

  @Test(dataProvider = "accessControls")
  public void testClusterAuthenticationAndAuthorizationStatus(AccessControl accessControl) {
    for (HttpHeaders invalidHeaders : List.of(headers(), headers("unknown", "wrong"), malformedHeaders())) {
      NotAuthorizedException exception = expectThrows(NotAuthorizedException.class,
          () -> AccessControlUtils.validatePermission(null, AccessType.READ, invalidHeaders, ENDPOINT_URL,
              accessControl));
      assertEquals(exception.getResponse().getStatus(), Response.Status.UNAUTHORIZED.getStatusCode());
    }

    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> AccessControlUtils.validatePermission(null, AccessType.UPDATE,
            headers(RESTRICTED_USER, RESTRICTED_PASSWORD), ENDPOINT_URL, accessControl));
    assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    assertFalse(exception.getMessage().contains(RESTRICTED_PASSWORD));
    assertFalse(exception.getMessage().contains(
        BasicAuthTokenUtils.toBasicAuthToken(RESTRICTED_USER, RESTRICTED_PASSWORD)));
  }

  @Test(dataProvider = "accessControls")
  public void testFineGrainedAuthorizationRequiresAuthentication(AccessControl accessControl) {
    for (HttpHeaders invalidHeaders : List.of(headers(), headers("unknown", "wrong"), malformedHeaders())) {
      assertFalse(accessControl.hasAccess(invalidHeaders, TargetType.CLUSTER, null,
          Actions.Cluster.UPDATE_CLUSTER_CONFIG));
    }
    assertTrue(accessControl.hasAccess(headers(RESTRICTED_USER, RESTRICTED_PASSWORD), TargetType.CLUSTER, null,
        Actions.Cluster.UPDATE_CLUSTER_CONFIG));
  }

  @Test(dataProvider = "accessControls")
  public void testAuthenticationProbeRemainsNonThrowing(AccessControl accessControl) {
    assertFalse(accessControl.hasAccess(headers("unknown", "wrong"), TargetType.CLUSTER));
    assertTrue(accessControl.hasAccess(headers(FULL_USER, FULL_PASSWORD), TargetType.CLUSTER));

    // This probe backs the UI login gate, so it stays an authentication check: a table-scoped principal must still be
    // able to sign in and work with the tables it is scoped to. Cluster requests are denied by the checks above.
    assertTrue(accessControl.hasAccess(headers(RESTRICTED_USER, RESTRICTED_PASSWORD), TargetType.CLUSTER));
    assertTrue(accessControl.hasAccess(headers(SCOPED_ADMIN_USER, SCOPED_ADMIN_PASSWORD), TargetType.CLUSTER));
  }

  @Test(dataProvider = "accessControls")
  public void testBasicAuthWorkflow(AccessControl accessControl) {
    assertFalse(accessControl.protectAnnotatedOnly());
    assertEquals(accessControl.getAuthWorkflowInfo().getWorkflow(), AccessControl.WORKFLOW_BASIC);
  }

  @Test(dataProvider = "accessControls")
  public void testTablePermissionsAndTypedNameCompatibility(AccessControl accessControl) {
    HttpHeaders restrictedHeaders = headers(RESTRICTED_USER, RESTRICTED_PASSWORD);
    assertTrue(accessControl.hasAccess(ALLOWED_TABLE, AccessType.READ, restrictedHeaders, ENDPOINT_URL));
    assertFalse(accessControl.hasAccess(ALLOWED_TABLE, AccessType.CREATE, restrictedHeaders, ENDPOINT_URL));
    assertFalse(accessControl.hasAccess(ALLOWED_TABLE, AccessType.UPDATE, restrictedHeaders, ENDPOINT_URL));
    assertFalse(accessControl.hasAccess(ALLOWED_TABLE, AccessType.DELETE, restrictedHeaders, ENDPOINT_URL));
    assertFalse(accessControl.hasAccess("otherTable", AccessType.READ, restrictedHeaders, ENDPOINT_URL));
    assertTrue(accessControl.hasAccess(ALLOWED_TABLE + "_OFFLINE", AccessType.READ, restrictedHeaders, ENDPOINT_URL));

    HttpHeaders fullHeaders = headers(FULL_USER, FULL_PASSWORD);
    for (AccessType accessType : AccessType.values()) {
      assertTrue(accessControl.hasAccess(ALLOWED_TABLE, accessType, fullHeaders, ENDPOINT_URL));
    }

    // An exclude-list principal loses cluster access but keeps every table it was not excluded from.
    HttpHeaders excludedHeaders = headers(EXCLUDED_USER, EXCLUDED_PASSWORD);
    assertTrue(accessControl.hasAccess(ALLOWED_TABLE, AccessType.READ, excludedHeaders, ENDPOINT_URL));
    assertFalse(accessControl.hasAccess("otherTable", AccessType.READ, excludedHeaders, ENDPOINT_URL));
  }

  @Test(dataProvider = "accessControls")
  public void testTableAuthenticationAndAuthorizationStatus(AccessControl accessControl) {
    for (HttpHeaders invalidHeaders : List.of(headers(), headers("unknown", "wrong"), malformedHeaders())) {
      NotAuthorizedException exception = expectThrows(NotAuthorizedException.class,
          () -> AccessControlUtils.validatePermission(ALLOWED_TABLE, AccessType.READ, invalidHeaders, ENDPOINT_URL,
              accessControl));
      assertEquals(exception.getResponse().getStatus(), Response.Status.UNAUTHORIZED.getStatusCode());
    }

    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> AccessControlUtils.validatePermission("otherTable", AccessType.READ,
            headers(RESTRICTED_USER, RESTRICTED_PASSWORD), ENDPOINT_URL, accessControl));
    assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    assertFalse(exception.getMessage().contains(RESTRICTED_PASSWORD));
    assertFalse(exception.getMessage().contains(
        BasicAuthTokenUtils.toBasicAuthToken(RESTRICTED_USER, RESTRICTED_PASSWORD)));
  }

  /// Regression test for [issue 14595](https://github.com/apache/pinot/issues/14595): a principal scoped to a single
  /// table held every permission on cluster-level endpoints, letting it delete resources such as other users. Table
  /// scope must be honored on endpoints that name no table, while the principal keeps access to its own table.
  @Test(dataProvider = "accessControls")
  public void testTableScopedPrincipalCannotReachClusterEndpoints(AccessControl accessControl) {
    HttpHeaders scopedAdminHeaders = headers(SCOPED_ADMIN_USER, SCOPED_ADMIN_PASSWORD);

    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> AccessControlUtils.validatePermission(null, AccessType.DELETE, scopedAdminHeaders, "/users/admin",
            accessControl));
    assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());

    // The same principal still holds every permission on the table it is scoped to.
    for (AccessType accessType : AccessType.values()) {
      assertTrue(accessControl.hasAccess(ALLOWED_TABLE, accessType, scopedAdminHeaders, ENDPOINT_URL));
    }
    assertFalse(accessControl.hasAccess("otherTable", AccessType.DELETE, scopedAdminHeaders, ENDPOINT_URL));

    // An unrestricted principal with the same permission is still authorized cluster-wide.
    AccessControlUtils.validatePermission(null, AccessType.DELETE, headers(FULL_USER, FULL_PASSWORD), "/users/admin",
        accessControl);

    // Omitting the table name must not sidestep the scope rule via the table overload. An exclude-list principal is
    // the case that slips through a plain hasTable(null) check, since it has no allow-list to fail against.
    for (String tableScopedUser : List.of(SCOPED_ADMIN_USER, EXCLUDED_USER)) {
      HttpHeaders tableScopedHeaders = headers(tableScopedUser, tableScopedUser + "Password");
      assertFalse(accessControl.hasAccess(null, AccessType.DELETE, tableScopedHeaders, "/users/admin"),
          tableScopedUser + " must not be authorized for a request that names no table");
    }
  }

  @Test
  public void testMalformedCachedZkPrincipalFailsClosed()
      throws Exception {
    AccessControl accessControl = createZkAccessControl();
    String suppliedPassword = "notTheCachedPassword";
    NotAuthorizedException exception = expectThrows(NotAuthorizedException.class,
        () -> AccessControlUtils.validatePermission(null, AccessType.READ,
            headers(CORRUPT_USER, suppliedPassword), ENDPOINT_URL, accessControl));
    assertEquals(exception.getResponse().getStatus(), Response.Status.UNAUTHORIZED.getStatusCode());
    assertFalse(exception.getMessage().contains(CORRUPT_USER));
    assertFalse(exception.getMessage().contains(suppliedPassword));
  }

  private static AccessControl createStaticAccessControl() {
    Map<String, Object> properties = Map.ofEntries(
        Map.entry("controller.admin.access.control.principals",
            "restricted,full,wildcard,scopedAdmin,excluded,clusterReader"),
        Map.entry("controller.admin.access.control.principals.restricted.password", RESTRICTED_PASSWORD),
        Map.entry("controller.admin.access.control.principals.restricted.tables", ALLOWED_TABLE),
        Map.entry("controller.admin.access.control.principals.restricted.permissions", "read"),
        Map.entry("controller.admin.access.control.principals.full.password", FULL_PASSWORD),
        Map.entry("controller.admin.access.control.principals.full.permissions", "create,read,update,delete"),
        Map.entry("controller.admin.access.control.principals.wildcard.password", WILDCARD_PASSWORD),
        Map.entry("controller.admin.access.control.principals.scopedAdmin.password", SCOPED_ADMIN_PASSWORD),
        Map.entry("controller.admin.access.control.principals.scopedAdmin.tables", ALLOWED_TABLE),
        Map.entry("controller.admin.access.control.principals.scopedAdmin.permissions", "create,read,update,delete"),
        Map.entry("controller.admin.access.control.principals.excluded.password", EXCLUDED_PASSWORD),
        Map.entry("controller.admin.access.control.principals.excluded.excludeTables", "otherTable"),
        Map.entry("controller.admin.access.control.principals.clusterReader.password", CLUSTER_READER_PASSWORD),
        Map.entry("controller.admin.access.control.principals.clusterReader.permissions", "read"));
    BasicAuthAccessControlFactory factory = new BasicAuthAccessControlFactory();
    factory.init(new PinotConfiguration(properties));
    return factory.create();
  }

  private static AccessControl createZkAccessControl()
      throws Exception {
    List<UserConfig> users = List.of(
        user(RESTRICTED_USER, RESTRICTED_PASSWORD, List.of(ALLOWED_TABLE),
            List.of(org.apache.pinot.spi.config.user.AccessType.READ)),
        user(FULL_USER, FULL_PASSWORD, null,
            List.of(org.apache.pinot.spi.config.user.AccessType.values())),
        user(WILDCARD_USER, WILDCARD_PASSWORD, null, null),
        user(SCOPED_ADMIN_USER, SCOPED_ADMIN_PASSWORD, List.of(ALLOWED_TABLE),
            List.of(org.apache.pinot.spi.config.user.AccessType.values())),
        user(EXCLUDED_USER, EXCLUDED_PASSWORD, null, List.of("otherTable"), null),
        user(CLUSTER_READER_USER, CLUSTER_READER_PASSWORD, null,
            List.of(org.apache.pinot.spi.config.user.AccessType.READ)),
        new UserConfig(CORRUPT_USER, " ", ComponentType.CONTROLLER.name(), RoleType.USER.name(), null, null, null));
    List<String> userNames = users.stream().map(UserConfig::getUsernameWithComponent).toList();
    List<String> userPaths = userNames.stream().map(name -> "/CONFIGS/USER/" + name).toList();
    List<ZNRecord> userRecords = users.stream().map(user -> {
      try {
        return AccessControlUserConfigUtils.toZNRecord(user);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).toList();

    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    Mockito.when(propertyStore.getChildNames("/CONFIGS/USER", AccessOption.PERSISTENT)).thenReturn(userNames);
    Mockito.when(propertyStore.get(Mockito.eq(userPaths), Mockito.isNull(), Mockito.eq(AccessOption.PERSISTENT),
        Mockito.eq(false))).thenReturn(userRecords);

    PinotHelixResourceManager resourceManager = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(resourceManager.getPropertyStore()).thenReturn(propertyStore);

    ZkBasicAuthAccessControlFactory factory = new ZkBasicAuthAccessControlFactory();
    factory.init(new ControllerConf(), resourceManager);
    return factory.create();
  }

  private static UserConfig user(String username, String password, List<String> tables,
      List<org.apache.pinot.spi.config.user.AccessType> permissions) {
    return user(username, password, tables, null, permissions);
  }

  private static UserConfig user(String username, String password, List<String> tables, List<String> excludeTables,
      List<org.apache.pinot.spi.config.user.AccessType> permissions) {
    return new UserConfig(username, BcryptUtils.encrypt(password), ComponentType.CONTROLLER.name(),
        RoleType.USER.name(), tables, excludeTables, permissions);
  }

  private static HttpHeaders headers(String username, String password) {
    return headers(BasicAuthTokenUtils.toBasicAuthToken(username, password));
  }

  private static HttpHeaders headers() {
    return headers((String) null);
  }

  private static HttpHeaders malformedHeaders() {
    return headers("Basic not-base64");
  }

  private static HttpHeaders headers(String authorization) {
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    Mockito.when(headers.getRequestHeader(HttpHeaders.AUTHORIZATION))
        .thenReturn(authorization == null ? null : List.of(authorization));
    return headers;
  }
}
