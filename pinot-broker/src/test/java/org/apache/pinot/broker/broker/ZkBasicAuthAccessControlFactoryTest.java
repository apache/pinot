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
package org.apache.pinot.broker.broker;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.NotAuthorizedException;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the broker access control backed by the user configs in ZooKeeper, with the users served from a mocked
/// property store.
public class ZkBasicAuthAccessControlFactoryTest {
  private static final String USER_CONFIG_PARENT_PATH = "/CONFIGS/USER";
  private static final String USER_CONFIG_PATH_PREFIX = "/CONFIGS/USER/";

  private AccessControl _accessControl;

  @BeforeClass
  public void setUp()
      throws Exception {
    List<UserConfig> users = List.of(
        brokerUser("deleter", "delsecret", List.of("orders"), null, List.of(AccessType.READ, AccessType.DELETE)),
        brokerUser("reader", "readsecret", List.of("orders"), null, List.of(AccessType.READ)),
        brokerUser("unrestricted", "unrsecret", null, null, null),
        brokerUser("excluder", "exclsecret", null, List.of("billing"), List.of(AccessType.READ, AccessType.DELETE)),
        // Users of another component are not broker users
        new UserConfig("controllerDeleter", BcryptUtils.encrypt("ctrlsecret"), ComponentType.CONTROLLER.name(),
            RoleType.USER.name(), List.of("orders"), null, List.of(AccessType.READ, AccessType.DELETE)));
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    List<String> usernamesWithComponent =
        users.stream().map(UserConfig::getUsernameWithComponent).collect(Collectors.toList());
    List<String> paths = usernamesWithComponent.stream().map(username -> USER_CONFIG_PATH_PREFIX + username)
        .collect(Collectors.toList());
    List<ZNRecord> userRecords = users.stream().map(userConfig -> {
      try {
        return AccessControlUserConfigUtils.toZNRecord(userConfig);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    when(propertyStore.getChildNames(USER_CONFIG_PARENT_PATH, AccessOption.PERSISTENT))
        .thenReturn(usernamesWithComponent);
    when(propertyStore.get(eq(paths), isNull(), eq(AccessOption.PERSISTENT), eq(false))).thenReturn(userRecords);

    ZkBasicAuthAccessControlFactory factory = new ZkBasicAuthAccessControlFactory();
    factory.init(new PinotConfiguration(), propertyStore);
    _accessControl = factory.create();
  }

  @Test
  public void testQueryAuthorization() {
    assertTrue(_accessControl.authorize(identity("deleter", "delsecret"), Set.of("orders")).hasAccess());
    assertTrue(_accessControl.authorize(identity("deleter", "delsecret"), Set.of("orders_OFFLINE")).hasAccess());
    assertFalse(_accessControl.authorize(identity("deleter", "delsecret"), Set.of("other")).hasAccess());
    // The first step does not authenticate the caller, the table check does, even without a table: the broker relies
    // on it to reject an unauthenticated DELETE before looking up its table
    assertTrue(_accessControl.authorize(identity(null, null)).hasAccess());
    expectThrows(NotAuthorizedException.class, () -> _accessControl.authorize(identity(null, null), Set.of()));
    expectThrows(NotAuthorizedException.class,
        () -> _accessControl.authorize(identity("deleter", "wrongsecret"), Set.of("orders")));
  }

  @Test
  public void testDeleteRowsRequiresTheDeletePermission() {
    assertDeleteRows("deleter", "delsecret", "orders", true, "");
    assertDeleteRows("deleter", "delsecret", "other", false,
        "Principal: deleter does not have access to table: other");
    assertDeleteRows("reader", "readsecret", "orders", false,
        "Principal: reader is not granted the DELETE permission");
    // A user without permissions queries every table, but only deletes rows when granted the permission
    assertDeleteRows("unrestricted", "unrsecret", "orders", false,
        "Principal: unrestricted is not granted the DELETE permission");
    // Credentials of a user of another component, or wrong ones, are not broker credentials
    assertDeleteRows("controllerDeleter", "ctrlsecret", "orders", false, "Missing or invalid credentials");
    assertDeleteRows("deleter", "wrongsecret", "orders", false, "Missing or invalid credentials");
    assertDeleteRows(null, null, "orders", false, "Missing or invalid credentials");
  }

  @Test
  public void testDeleteRowsChecksTheRawTableName() {
    // Tables are matched by raw name, as for queries
    for (String tableName : List.of("orders_OFFLINE", "orders_REALTIME")) {
      assertDeleteRows("deleter", "delsecret", tableName, true, "");
    }
    for (String tableName : List.of("billing", "billing_OFFLINE", "billing_REALTIME")) {
      assertDeleteRows("excluder", "exclsecret", tableName, false,
          "Principal: excluder does not have access to table: " + tableName);
    }
    assertDeleteRows("excluder", "exclsecret", "other_OFFLINE", true, "");
  }

  private void assertDeleteRows(@Nullable String username, @Nullable String password, String tableName,
      boolean expectedAccess, String expectedMessage) {
    AuthorizationResult result = _accessControl.authorizeDeleteRows(identity(username, password), null, tableName);
    assertEquals(result.hasAccess(), expectedAccess, username + " on " + tableName);
    assertEquals(result.getFailureMessage(), expectedMessage);
  }

  private static UserConfig brokerUser(String username, String password, @Nullable List<String> tables,
      @Nullable List<String> excludeTables, @Nullable List<AccessType> permissions) {
    return new UserConfig(username, BcryptUtils.encrypt(password), ComponentType.BROKER.name(), RoleType.USER.name(),
        tables, excludeTables, permissions);
  }

  /// Identity with the basic auth credentials, or without an `Authorization` header when the username is null.
  private static HttpRequesterIdentity identity(@Nullable String username, @Nullable String password) {
    Multimap<String, String> headers = ArrayListMultimap.create();
    if (username != null) {
      headers.put("authorization", BasicAuthTokenUtils.toBasicAuthToken(username, password));
    }
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);
    return identity;
  }
}
