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
package org.apache.pinot.server.access;

import java.util.List;
import java.util.Map;
import javax.ws.rs.NotAuthorizedException;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/// Tests the ZK-backed Server administrative authorization contract.
public class ZkBasicAuthAccessFactoryTest {
  @Test
  public void testAdminAccessRequiresServerAdminIdentity()
      throws Exception {
    UserConfig admin = user("admin", "admin-secret", RoleType.ADMIN);
    UserConfig reader = user("reader", "reader-secret", RoleType.USER);

    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.getChildNames(anyString(), anyInt())).thenReturn(List.of("admin_SERVER", "reader_SERVER"));
    when(propertyStore.get(anyList(), isNull(), anyInt(), org.mockito.ArgumentMatchers.eq(false)))
        .thenReturn(List.of(AccessControlUserConfigUtils.toZNRecord(admin),
            AccessControlUserConfigUtils.toZNRecord(reader)));
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

    ZkBasicAuthAccessFactory factory = new ZkBasicAuthAccessFactory();
    factory.init(new PinotConfiguration(), helixManager);
    AccessControl accessControl = factory.create();

    Assert.assertTrue(accessControl.authorizeAdminAccess(identity("admin", "admin-secret")).hasAccess());
    Assert.assertFalse(accessControl.authorizeAdminAccess(identity("reader", "reader-secret")).hasAccess());
    Assert.expectThrows(NotAuthorizedException.class,
        () -> accessControl.authorizeAdminAccess(identity("admin", "wrong-secret")));
    Assert.expectThrows(NotAuthorizedException.class,
        () -> accessControl.authorizeAdminAccess(new GrpcRequesterIdentity(Map.of())));
  }

  private static UserConfig user(String name, String password, RoleType roleType) {
    return new UserConfig(name, BcryptUtils.encrypt(password), ComponentType.SERVER.name(), roleType.name(),
        List.of("testTable"), List.of(), List.of());
  }

  private static GrpcRequesterIdentity identity(String name, String password) {
    return new GrpcRequesterIdentity(
        Map.of("authorization", BasicAuthTokenUtils.toBasicAuthToken(name, password)));
  }
}
