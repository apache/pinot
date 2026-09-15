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
package org.apache.pinot.common.config.provider;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.spi.config.user.ComponentType;
import org.apache.pinot.spi.config.user.RoleType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class AccessControlUserCacheTest {
  @Test
  public void testControllerUserLookup() throws Exception {
    UserConfig userConfig = new UserConfig("controllerUser", "password", ComponentType.CONTROLLER.name(),
        RoleType.USER.name(), List.of("table"), null, List.of());
    UserConfig whitespaceUserConfig = new UserConfig(" whitespaceUser ", "password",
        ComponentType.CONTROLLER.name(), RoleType.USER.name(), List.of("otherTable"), null, List.of());
    UserConfig shadowUserConfig = new UserConfig(" controllerUser ", "shadowPassword",
        ComponentType.CONTROLLER.name(), RoleType.USER.name(), List.of("shadowTable"), null, List.of());
    AccessControlUserCache cache = createCache(List.of(userConfig, whitespaceUserConfig, shadowUserConfig));

    UserConfig cachedUser = cache.getControllerUserConfigForUsername("controllerUser");
    assertEquals(cachedUser.getUserName(), "controllerUser");
    assertEquals(cachedUser.getTables(), List.of("table"));
    UserConfig whitespaceCachedUser = cache.getControllerUserConfigForUsername("whitespaceUser");
    assertEquals(whitespaceCachedUser.getUserName(), " whitespaceUser ");
    assertEquals(whitespaceCachedUser.getTables(), List.of("otherTable"));
    assertNull(cache.getControllerUserConfigForUsername(" whitespaceUser "));
    assertNull(cache.getControllerUserConfigForUsername("missing"));
  }

  @Test
  public void testAmbiguousTrimmedControllerUserLookupFailsClosed()
      throws Exception {
    UserConfig whitespaceUserConfig = new UserConfig(" ambiguousUser ", "password",
        ComponentType.CONTROLLER.name(), RoleType.USER.name(), null, null, List.of());
    UserConfig duplicateWhitespaceUserConfig = new UserConfig("  ambiguousUser  ", "otherPassword",
        ComponentType.CONTROLLER.name(), RoleType.USER.name(), null, null, List.of());
    AccessControlUserCache cache = createCache(List.of(whitespaceUserConfig, duplicateWhitespaceUserConfig));

    assertNull(cache.getControllerUserConfigForUsername("ambiguousUser"));
  }

  private static AccessControlUserCache createCache(List<UserConfig> userConfigs)
      throws Exception {
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    List<String> usernamesWithComponent =
        userConfigs.stream().map(UserConfig::getUsernameWithComponent).collect(Collectors.toList());
    List<String> paths = usernamesWithComponent.stream().map(username -> "/CONFIGS/USER/" + username)
        .collect(Collectors.toList());
    List<ZNRecord> userRecords = userConfigs.stream().map(userConfig -> {
      try {
        return AccessControlUserConfigUtils.toZNRecord(userConfig);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    when(propertyStore.getChildNames("/CONFIGS/USER", AccessOption.PERSISTENT)).thenReturn(usernamesWithComponent);
    when(propertyStore.get(eq(paths), isNull(), eq(AccessOption.PERSISTENT), eq(false))).thenReturn(userRecords);
    return new AccessControlUserCache(propertyStore);
  }
}
