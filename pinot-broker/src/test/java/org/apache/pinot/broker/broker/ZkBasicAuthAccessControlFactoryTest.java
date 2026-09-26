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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.common.utils.config.AccessControlUserConfigUtils;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ZkBasicAuthAccessControlFactoryTest {
  private static final String TOKEN_DELETER = "Basic ZGVsZXRlcjpkZWxzZWNyZXQ="; // deleter:delsecret
  private static final String TOKEN_READER = "Basic cmVhZGVyOnJlYWRzZWNyZXQ="; // reader:readsecret
  private static final String TOKEN_USER = "Basic dXNlcjpzZWNyZXQ="; // user:secret
  private static final String TOKEN_WRONG_PASSWORD = "Basic ZGVsZXRlcjp3cm9uZw=="; // deleter:wrong

  private AccessControl _accessControl;

  @BeforeClass
  @SuppressWarnings("unchecked")
  public void setUp()
      throws Exception {
    List<ZNRecord> userRecords = new ArrayList<>();
    userRecords.add(userRecord("deleter", "delsecret", List.of(AccessType.READ, AccessType.DELETE)));
    userRecords.add(userRecord("reader", "readsecret", List.of(AccessType.READ)));
    userRecords.add(userRecord("user", "secret", null));
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(propertyStore.getChildNames(anyString(), anyInt()))
        .thenReturn(List.of("deleter_BROKER", "reader_BROKER", "user_BROKER"));
    when(propertyStore.get(anyList(), any(), anyInt(), anyBoolean())).thenReturn(userRecords);

    AccessControlFactory factory = new ZkBasicAuthAccessControlFactory();
    factory.init(new PinotConfiguration(), propertyStore);
    _accessControl = factory.create();
  }

  @Test
  public void testDeleteRowsIsDenied() {
    // The controller UI saves every permission for a user stored without any, so a DELETE permission does not show
    // that it was granted on purpose: even a user granted it does not delete rows through the broker
    for (String token : new String[]{TOKEN_DELETER, TOKEN_READER, TOKEN_USER, TOKEN_WRONG_PASSWORD, null}) {
      for (String tableName : new String[]{"lessImportantStuff", "lessImportantStuff_OFFLINE", "veryImportantStuff"}) {
        assertDeleteRows(token, tableName, false, "The access control of the broker does not allow deleting rows");
      }
    }
  }

  @Test
  public void testQueriesAreAuthorized() {
    // The users still query the tables they have access to
    assertTrue(_accessControl.authorize(identity(TOKEN_DELETER), Set.of("lessImportantStuff")).hasAccess());
    assertFalse(_accessControl.authorize(identity(TOKEN_READER), Set.of("veryImportantStuff")).hasAccess());
  }

  private void assertDeleteRows(@Nullable String token, String tableName, boolean expectedAccess,
      String expectedMessage) {
    AuthorizationResult result = _accessControl.authorizeDeleteRows(identity(token), null, tableName);
    assertEquals(result.hasAccess(), expectedAccess, token + " on " + tableName);
    assertEquals(result.getFailureMessage(), expectedMessage);
  }

  private static HttpRequesterIdentity identity(@Nullable String token) {
    Multimap<String, String> headers = ArrayListMultimap.create();
    if (token != null) {
      headers.put(AccessControlFactory.HEADER_AUTHORIZATION, token);
    }
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);
    return identity;
  }

  private static ZNRecord userRecord(String name, String password, @Nullable List<AccessType> permissions)
      throws Exception {
    return AccessControlUserConfigUtils.toZNRecord(new UserConfig(name, BcryptUtils.encrypt(password), "BROKER",
        "USER", List.of("lessImportantStuff"), null, permissions));
  }
}
