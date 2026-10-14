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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.WebApplicationException;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class BasicAuthAccessControlTest {
  private static final String TOKEN_USER = "Basic dXNlcjpzZWNyZXQ"; // user:secret
  private static final String TOKEN_ADMIN = "Basic YWRtaW46dmVyeXNlY3JldA"; // admin:verysecret
  private static final String TOKEN_OPERATOR = "Basic b3BlcmF0b3I6b3BzZWNyZXQ="; // operator:opsecret

  private static final String HEADER_AUTHORIZATION = "authorization";

  private AccessControl _accessControl;

  Set<String> _tableNames;

  @BeforeClass
  public void setup() {
    Map<String, Object> config = new HashMap<>();
    config.put("principals", "admin,user,operator");
    config.put("principals.admin.password", "verysecret");
    config.put("principals.user.password", "secret");
    config.put("principals.operator.password", "opsecret");
    config.put("principals.operator.permissions", "READ,ADMIN");
    config.put("principals.user.tables", "lessImportantStuff,lesserImportantStuff,leastImportantStuff");

    _tableNames = new HashSet<>();
    _tableNames.add("lessImportantStuff");
    _tableNames.add("lesserImportantStuff");
    _tableNames.add("leastImportantStuff");

    AccessControlFactory factory = new BasicAuthAccessControlFactory();
    factory.init(new PinotConfiguration(config));

    _accessControl = factory.create();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNullEntity() {
    _accessControl.authorize(null, (BrokerRequest) null);
  }

  @Test
  public void testNullToken() {
    Multimap<String, String> headers = ArrayListMultimap.create();

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);

    try {
      _accessControl.authorize(identity, (BrokerRequest) null);
    } catch (WebApplicationException e) {
      Assert.assertEquals(e.getResponse().getStatus(), 401, "must return 401");
    }
  }

  @Test
  public void testAllow() {
    Multimap<String, String> headers = ArrayListMultimap.create();
    headers.put(HEADER_AUTHORIZATION, TOKEN_USER);

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);

    QuerySource source = new QuerySource();
    source.setTableName("lessImportantStuff");

    BrokerRequest request = new BrokerRequest();
    request.setQuerySource(source);

    Assert.assertTrue(_accessControl.authorize(identity, request).hasAccess());
    Assert.assertTrue(_accessControl.authorize(identity, _tableNames).hasAccess());
  }

  @Test
  public void testDeny() {
    Multimap<String, String> headers = ArrayListMultimap.create();
    headers.put(HEADER_AUTHORIZATION, TOKEN_USER);

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);

    QuerySource source = new QuerySource();
    source.setTableName("veryImportantStuff");

    BrokerRequest request = new BrokerRequest();
    request.setQuerySource(source);
    AuthorizationResult authorizationResult = _accessControl.authorize(identity, request);
    Assert.assertFalse(authorizationResult.hasAccess());
    Assert.assertEquals(authorizationResult.getFailureMessage(),
        "Authorization Failed for tables: [veryImportantStuff]");

    Set<String> tableNames = new HashSet<>();
    tableNames.add("veryImportantStuff");
    authorizationResult = _accessControl.authorize(identity, tableNames);
    Assert.assertFalse(authorizationResult.hasAccess());
    Assert.assertEquals(authorizationResult.getFailureMessage(),
        "Authorization Failed for tables: [veryImportantStuff]");
    tableNames.add("lessImportantStuff");
    authorizationResult = _accessControl.authorize(identity, tableNames);
    Assert.assertFalse(authorizationResult.hasAccess());
    Assert.assertEquals(authorizationResult.getFailureMessage(),
        "Authorization Failed for tables: [veryImportantStuff]");
    tableNames.add("lesserImportantStuff");
    authorizationResult = _accessControl.authorize(identity, tableNames);
    Assert.assertFalse(authorizationResult.hasAccess());
    Assert.assertEquals(authorizationResult.getFailureMessage(),
        "Authorization Failed for tables: [veryImportantStuff]");
  }

  @Test
  public void testAllowAll() {
    Multimap<String, String> headers = ArrayListMultimap.create();
    headers.put(HEADER_AUTHORIZATION, TOKEN_ADMIN);

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);

    QuerySource source = new QuerySource();
    source.setTableName("veryImportantStuff");

    BrokerRequest request = new BrokerRequest();
    request.setQuerySource(source);
    AuthorizationResult authorizationResult = _accessControl.authorize(identity, request);
    Assert.assertTrue(authorizationResult.hasAccess());
    Assert.assertEquals(authorizationResult.getFailureMessage(), "");

    Set<String> tableNames = new HashSet<>();
    tableNames.add("lessImportantStuff");
    tableNames.add("veryImportantStuff");
    tableNames.add("lesserImportantStuff");

    authorizationResult = _accessControl.authorize(identity, tableNames);
    Assert.assertTrue(authorizationResult.hasAccess());
    Assert.assertEquals(authorizationResult.getFailureMessage(), "");
  }

  @Test
  public void testAllowNonTable() {
    Multimap<String, String> headers = ArrayListMultimap.create();
    headers.put(HEADER_AUTHORIZATION, TOKEN_USER);

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);

    BrokerRequest request = new BrokerRequest();
    AuthorizationResult authorizationResult = _accessControl.authorize(identity, request);
    Assert.assertTrue(authorizationResult.hasAccess());

    Set<String> tableNames = new HashSet<>();
    authorizationResult = _accessControl.authorize(identity, tableNames);
    Assert.assertTrue(authorizationResult.hasAccess());
  }

  @Test
  public void testAdminAccessRequiresExplicitAdminPermission() {
    // Principals without configured permissions keep full query access, but must not implicitly gain admin access
    Assert.assertFalse(_accessControl.authorizeAdminAccess(identityWithToken(TOKEN_ADMIN)).hasAccess());
    Assert.assertFalse(_accessControl.authorizeAdminAccess(identityWithToken(TOKEN_USER)).hasAccess());
    Assert.assertTrue(_accessControl.authorizeAdminAccess(identityWithToken(TOKEN_OPERATOR)).hasAccess());
    Assert.expectThrows(NotAuthorizedException.class,
        () -> _accessControl.authorizeAdminAccess(identityWithToken("Basic dW5rbm93bjpwYXNzd29yZA")));
  }

  private static HttpRequesterIdentity identityWithToken(String token) {
    Multimap<String, String> headers = ArrayListMultimap.create();
    headers.put(HEADER_AUTHORIZATION, token);
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);
    return identity;
  }

  @Test
  public void testNormalizeToken() {
    Multimap<String, String> headers = ArrayListMultimap.create();
    headers.put(HEADER_AUTHORIZATION, "  " + TOKEN_USER + "== ");

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);

    QuerySource source = new QuerySource();
    source.setTableName("lessImportantStuff");

    BrokerRequest request = new BrokerRequest();
    request.setQuerySource(source);

    Assert.assertTrue(_accessControl.authorize(identity, request).hasAccess());
    Assert.assertTrue(_accessControl.authorize(identity, _tableNames).hasAccess());
  }
}
