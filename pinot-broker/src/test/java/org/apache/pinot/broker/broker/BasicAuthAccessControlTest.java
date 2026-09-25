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

import static org.testng.Assert.assertEquals;


public class BasicAuthAccessControlTest {
  private static final String TOKEN_USER = "Basic dXNlcjpzZWNyZXQ"; // user:secret
  private static final String TOKEN_ADMIN = "Basic YWRtaW46dmVyeXNlY3JldA"; // admin:verysecret
  private static final String TOKEN_DELETER = "Basic ZGVsZXRlcjpkZWxzZWNyZXQ="; // deleter:delsecret
  private static final String TOKEN_READER = "Basic cmVhZGVyOnJlYWRzZWNyZXQ="; // reader:readsecret
  private static final String TOKEN_EXCLUDER = "Basic ZXhjbHVkZXI6ZXhjbHNlY3JldA=="; // excluder:exclsecret

  private static final String HEADER_AUTHORIZATION = "authorization";

  private AccessControl _accessControl;

  Set<String> _tableNames;

  @BeforeClass
  public void setup() {
    Map<String, Object> config = new HashMap<>();
    config.put("principals", "admin,user,deleter,reader,excluder");
    config.put("principals.admin.password", "verysecret");
    config.put("principals.user.password", "secret");
    config.put("principals.user.tables", "lessImportantStuff,lesserImportantStuff,leastImportantStuff");
    config.put("principals.deleter.password", "delsecret");
    config.put("principals.deleter.tables", "lessImportantStuff");
    config.put("principals.deleter.permissions", "read,delete");
    config.put("principals.reader.password", "readsecret");
    config.put("principals.reader.tables", "lessImportantStuff");
    config.put("principals.reader.permissions", "read");
    config.put("principals.excluder.password", "exclsecret");
    config.put("principals.excluder.excludeTables", "billing");
    config.put("principals.excluder.permissions", "read,delete");

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

  @Test
  public void testDeleteRowsRequiresTheDeletePermission() {
    assertDeleteRows(TOKEN_DELETER, "lessImportantStuff", true, "");
    assertDeleteRows(TOKEN_DELETER, "veryImportantStuff", false,
        "Principal: deleter does not have access to table: veryImportantStuff");
    assertDeleteRows(TOKEN_READER, "lessImportantStuff", false,
        "Principal: reader is not granted the DELETE permission");
    // Principals configured without permissions query their tables, but only delete rows when granted the permission
    assertDeleteRows(TOKEN_USER, "lessImportantStuff", false, "Principal: user is not granted the DELETE permission");
    assertDeleteRows(TOKEN_ADMIN, "lessImportantStuff", false, "Principal: admin is not granted the DELETE permission");
    assertDeleteRows(null, "lessImportantStuff", false, "Missing or invalid credentials");
    assertDeleteRows("Basic d3Jvbmc6Y3JlZGVudGlhbHM=", "lessImportantStuff", false, "Missing or invalid credentials");
  }

  @Test
  public void testDeleteRowsChecksTheRawTableName() {
    // excludeTables lists raw names: a type suffix does not bypass it
    assertDeleteRows(TOKEN_EXCLUDER, "billing", false, "Principal: excluder does not have access to table: billing");
    for (String tableName : new String[]{"billing_OFFLINE", "billing_REALTIME"}) {
      assertDeleteRows(TOKEN_EXCLUDER, tableName, false,
          "Principal: excluder does not have access to table: " + tableName);
    }
    assertDeleteRows(TOKEN_EXCLUDER, "other_OFFLINE", true, "");
    // A principal listing its tables by raw name deletes from them by raw name, as it queries them
    assertDeleteRows(TOKEN_DELETER, "lessImportantStuff_OFFLINE", false,
        "Principal: deleter does not have access to table: lessImportantStuff_OFFLINE");
  }

  private void assertDeleteRows(String token, String tableName, boolean expectedAccess, String expectedMessage) {
    Multimap<String, String> headers = ArrayListMultimap.create();
    if (token != null) {
      headers.put(HEADER_AUTHORIZATION, token);
    }
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);
    AuthorizationResult result = _accessControl.authorizeDeleteRows(identity, null, tableName);
    assertEquals(result.hasAccess(), expectedAccess, token + " on " + tableName);
    assertEquals(result.getFailureMessage(), expectedMessage);
  }
}
