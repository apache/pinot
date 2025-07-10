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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.NotAuthorizedException;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.grpc.GrpcRequesterIdentity;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BasicAuthAccessControlFactoryGrpcTest {
  private BasicAuthAccessControlFactory _factory;
  private AccessControl _accessControl;

  @BeforeMethod
  public void setUp() {
    _factory = new BasicAuthAccessControlFactory();
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty("principals", "admin,user");
    config.setProperty("principals.admin.password", "verysecret");
    config.setProperty("principals.user.password", "secret");
    config.setProperty("principals.user.tables", "testTable,anotherTable");
    _factory.init(config);
    _accessControl = _factory.create();
  }

  private GrpcRequesterIdentity createGrpcRequesterIdentity(Map<String, String> metadata) {
    Multimap<String, String> multimap = ArrayListMultimap.create();
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      multimap.put(entry.getKey(), entry.getValue());
    }
    GrpcRequesterIdentity identity = new GrpcRequesterIdentity();
    identity.setMetadata(multimap);
    return identity;
  }

  @Test
  public void testGrpcRequesterIdentityWithValidAuth() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic YWRtaW46dmVyeXNlY3JldA=="); // admin:verysecret
    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity);
    Assert.assertTrue(result.hasAccess());
  }

  @Test
  public void testGrpcRequesterIdentityWithValidAuthCaseInsensitive() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("Authorization", "Basic YWRtaW46dmVyeXNlY3JldA=="); // admin:verysecret
    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity);
    Assert.assertTrue(result.hasAccess());
  }

  @Test
  public void testGrpcRequesterIdentityWithUserAuth() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic dXNlcjpzZWNyZXQ="); // user:secret
    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity);
    Assert.assertTrue(result.hasAccess());
  }

  @Test(expectedExceptions = NotAuthorizedException.class)
  public void testGrpcRequesterIdentityWithInvalidAuth() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic aW52YWxpZDppbnZhbGlk"); // invalid:invalid
    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    _accessControl.authorize(identity);
  }

  @Test(expectedExceptions = NotAuthorizedException.class)
  public void testGrpcRequesterIdentityWithoutAuth() {
    Map<String, String> metadata = new HashMap<>();
    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    _accessControl.authorize(identity);
  }

  @Test(expectedExceptions = NotAuthorizedException.class)
  public void testGrpcRequesterIdentityWithEmptyAuth() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "");
    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    _accessControl.authorize(identity);
  }

  @Test
  public void testGrpcRequesterIdentityWithTableAccess() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic dXNlcjpzZWNyZXQ="); // user:secret

    BrokerRequest brokerRequest = new BrokerRequest();
    QuerySource querySource = new QuerySource();
    querySource.setTableName("testTable");
    brokerRequest.setQuerySource(querySource);

    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity, brokerRequest);
    Assert.assertTrue(result.hasAccess());
  }

  @Test
  public void testGrpcRequesterIdentityWithTableAccessDenied() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic dXNlcjpzZWNyZXQ="); // user:secret

    BrokerRequest brokerRequest = new BrokerRequest();
    QuerySource querySource = new QuerySource();
    querySource.setTableName("restrictedTable");
    brokerRequest.setQuerySource(querySource);

    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity, brokerRequest);
    Assert.assertFalse(result.hasAccess());
    Assert.assertTrue(result instanceof TableAuthorizationResult);
    TableAuthorizationResult tableResult = (TableAuthorizationResult) result;
    Assert.assertEquals(tableResult.getFailedTables(), Collections.singleton("restrictedTable"));
  }

  @Test
  public void testGrpcRequesterIdentityWithMultipleTables() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic dXNlcjpzZWNyZXQ="); // user:secret

    Set<String> tables = Set.of("testTable", "restrictedTable", "anotherTable");

    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity, tables);
    Assert.assertFalse(result.hasAccess());
    Assert.assertTrue(result instanceof TableAuthorizationResult);
    TableAuthorizationResult tableResult = (TableAuthorizationResult) result;
    Assert.assertEquals(tableResult.getFailedTables(), Collections.singleton("restrictedTable"));
  }

  @Test
  public void testGrpcRequesterIdentityWithAdminTableAccess() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic YWRtaW46dmVyeXNlY3JldA=="); // admin:verysecret

    BrokerRequest brokerRequest = new BrokerRequest();
    QuerySource querySource = new QuerySource();
    querySource.setTableName("restrictedTable");
    brokerRequest.setQuerySource(querySource);

    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity, brokerRequest);
    Assert.assertTrue(result.hasAccess());
  }

  @Test
  public void testGrpcRequesterIdentityWithNullTable() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic dXNlcjpzZWNyZXQ="); // user:secret

    BrokerRequest brokerRequest = new BrokerRequest();
    // No query source set

    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity, brokerRequest);
    Assert.assertTrue(result.hasAccess());
  }

  @Test
  public void testGrpcRequesterIdentityWithEmptyTableSet() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic dXNlcjpzZWNyZXQ="); // user:secret

    Set<String> tables = Collections.emptySet();

    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity, tables);
    Assert.assertTrue(result.hasAccess());
  }

  @Test
  public void testGrpcRequesterIdentityWithNullTableSet() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic dXNlcjpzZWNyZXQ="); // user:secret

    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity, (Set<String>) null);
    Assert.assertTrue(result.hasAccess());
  }

  @Test
  public void testGrpcRequesterIdentityWithMultipleAuthHeaders() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic YWRtaW46dmVyeXNlY3JldA=="); // admin:verysecret
    metadata.put("Authorization", "Basic dXNlcjpzZWNyZXQ="); // user:secret (should be ignored)

    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity);
    Assert.assertTrue(result.hasAccess());
  }

  @Test
  public void testGrpcRequesterIdentityWithNonAuthMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("authorization", "Basic YWRtaW46dmVyeXNlY3JldA=="); // admin:verysecret
    metadata.put("custom-header", "custom-value");
    metadata.put("content-type", "application/json");

    GrpcRequesterIdentity identity = createGrpcRequesterIdentity(metadata);

    AuthorizationResult result = _accessControl.authorize(identity);
    Assert.assertTrue(result.hasAccess());
  }
}
