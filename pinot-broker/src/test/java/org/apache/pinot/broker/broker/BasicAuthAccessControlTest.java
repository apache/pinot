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
import java.util.Map;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.HttpRequesterIdentity;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class BasicAuthAccessControlTest {
  private static final String TOKEN_USER = "Basic dXNlcjpzZWNyZXQ"; // user:secret
  private static final String TOKEN_ADMIN = "Basic YWRtaW46dmVyeXNlY3JldA"; // admin:verysecret

  private static final String HEADER_AUTHORIZATION = "authorization";

  private AccessControl _accessControl;

  @BeforeClass
  public void setup() {
    Map<String, Object> config = new HashMap<>();
    config.put("principals", "admin,user");
    config.put("principals.admin.password", "verysecret");
    config.put("principals.user.password", "secret");
    config.put("principals.user.tables", "lessImportantStuff");

    AccessControlFactory factory = new BasicAuthAccessControlFactory();
    factory.init(new PinotConfiguration(config));

    _accessControl = factory.create();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNullEntity() {
    _accessControl.hasAccess(null, null);
  }

  @Test
  public void testNullToken() {
    Multimap<String, String> headers = ArrayListMultimap.create();

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);

    Assert.assertFalse(_accessControl.hasAccess(identity, null));
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

    Assert.assertTrue(_accessControl.hasAccess(identity, request));
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

    Assert.assertFalse(_accessControl.hasAccess(identity, request));
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

    Assert.assertTrue(_accessControl.hasAccess(identity, request));
  }

  @Test
  public void testAllowNonTable() {
    Multimap<String, String> headers = ArrayListMultimap.create();
    headers.put(HEADER_AUTHORIZATION, TOKEN_USER);

    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    identity.setHttpHeaders(headers);

    BrokerRequest request = new BrokerRequest();

    Assert.assertTrue(_accessControl.hasAccess(identity, request));
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

    Assert.assertTrue(_accessControl.hasAccess(identity, request));
  }
}
