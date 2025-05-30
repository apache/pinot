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
package org.apache.pinot.broker.api;

import java.util.Set;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.broker.AccessControl;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.auth.request.BrokerRequest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class AccessControlBackwardCompatibleTest {

  @Test
  public void testBackwardCompatibleHasAccessBrokerRequest() {
    AccessControl accessControl = new AllFalseAccessControlImpl();
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    BrokerRequest request = new BrokerRequest();
    assertFalse(accessControl.authorize(identity, request).hasAccess());
  }

  @Test
  public void testBackwardCompatibleHasAccessMutliTable() {
    AccessControl accessControl = new AllFalseAccessControlImpl();
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    Set<String> tables = Set.of("table1", "table2");
    AuthorizationResult result = accessControl.authorize(identity, tables);
    assertFalse(result.hasAccess());
    assertEquals(result.getFailureMessage(), "Authorization Failed for tables: [table1, table2]");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testExceptionForNoImplAccessControlMultiTable() {
    AccessControl accessControl = new NoImplAccessControl();
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    Set<String> tables = Set.of("table1", "table2");
    accessControl.hasAccess(identity, tables);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testExceptionForNoImplAccessControlBrokerRequest() {
    AccessControl accessControl = new NoImplAccessControl();
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    BrokerRequest request = new BrokerRequest();
    accessControl.hasAccess(identity, request);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testExceptionForNoImplAccessControlAuthorizeMultiTable() {
    AccessControl accessControl = new NoImplAccessControl();
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    Set<String> tables = Set.of("table1", "table2");
    accessControl.authorize(identity, tables);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testExceptionForNoImplAccessControlAuthorizeBrokerRequest() {
    AccessControl accessControl = new NoImplAccessControl();
    HttpRequesterIdentity identity = new HttpRequesterIdentity();
    BrokerRequest request = new BrokerRequest();
    accessControl.authorize(identity, request);
  }

  class AllFalseAccessControlImpl implements AccessControl {

    @Override
    public boolean hasAccess(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
      return false;
    }

    @Override
    public boolean hasAccess(RequesterIdentity requesterIdentity, Set<String> tables) {
      return false;
    }
  }

  class NoImplAccessControl implements AccessControl {
  }
}
