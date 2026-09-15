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
package org.apache.pinot.controller.api.resources;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;


/// Verifies compatibility behavior for the controller authentication probes.
public class PinotControllerAuthResourceTest {
  private static final String TABLE_NAME = "testTable";
  private static final String ENDPOINT_URL = "/tables/testTable";

  @Mock
  private AccessControlFactory _accessControlFactory;
  @Mock
  private AccessControl _accessControl;
  @Mock
  private HttpHeaders _httpHeaders;
  @InjectMocks
  private PinotControllerAuthResource _resource;

  private AutoCloseable _mocks;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testDeprecatedVerifyReturnsFalseForInvalidCredentials() {
    when(_accessControlFactory.create()).thenReturn(_accessControl);
    when(_accessControl.hasAccess(TABLE_NAME, AccessType.READ, _httpHeaders, ENDPOINT_URL))
        .thenThrow(new NotAuthorizedException("Basic"));

    assertFalse(_resource.verify(TABLE_NAME, AccessType.READ, ENDPOINT_URL));
  }
}
