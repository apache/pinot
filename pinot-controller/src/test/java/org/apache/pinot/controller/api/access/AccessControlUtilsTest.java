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
package org.apache.pinot.controller.api.access;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.glassfish.grizzly.http.server.Request;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AccessControlUtilsTest {

  private final String _table = "testTable";
  private final String _endpoint = "/testEndpoint";

  private AccessControl _mockAccessControl;
  private HttpHeaders _mockHttpHeaders;
  private Request _mockRequest;

  @BeforeMethod
  public void beforeMethod() {
    _mockAccessControl = Mockito.mock(AccessControl.class);
    _mockHttpHeaders = Mockito.mock(HttpHeaders.class);
    _mockRequest = Mockito.mock(Request.class);
  }

  @Test
  public void testValidatePermissionAllowed() {
    Mockito.when(_mockAccessControl.hasAccess(_table, AccessType.READ, _mockHttpHeaders, _mockRequest, _endpoint))
        .thenReturn(true);
    AccessControlUtils.validatePermission(_table, AccessType.READ, _mockHttpHeaders, _mockRequest, _endpoint,
        _mockAccessControl);
  }

  @Test
  public void testValidatePermissionDenied() {
    Mockito.when(_mockAccessControl.hasAccess(_table, AccessType.READ, _mockHttpHeaders, _mockRequest, _endpoint))
        .thenReturn(false);
    try {
      AccessControlUtils.validatePermission(_table, AccessType.READ, _mockHttpHeaders, _mockRequest, _endpoint,
          _mockAccessControl);
      Assert.fail("Expected ControllerApplicationException");
    } catch (ControllerApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("Permission is denied"));
      Assert.assertEquals(e.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }
  }

  @Test
  public void testValidatePermissionWithNoSuchMethodError() {
    Mockito.when(_mockAccessControl.hasAccess(_table, AccessType.READ, _mockHttpHeaders, _mockRequest, _endpoint))
        .thenThrow(new NoSuchMethodError("Method not found"));

    try {
      AccessControlUtils.validatePermission(_table, AccessType.READ, _mockHttpHeaders, _mockRequest, _endpoint,
          _mockAccessControl);
    } catch (ControllerApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("Caught exception while validating permission"));
      Assert.assertEquals(e.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }
}
