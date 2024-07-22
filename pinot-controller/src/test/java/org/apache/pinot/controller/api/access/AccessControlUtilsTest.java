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
import org.testng.annotations.Test;


public class AccessControlUtilsTest {

  private final String _table = "testTable";
  private final String _endpoint = "/testEndpoint";

  @Test
  public void testValidatePermissionAllowed() {
    AccessControl ac = Mockito.mock(AccessControl.class);
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);
    Request mockRequest = Mockito.mock(Request.class);

    Mockito.when(ac.hasAccess(_table, AccessType.READ, mockHttpHeaders, _endpoint, mockRequest)).thenReturn(true);

    AccessControlUtils.validatePermission(_table, AccessType.READ, mockHttpHeaders, _endpoint, mockRequest, ac);
  }

  @Test
  public void testValidatePermissionDenied() {
    AccessControl ac = Mockito.mock(AccessControl.class);
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);
    Request mockRequest = Mockito.mock(Request.class);

    Mockito.when(ac.hasAccess(_table, AccessType.READ, mockHttpHeaders, _endpoint, mockRequest)).thenReturn(false);

    try {
      AccessControlUtils.validatePermission(_table, AccessType.READ, mockHttpHeaders, _endpoint, mockRequest, ac);
      Assert.fail("Expected ControllerApplicationException");
    } catch (ControllerApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("Permission is denied"));
      Assert.assertEquals(e.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }
  }

  @Test
  public void testValidatePermissionWithNoSuchMethodError() {
    AccessControl ac = Mockito.mock(AccessControl.class);
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);
    Request mockRequest = Mockito.mock(Request.class);

    Mockito.when(ac.hasAccess(_table, AccessType.READ, mockHttpHeaders, _endpoint, mockRequest))
        .thenThrow(new NoSuchMethodError("Method not found"));

    try {
      AccessControlUtils.validatePermission(_table, AccessType.READ, mockHttpHeaders, _endpoint, mockRequest, ac);
    } catch (ControllerApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("Caught exception while validating permission"));
      Assert.assertEquals(e.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }
}
