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
import org.apache.pinot.spi.auth.controller.AccessControl;
import org.apache.pinot.spi.auth.controller.AccessType;
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

    Mockito.when(ac.hasAccess(_table, AccessType.READ, mockHttpHeaders, _endpoint)).thenReturn(true);

    AccessControlUtils.validatePermission(_table, AccessType.READ, mockHttpHeaders, _endpoint, ac);
  }

  @Test
  public void testValidatePermissionDenied() {
    AccessControl ac = Mockito.mock(AccessControl.class);
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

    Mockito.when(ac.hasAccess(_table, AccessType.READ, mockHttpHeaders, _endpoint)).thenReturn(false);

    try {
      AccessControlUtils.validatePermission(_table, AccessType.READ, mockHttpHeaders, _endpoint, ac);
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

    Mockito.when(ac.hasAccess(_table, AccessType.READ, mockHttpHeaders, _endpoint))
        .thenThrow(new NoSuchMethodError("Method not found"));

    try {
      AccessControlUtils.validatePermission(_table, AccessType.READ, mockHttpHeaders, _endpoint, ac);
    } catch (ControllerApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("Caught exception while validating permission"));
      Assert.assertEquals(e.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }
}
