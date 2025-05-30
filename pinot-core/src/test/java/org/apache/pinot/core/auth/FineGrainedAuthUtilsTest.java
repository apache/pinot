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
package org.apache.pinot.core.auth;

import java.lang.reflect.Method;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.pinot.spi.auth.core.FineGrainedAccessControl;
import org.apache.pinot.spi.auth.core.TargetType;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FineGrainedAuthUtilsTest {

  @Test
  public void testValidateFineGrainedAuthAllowed() {
    FineGrainedAccessControl ac = Mockito.mock(FineGrainedAccessControl.class);
    Mockito.when(ac.hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(true);

    UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

    FineGrainedAuthUtils.validateFineGrainedAuth(getAnnotatedMethod(), mockUriInfo, mockHttpHeaders, ac);
  }

  @Test
  public void testValidateFineGrainedAuthDenied() {
    FineGrainedAccessControl ac = Mockito.mock(FineGrainedAccessControl.class);
    Mockito.when(ac.hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(false);

    UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

    try {
      FineGrainedAuthUtils.validateFineGrainedAuth(getAnnotatedMethod(), mockUriInfo, mockHttpHeaders, ac);
      Assert.fail("Expected WebApplicationException");
    } catch (WebApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("Access denied to getCluster in the cluster"));
      Assert.assertEquals(e.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }
  }

  @Test
  public void testValidateFineGrainedAuthWithNoSuchMethodError() {
    FineGrainedAccessControl ac = Mockito.mock(FineGrainedAccessControl.class);
    Mockito.when(ac.hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenThrow(new NoSuchMethodError("Method not found"));

    UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

    try {
      FineGrainedAuthUtils.validateFineGrainedAuth(getAnnotatedMethod(), mockUriInfo, mockHttpHeaders, ac);
      Assert.fail("Expected WebApplicationException");
    } catch (WebApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to check for access"));
      Assert.assertEquals(e.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  static class TestResource {
    @Authorize(targetType = TargetType.CLUSTER, action = "getCluster")
    void getCluster() {
    }
  }

  private Method getAnnotatedMethod() {
    try {
      return TestResource.class.getDeclaredMethod("getCluster");
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }
}
