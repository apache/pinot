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
import java.net.URI;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class FineGrainedAuthUtilsTest {

  @Test
  public void testFindRawTargetId() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    Authorize tableAuth = TestResource.class.getDeclaredMethod("getTable").getAnnotation(Authorize.class);
    Authorize clusterAuth = getAnnotatedMethod().getAnnotation(Authorize.class);

    pathParams.putSingle("tableName", "pathTable");
    assertEquals(FineGrainedAuthUtils.findRawTargetId(tableAuth, pathParams, queryParams), "pathTable");

    pathParams.clear();
    queryParams.putSingle("tableName", "queryTable");
    assertEquals(FineGrainedAuthUtils.findRawTargetId(tableAuth, pathParams, queryParams), "queryTable");
    assertNull(FineGrainedAuthUtils.findRawTargetId(clusterAuth, pathParams, queryParams));

    queryParams.clear();
    assertNull(FineGrainedAuthUtils.findRawTargetId(tableAuth, pathParams, queryParams));
  }

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

  @Test
  public void testMissingTableParameterIsBadRequest() {
    UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
    Mockito.when(mockUriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    Mockito.when(mockUriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    Mockito.when(mockUriInfo.getRequestUri()).thenReturn(URI.create("http://localhost/v2/segments"));

    WebApplicationException exception = Assert.expectThrows(WebApplicationException.class,
        () -> FineGrainedAuthUtils.validateFineGrainedAuth(getTableAnnotatedMethod(), mockUriInfo,
            Mockito.mock(HttpHeaders.class), Mockito.mock(FineGrainedAccessControl.class)));

    Assert.assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertTrue(exception.getMessage().contains("Missing required table parameter 'tableName'"));
  }

  @Test
  public void testInvalidTableParameterIsBadRequest() {
    UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
    MultivaluedHashMap<String, String> queryParameters = new MultivaluedHashMap<>();
    queryParameters.putSingle("tableName", "databaseB.testTable");
    Mockito.when(mockUriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    Mockito.when(mockUriInfo.getQueryParameters()).thenReturn(queryParameters);
    Mockito.when(mockUriInfo.getRequestUri()).thenReturn(URI.create("http://localhost/v2/segments"));
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    Mockito.when(headers.getHeaderString(CommonConstants.DATABASE)).thenReturn("databaseA");

    WebApplicationException exception = Assert.expectThrows(WebApplicationException.class,
        () -> FineGrainedAuthUtils.validateFineGrainedAuth(getTableAnnotatedMethod(), mockUriInfo, headers,
            Mockito.mock(FineGrainedAccessControl.class)));

    Assert.assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    Assert.assertTrue(exception.getMessage().contains("Invalid table parameter 'tableName'"));
  }

  static class TestResource {
    @Authorize(targetType = TargetType.CLUSTER, action = "getCluster")
    void getCluster() {
    }

    @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = "getTable")
    void getTable() {
    }

    @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = "uploadSegment")
    void uploadSegment() {
    }
  }

  private Method getAnnotatedMethod() {
    try {
      return TestResource.class.getDeclaredMethod("getCluster");
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private Method getTableAnnotatedMethod() {
    try {
      return TestResource.class.getDeclaredMethod("uploadSegment");
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }
}
