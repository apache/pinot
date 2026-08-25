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
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class FineGrainedAuthUtilsTest {

  @Test
  public void testFindRawTargetId()
      throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    Method tableMethod = TestResource.class.getDeclaredMethod("getTable");
    Method tableQueryMethod = TestResource.class.getDeclaredMethod("getTableByQuery", String.class);
    Authorize tableAuth = tableMethod.getAnnotation(Authorize.class);
    Authorize tableQueryAuth = tableQueryMethod.getAnnotation(Authorize.class);
    Authorize clusterAuth = getAnnotatedMethod().getAnnotation(Authorize.class);

    pathParams.putSingle("tableName", "pathTable");
    assertEquals(FineGrainedAuthUtils.findRawTargetId(tableAuth, tableMethod, pathParams, queryParams), "pathTable");

    pathParams.clear();
    queryParams.putSingle("tableName", "queryTable");
    // The annotation names tableName, but getTable never binds it, so the query value is not trusted.
    assertNull(FineGrainedAuthUtils.findRawTargetId(tableAuth, tableMethod, pathParams, queryParams));
    assertEquals(FineGrainedAuthUtils.findRawTargetId(tableQueryAuth, tableQueryMethod, pathParams, queryParams),
        "queryTable");
    assertNull(FineGrainedAuthUtils.findRawTargetId(clusterAuth, getAnnotatedMethod(), pathParams, queryParams));

    queryParams.clear();
    assertNull(FineGrainedAuthUtils.findRawTargetId(tableAuth, tableMethod, pathParams, queryParams));
  }

  @Test
  public void testValidateFineGrainedAuthIgnoresUndeclaredTableQueryParam()
      throws Exception {
    FineGrainedAccessControl ac = Mockito.mock(FineGrainedAccessControl.class);
    Mockito.when(ac.hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(true);

    UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("tableName", "callerPicked");
    Mockito.when(mockUriInfo.getPathParameters()).thenReturn(pathParams);
    Mockito.when(mockUriInfo.getQueryParameters()).thenReturn(queryParams);
    Mockito.when(mockUriInfo.getRequestUri()).thenReturn(URI.create("http://localhost/tables"));
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

    Method unboundTableMethod = TestResource.class.getDeclaredMethod("getTable");
    try {
      FineGrainedAuthUtils.validateFineGrainedAuth(unboundTableMethod, mockUriInfo, mockHttpHeaders, ac);
      Assert.fail("Expected WebApplicationException");
    } catch (WebApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("Could not find paramName"));
      Assert.assertEquals(e.getResponse().getStatus(),
          FineGrainedAuthUtils.UNBOUND_TABLE_PARAM_STATUS.getStatusCode());
      Assert.assertEquals(e.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    }
    Mockito.verify(ac, Mockito.never())
        .hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testUnboundTableParamExceptionRevertsToInternalServerError() {
    URI requestUri = URI.create("http://localhost/tables");
    WebApplicationException current = FineGrainedAuthUtils.unboundTableParamException("tableName", requestUri);
    Assert.assertEquals(current.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());

    // Revert path: passing the previous status restores the 500 that this case used to pin.
    WebApplicationException reverted = FineGrainedAuthUtils.unboundTableParamException("tableName", requestUri,
        Response.Status.INTERNAL_SERVER_ERROR);
    Assert.assertTrue(reverted.getMessage().contains("Could not find paramName"));
    Assert.assertEquals(reverted.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    Assert.assertEquals(reverted.getMessage(), current.getMessage());
  }

  @Test
  public void testValidateFineGrainedAuthEmptyParamNameStaysInternalServerError()
      throws Exception {
    FineGrainedAccessControl ac = Mockito.mock(FineGrainedAccessControl.class);
    UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
    Mockito.when(mockUriInfo.getRequestUri()).thenReturn(URI.create("http://localhost/tables/foo/copy"));
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

    Method copyTableLike = TestResource.class.getDeclaredMethod("copyTableWithoutParamName");
    try {
      FineGrainedAuthUtils.validateFineGrainedAuth(copyTableLike, mockUriInfo, mockHttpHeaders, ac);
      Assert.fail("Expected WebApplicationException");
    } catch (WebApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("paramName not found for table level authorization"));
      Assert.assertEquals(e.getResponse().getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    Mockito.verify(ac, Mockito.never())
        .hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testValidateFineGrainedAuthAllowsDeclaredTableQueryParam()
      throws Exception {
    FineGrainedAccessControl ac = Mockito.mock(FineGrainedAccessControl.class);
    Mockito.when(ac.hasAccess(Mockito.any(HttpHeaders.class), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(true);

    UriInfo mockUriInfo = Mockito.mock(UriInfo.class);
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("tableName", "ownedTable");
    Mockito.when(mockUriInfo.getPathParameters()).thenReturn(pathParams);
    Mockito.when(mockUriInfo.getQueryParameters()).thenReturn(queryParams);
    HttpHeaders mockHttpHeaders = Mockito.mock(HttpHeaders.class);

    Method declaredTableMethod = TestResource.class.getDeclaredMethod("getTableByQuery", String.class);
    FineGrainedAuthUtils.validateFineGrainedAuth(declaredTableMethod, mockUriInfo, mockHttpHeaders, ac);
    Mockito.verify(ac).hasAccess(mockHttpHeaders, TargetType.TABLE, "ownedTable", "getTable");
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

  static class TestResource {
    @Authorize(targetType = TargetType.CLUSTER, action = "getCluster")
    void getCluster() {
    }

    @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = "getTable")
    void getTable() {
    }

    @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = "getTable")
    void getTableByQuery(@QueryParam("tableName") String tableName) {
    }

    @Authorize(targetType = TargetType.TABLE, action = "createTable")
    void copyTableWithoutParamName() {
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
