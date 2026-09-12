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
package org.apache.pinot.server.api;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.access.HttpRequesterIdentity;
import org.apache.pinot.server.api.resources.HealthCheckResource;
import org.apache.pinot.spi.auth.BasicAuthorizationResultImpl;
import org.apache.pinot.spi.auth.server.RequesterIdentity;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


/// Tests the fail-closed classification and request identity supplied by the Server administrative filter.
public class ServerAdminApiAccessControlFilterTest {
  private static final URI HEALTH_URI = URI.create("http://localhost:8098/health");

  @Test
  public void testOnlyAnnotatedHealthGetIsPublic()
      throws Exception {
    Method healthMethod = HealthCheckResource.class.getMethod("checkHealth", String.class);

    AccessControlFactory accessControlFactory = mock(AccessControlFactory.class);
    ServerAdminApiAccessControlFilter filter = createFilter(accessControlFactory,
        resourceInfo(HealthCheckResource.class, healthMethod));
    filter.filter(request("GET", HEALTH_URI));
    verifyNoInteractions(accessControlFactory);

    assertAdministrativeAccess(HealthCheckResource.class, healthMethod, "HEAD");

    Method customHealthMethod = CustomHealthResource.class.getMethod("checkHealth");
    assertAdministrativeAccess(CustomHealthResource.class, customHealthMethod, "GET");

    Method customDataMethod = CustomDataResource.class.getMethod("getData");
    assertAdministrativeAccess(CustomDataResource.class, customDataMethod, "GET");
  }

  @Test
  public void testStaticHandlerPreservesAllAuthenticationChallenges()
      throws Exception {
    HttpHandler delegate = mock(HttpHandler.class);
    AccessControlFactory accessControlFactory = mock(AccessControlFactory.class);
    AccessControl accessControl = mock(AccessControl.class);
    when(accessControlFactory.create()).thenReturn(accessControl);
    when(accessControl.authorizeAdminAccess(any()))
        .thenThrow(new NotAuthorizedException((Object) "Basic", "Bearer"));

    Request request = mock(Request.class);
    when(request.getHeaderNames()).thenReturn(List.of());
    when(request.getRequestURL()).thenReturn(new StringBuilder("http://localhost:8098/api/index.html"));
    Response response = mock(Response.class);

    new ServerAdminApiAccessControlHandler(delegate, accessControlFactory).service(request, response);

    verify(response).addHeader(HttpHeaders.WWW_AUTHENTICATE, "Basic");
    verify(response).addHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
    verify(response).sendError(401, "Unauthorized");
    verify(delegate, never()).service(any(), any());
  }

  @Test
  public void testStaticHandlerSuppliesRequestIdentity()
      throws Exception {
    HttpHandler delegate = mock(HttpHandler.class);
    AccessControlFactory accessControlFactory = mock(AccessControlFactory.class);
    AccessControl accessControl = mock(AccessControl.class);
    when(accessControlFactory.create()).thenReturn(accessControl);
    when(accessControl.authorizeAdminAccess(any())).thenReturn(new BasicAuthorizationResultImpl(true));

    Request request = mock(Request.class);
    when(request.getHeaderNames()).thenReturn(List.of("X-Custom"));
    when(request.getHeaders("X-Custom")).thenReturn(List.of("value"));
    when(request.getRequestURL()).thenReturn(new StringBuilder("http://localhost:8098/api/index.html"));
    when(request.getQueryString()).thenReturn("mode=test");
    Response response = mock(Response.class);

    new ServerAdminApiAccessControlHandler(delegate, accessControlFactory).service(request, response);

    ArgumentCaptor<RequesterIdentity> identityCaptor = ArgumentCaptor.forClass(RequesterIdentity.class);
    verify(accessControl).authorizeAdminAccess(identityCaptor.capture());
    HttpRequesterIdentity identity = (HttpRequesterIdentity) identityCaptor.getValue();
    Assert.assertEquals(identity.getEndpointUrl(), "http://localhost:8098/api/index.html?mode=test");
    Assert.assertEquals(identity.getHeaderValues("x-custom"), List.of("value"));
    verify(delegate).service(request, response);
  }

  @Test
  public void testStaticHandlerAcceptsAuthenticationExceptionWithoutChallenge()
      throws Exception {
    HttpHandler delegate = mock(HttpHandler.class);
    AccessControlFactory accessControlFactory = mock(AccessControlFactory.class);
    AccessControl accessControl = mock(AccessControl.class);
    when(accessControlFactory.create()).thenReturn(accessControl);
    when(accessControl.authorizeAdminAccess(any()))
        .thenThrow(new NotAuthorizedException(javax.ws.rs.core.Response.status(Status.UNAUTHORIZED).build()));

    Request request = mock(Request.class);
    when(request.getHeaderNames()).thenReturn(List.of());
    when(request.getRequestURL()).thenReturn(new StringBuilder("http://localhost:8098/api/index.html"));
    Response response = mock(Response.class);

    new ServerAdminApiAccessControlHandler(delegate, accessControlFactory).service(request, response);

    verify(response).sendError(401, "Unauthorized");
    verify(response, never()).addHeader(any(String.class), any(String.class));
    verify(delegate, never()).service(any(), any());
  }

  private static void assertAdministrativeAccess(Class<?> resourceClass, Method resourceMethod, String httpMethod)
      throws Exception {
    AccessControlFactory accessControlFactory = mock(AccessControlFactory.class);
    AccessControl accessControl = mock(AccessControl.class);
    when(accessControlFactory.create()).thenReturn(accessControl);
    when(accessControl.authorizeAdminAccess(any())).thenReturn(new BasicAuthorizationResultImpl(true));
    ServerAdminApiAccessControlFilter filter = createFilter(accessControlFactory,
        resourceInfo(resourceClass, resourceMethod));

    filter.filter(request(httpMethod, HEALTH_URI));

    ArgumentCaptor<RequesterIdentity> identityCaptor = ArgumentCaptor.forClass(RequesterIdentity.class);
    verify(accessControl).authorizeAdminAccess(identityCaptor.capture());
    RequesterIdentity requesterIdentity = identityCaptor.getValue();
    Assert.assertTrue(requesterIdentity instanceof HttpRequesterIdentity);
    Assert.assertEquals(((HttpRequesterIdentity) requesterIdentity).getEndpointUrl(), HEALTH_URI.toString());
  }

  private static ServerAdminApiAccessControlFilter createFilter(AccessControlFactory accessControlFactory,
      ResourceInfo resourceInfo)
      throws Exception {
    ServerAdminApiAccessControlFilter filter = new ServerAdminApiAccessControlFilter();
    setField(filter, "_accessControlFactory", accessControlFactory);
    setField(filter, "_resourceInfo", resourceInfo);
    HttpHeaders httpHeaders = mock(HttpHeaders.class);
    when(httpHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    setField(filter, "_httpHeaders", httpHeaders);
    return filter;
  }

  private static ResourceInfo resourceInfo(Class<?> resourceClass, Method resourceMethod) {
    ResourceInfo resourceInfo = mock(ResourceInfo.class);
    doReturn(resourceClass).when(resourceInfo).getResourceClass();
    when(resourceInfo.getResourceMethod()).thenReturn(resourceMethod);
    return resourceInfo;
  }

  private static ContainerRequestContext request(String method, URI uri) {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getRequestUri()).thenReturn(uri);
    when(requestContext.getMethod()).thenReturn(method);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    return requestContext;
  }

  private static void setField(Object target, String fieldName, Object value)
      throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static class CustomHealthResource {
    @ServerPublicAccess
    public void checkHealth() {
    }
  }

  private static class CustomDataResource {
    @ServerDataAccess
    public void getData() {
    }
  }
}
