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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.pinot.controller.ControllerConf;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/**
 * Unit tests for {@link SessionAuthenticationFilter}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Session mode disabled → pass through without aborting</li>
 *   <li>Null controller conf → pass through</li>
 *   <li>Unprotected paths → pass through</li>
 *   <li>Static assets → pass through</li>
 *   <li>Authorization header present → pass through to downstream AuthenticationFilter</li>
 *   <li>Valid session cookie → pass through</li>
 *   <li>Expired / invalid session cookie → abort with 401</li>
 *   <li>No session cookie → abort with 401</li>
 * </ul>
 */
public class SessionAuthenticationFilterTest {

  private SessionAuthenticationFilter _filter;
  private SessionManager _sessionManager;
  private ControllerConf _controllerConf;

  @BeforeMethod
  public void setUp() throws Exception {
    _filter = new SessionAuthenticationFilter();
    _sessionManager = mock(SessionManager.class);
    _controllerConf = new ControllerConf();
    inject("_sessionManager", _sessionManager);
    inject("_controllerConf", _controllerConf);
  }

  // ---------------------------------------------------------------------------
  // Session mode disabled
  // ---------------------------------------------------------------------------

  @Test
  public void testSessionDisabledPassesThrough() throws IOException {
    // CONTROLLER_UI_SESSION_ENABLED not set → defaults to false
    ContainerRequestContext ctx = buildRequest("api/tables", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  @Test
  public void testNullControllerConfPassesThrough() throws Exception {
    inject("_controllerConf", null);
    ContainerRequestContext ctx = buildRequest("api/tables", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  // ---------------------------------------------------------------------------
  // Unprotected paths
  // ---------------------------------------------------------------------------

  @Test
  public void testUnprotectedAuthLoginPassesThrough() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("auth/login", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  @Test
  public void testUnprotectedAuthLogoutPassesThrough() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("auth/logout", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  @Test
  public void testUnprotectedAuthSessionPassesThrough() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("auth/session", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  @Test
  public void testUnprotectedAuthInfoPassesThrough() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("auth/info", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  @Test
  public void testUnprotectedHealthPassesThrough() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("health", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  @Test
  public void testUnprotectedPathWithLeadingSlashPassesThrough() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("/auth/login", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  // ---------------------------------------------------------------------------
  // Static assets
  // ---------------------------------------------------------------------------

  @Test
  public void testStaticAssetIndexHtmlPassesThrough() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("index.html", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  @Test
  public void testStaticAssetFaviconIcoPassesThrough() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("favicon.ico", Map.of(), null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  // ---------------------------------------------------------------------------
  // Authorization header bypass
  // ---------------------------------------------------------------------------

  @Test
  public void testAuthorizationHeaderBypassesSessionFilter() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("api/tables", Map.of(), List.of("Basic dXNlcjpwYXNz"));
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  // ---------------------------------------------------------------------------
  // Valid session
  // ---------------------------------------------------------------------------

  @Test
  public void testValidSessionCookiePassesThrough() throws IOException {
    enableSession();
    when(_sessionManager.getUsername("valid-token")).thenReturn(Optional.of("alice"));
    Cookie cookie = mockCookie("valid-token");
    Map<String, Cookie> cookies = Map.of(SessionManager.SESSION_COOKIE_NAME, cookie);
    ContainerRequestContext ctx = buildRequest("api/tables", cookies, null);
    _filter.filter(ctx);
    verify(ctx, never()).abortWith(any());
  }

  // ---------------------------------------------------------------------------
  // Invalid / missing session → 401
  // ---------------------------------------------------------------------------

  @Test
  public void testExpiredSessionCookieAbortsWith401() throws IOException {
    enableSession();
    when(_sessionManager.getUsername("expired-token")).thenReturn(Optional.empty());
    Cookie cookie = mockCookie("expired-token");
    Map<String, Cookie> cookies = Map.of(SessionManager.SESSION_COOKIE_NAME, cookie);
    ContainerRequestContext ctx = buildRequest("api/tables", cookies, null);
    _filter.filter(ctx);
    verifyAbortedWith401(ctx);
  }

  @Test
  public void testNoCookieAbortsWith401() throws IOException {
    enableSession();
    ContainerRequestContext ctx = buildRequest("api/tables", Map.of(), null);
    _filter.filter(ctx);
    verifyAbortedWith401(ctx);
  }

  @Test
  public void testNullCookieValueAbortsWith401() throws IOException {
    enableSession();
    Cookie cookie = mockCookie(null);
    Map<String, Cookie> cookies = Map.of(SessionManager.SESSION_COOKIE_NAME, cookie);
    ContainerRequestContext ctx = buildRequest("api/tables", cookies, null);
    _filter.filter(ctx);
    verifyAbortedWith401(ctx);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void enableSession() {
    _controllerConf.setProperty(ControllerConf.CONTROLLER_UI_SESSION_ENABLED, "true");
  }

  private ContainerRequestContext buildRequest(String path, Map<String, Cookie> cookies,
      List<String> authHeaderValues) {
    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getPath()).thenReturn(path);
    when(ctx.getUriInfo()).thenReturn(uriInfo);
    when(ctx.getCookies()).thenReturn(cookies);

    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    if (authHeaderValues != null && !authHeaderValues.isEmpty()) {
      headers.put(HttpHeaders.AUTHORIZATION, authHeaderValues);
    }
    when(ctx.getHeaders()).thenReturn(headers);
    return ctx;
  }

  private static Cookie mockCookie(String value) {
    Cookie cookie = mock(Cookie.class);
    when(cookie.getValue()).thenReturn(value);
    return cookie;
  }

  private static void verifyAbortedWith401(ContainerRequestContext ctx) {
    ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
    verify(ctx).abortWith(captor.capture());
    assertEquals(captor.getValue().getStatus(), Response.Status.UNAUTHORIZED.getStatusCode());
  }

  private void inject(String fieldName, Object value) throws Exception {
    Field field = SessionAuthenticationFilter.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(_filter, value);
  }
}
