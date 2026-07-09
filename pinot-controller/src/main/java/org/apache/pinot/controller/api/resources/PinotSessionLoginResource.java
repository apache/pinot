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


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * REST resource for session-based UI authentication.
 *
 * <p><strong>Architecture (Trino/Ambari-style):</strong>
 * <p>This resource is a <em>session layer</em> that sits on top of ANY configured
 * {@link AccessControlFactory}. It does NOT require a specific factory class.
 * Whether you use {@code BasicAuthAccessControlFactory}, {@code ZkBasicAuthAccessControlFactory},
 * {@code PasswordAuthAccessControlFactory} (LDAP), or any custom factory — this session
 * layer works with all of them.
 *
 * <p><strong>How it works:</strong>
 * <ol>
 *   <li>UI calls {@code GET /auth/info} → receives {@code {"workflow":"SESSION"}} when
 *       {@code controller.ui.session.enabled=true}</li>
 *   <li>UI shows login form → user enters username/password</li>
 *   <li>UI POSTs to {@code POST /auth/login} (form-encoded, NOT Basic auth header)</li>
 *   <li>This resource builds a synthetic Basic-auth header and calls the configured
 *       {@link AccessControl#hasAccess} to validate credentials against whatever
 *       backend is configured (Basic, LDAP, ZK, etc.)</li>
 *   <li>On success: creates a server-side session → sets an HttpOnly cookie</li>
 *   <li>Subsequent requests: browser sends cookie automatically, NO Authorization header</li>
 *   <li>{@code GET /auth/logout}: immediately invalidates the server-side session</li>
 * </ol>
 *
 * <p><strong>Key security properties:</strong>
 * <ul>
 *   <li>Credentials are NOT visible in browser dev-tools network tab</li>
 *   <li>Cookie is HttpOnly (JS cannot read it → prevents XSS token theft)</li>
 *   <li>Logout immediately invalidates the session server-side</li>
 *   <li>Session expires automatically after 8 hours</li>
 * </ul>
 */
@Api(tags = "Auth")
@Path("/")
public class PinotSessionLoginResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSessionLoginResource.class);

  /** Path for the login endpoint. */
  public static final String AUTH_LOGIN_PATH = "auth/login";

  /** Path for the logout endpoint. */
  public static final String AUTH_LOGOUT_PATH = "auth/logout";

  /** Path for the session-check endpoint. */
  public static final String AUTH_SESSION_PATH = "auth/session";

  @Inject
  private AccessControlFactory _accessControlFactory;

  @Inject
  private SessionManager _sessionManager;

  @Inject
  private ControllerConf _controllerConf;

  // ---------------------------------------------------------------------------
  // POST /auth/login
  // ---------------------------------------------------------------------------

  /**
   * Validates username/password using the configured {@link AccessControlFactory}
   * (works with BasicAuth, ZkBasicAuth, LDAP/PasswordAuth, or any custom factory)
   * and on success creates a server-side session and returns an HttpOnly cookie.
   *
   * <p>This is the Trino/Ambari pattern: the session layer is independent of the
   * auth backend. The same login endpoint works regardless of which factory is configured.
   */
  @POST
  @Path(AUTH_LOGIN_PATH)
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Login with username and password to obtain a session cookie")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Login successful – session cookie set"),
      @ApiResponse(code = 401, message = "Invalid credentials")
  })
  public Response login(
      @ApiParam(value = "Username", required = true) @FormParam("username") String username,
      @ApiParam(value = "Password", required = true) @FormParam("password") String password) {

    if (username == null || username.trim().isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("{\"error\":\"username is required\"}")
          .build();
    }

    // Build a Basic-auth header to validate credentials via the configured AccessControl.
    // This works with ANY AccessControlFactory:
    //   - BasicAuthAccessControlFactory: validates against in-memory principals
    //   - ZkBasicAuthAccessControlFactory: validates against ZooKeeper-stored principals
    //   - PasswordAuthAccessControlFactory: validates against LDAP or other password backends
    //   - Any custom factory that implements hasAccess()
    String basicAuthToken = "Basic " + Base64.getEncoder()
        .encodeToString((username + ":" + (password == null ? "" : password))
            .getBytes(StandardCharsets.UTF_8));

    AccessControl accessControl = _accessControlFactory.create();
    boolean valid;
    try {
      valid = accessControl.hasAccess(AccessType.READ,
          new SyntheticHttpHeaders(basicAuthToken), AUTH_LOGIN_PATH);
    } catch (javax.ws.rs.NotAuthorizedException e) {
      valid = false;
    } catch (Exception e) {
      LOGGER.warn("Unexpected error during login for user '{}': {}", username, e.getMessage());
      valid = false;
    }

    if (!valid) {
      LOGGER.warn("Failed login attempt for user '{}'", username);
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity("{\"error\":\"Invalid username or password\"}")
          .build();
    }

    // Credentials are valid – create a server-side session.
    // Store the Basic-auth token server-side so the controller can inject it as an
    // Authorization header when forwarding queries to the broker (server-to-server).
    // This token is NEVER sent to the browser – it stays in server memory only.
    String sessionToken = _sessionManager.createSession(username.trim(), basicAuthToken);
    LOGGER.info("User '{}' logged in successfully", username);

    boolean secureCookie = _controllerConf == null
        || _controllerConf.getProperty(ControllerConf.CONTROLLER_UI_SESSION_COOKIE_SECURE, true);
    String sessionCookieHeader = buildSessionCookieHeader(
        sessionToken, (int) _sessionManager.getSessionTtlSeconds(), secureCookie);

    return Response.ok("{\"status\":\"ok\",\"username\":\"" + username.trim() + "\"}")
        .header("Set-Cookie", sessionCookieHeader)
        .build();
  }

  // ---------------------------------------------------------------------------
  // GET /auth/logout
  // ---------------------------------------------------------------------------

  /**
   * Invalidates the server-side session immediately and clears the session cookie.
   *
   * <p>This is proper Ambari/Trino-style logout: the session is removed from the
   * server store immediately. Even if someone captured the cookie, they cannot reuse it.
   * This is the key difference from stateless JWT: the server can revoke access instantly.
   */
  @GET
  @Path(AUTH_LOGOUT_PATH)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Logout and invalidate the current session")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Logout successful – session invalidated")
  })
  public Response logout(@Context HttpHeaders httpHeaders) {
    Cookie sessionCookie = httpHeaders.getCookies().get(SessionManager.SESSION_COOKIE_NAME);
    if (sessionCookie != null && sessionCookie.getValue() != null) {
      _sessionManager.invalidateSession(sessionCookie.getValue());
      LOGGER.info("Session invalidated on logout");
    }

    // Return a delete-cookie (Max-Age=0) to clear it from the browser
    String deleteCookieHeader = buildDeleteCookieHeader();

    return Response.ok("{\"status\":\"logged_out\"}")
        .header("Set-Cookie", deleteCookieHeader)
        .build();
  }

  // ---------------------------------------------------------------------------
  // GET /auth/session
  // ---------------------------------------------------------------------------

  /**
   * Returns the current session status (username) if a valid session cookie is present.
   *
   * <p>Used by the UI to:
   * <ol>
   *   <li>Check whether the user is still authenticated after a page refresh</li>
   *   <li>Periodically verify session validity (every 5 minutes) to detect expiry</li>
   * </ol>
   *
   * <p>Returns 401 when the session has expired, triggering a redirect to the login page.
   */
  @GET
  @Path(AUTH_SESSION_PATH)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Check current session validity")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Session is valid"),
      @ApiResponse(code = 401, message = "No valid session – session expired or not authenticated")
  })
  public Response checkSession(@Context HttpHeaders httpHeaders) {
    Cookie sessionCookie = httpHeaders.getCookies().get(SessionManager.SESSION_COOKIE_NAME);
    if (sessionCookie == null || sessionCookie.getValue() == null) {
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity("{\"error\":\"No session\"}")
          .build();
    }

    Optional<String> username = _sessionManager.getUsername(sessionCookie.getValue());
    if (!username.isPresent()) {
      // Session expired – return 401 so the UI redirects to login
      return Response.status(Response.Status.UNAUTHORIZED)
          .entity("{\"error\":\"Session expired\"}")
          .build();
    }

    return Response.ok("{\"status\":\"ok\",\"username\":\"" + username.get() + "\"}")
        .build();
  }

  // ---------------------------------------------------------------------------
  // Cookie helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds a raw {@code Set-Cookie} header value for the session cookie.
   *
   * <p>We construct the header manually (instead of using {@link javax.ws.rs.core.NewCookie})
   * because JAX-RS 2 {@code NewCookie} does not support the {@code SameSite} attribute.
   * {@code SameSite=Strict} is required to prevent CSRF attacks: the browser will only send
   * the cookie when the request originates from the same site (Pinot UI itself), blocking
   * forged cross-site requests from attacker-controlled pages.
   */
  private static String buildSessionCookieHeader(String token, int maxAgeSeconds, boolean secure) {
    StringBuilder sb = new StringBuilder();
    sb.append(SessionManager.SESSION_COOKIE_NAME).append('=').append(token);
    sb.append("; Path=/");
    sb.append("; Max-Age=").append(maxAgeSeconds);
    sb.append("; HttpOnly");
    sb.append("; SameSite=Strict");
    if (secure) {
      sb.append("; Secure");
    }
    return sb.toString();
  }

  /** Builds a {@code Set-Cookie} header that instructs the browser to delete the session cookie. */
  private static String buildDeleteCookieHeader() {
    return SessionManager.SESSION_COOKIE_NAME + "=deleted"
        + "; Path=/"
        + "; Max-Age=0"
        + "; HttpOnly"
        + "; SameSite=Strict";
  }

  // ---------------------------------------------------------------------------
  // Synthetic HttpHeaders for credential validation
  // ---------------------------------------------------------------------------

  /**
   * Minimal {@link HttpHeaders} implementation that carries only the Authorization header.
   *
   * <p>Used to validate credentials via the configured {@link AccessControl#hasAccess}
   * without requiring an actual HTTP request with an Authorization header. This allows
   * the session login endpoint to work with ANY AccessControlFactory implementation.
   */
  private static final class SyntheticHttpHeaders implements HttpHeaders {
    private final String _authorizationValue;

    SyntheticHttpHeaders(String authorizationValue) {
      _authorizationValue = authorizationValue;
    }

    @Override
    public List<String> getRequestHeader(String name) {
      if (HttpHeaders.AUTHORIZATION.equalsIgnoreCase(name)
          || "authorization".equalsIgnoreCase(name)) {
        return Collections.singletonList(_authorizationValue);
      }
      return List.of();
    }

    @Override
    public String getHeaderString(String name) {
      List<String> values = getRequestHeader(name);
      return values.isEmpty() ? null : values.get(0);
    }

    @Override
    public MultivaluedMap<String, String> getRequestHeaders() {
      MultivaluedMap<String, String> map = new MultivaluedHashMap<>();
      map.put(HttpHeaders.AUTHORIZATION, Collections.singletonList(_authorizationValue));
      return map;
    }

    @Override
    public List<javax.ws.rs.core.MediaType> getAcceptableMediaTypes() {
      return Collections.singletonList(javax.ws.rs.core.MediaType.WILDCARD_TYPE);
    }

    @Override
    public List<java.util.Locale> getAcceptableLanguages() {
      return List.of();
    }

    @Override
    public javax.ws.rs.core.MediaType getMediaType() {
      return null;
    }

    @Override
    public java.util.Locale getLanguage() {
      return null;
    }

    @Override
    public Map<String, Cookie> getCookies() {
      return Map.of();
    }

    @Override
    public java.util.Date getDate() {
      return null;
    }

    @Override
    public int getLength() {
      return -1;
    }
  }
}
