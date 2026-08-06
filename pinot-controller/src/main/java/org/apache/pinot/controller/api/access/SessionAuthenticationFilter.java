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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.ControllerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JAX-RS container filter that enforces session-based authentication for the Pinot UI.
 *
 * <p><strong>Architecture (Trino/Ambari-style):</strong>
 * <p>This filter is completely INDEPENDENT of the configured {@link AccessControlFactory}.
 * It activates when {@code controller.ui.session.enabled=true} is set in the controller
 * configuration, regardless of which auth factory is configured (BasicAuth, ZkBasicAuth,
 * LDAP/PasswordAuth, or any custom factory).
 *
 * <p>This mirrors Trino's {@code FormWebUiAuthenticationFilter} which is independent of
 * the {@code FormAuthenticator} implementation. The session layer is a UI concern, not
 * an auth backend concern.
 *
 * <p><strong>How it works:</strong>
 * <ol>
 *   <li>Skips public/unprotected paths (login, logout, health, auth/info, static assets)</li>
 *   <li>Reads the {@value SessionManager#SESSION_COOKIE_NAME} HttpOnly cookie</li>
 *   <li>Validates the session token against the server-side {@link SessionManager}</li>
 *   <li>If valid → allows the request to proceed</li>
 *   <li>If invalid/missing → returns 401 (UI redirects to login page)</li>
 * </ol>
 *
 * <p><strong>Key security property:</strong> The Authorization header is <em>not</em> used
 * for UI requests. The session cookie is the sole credential, and it is HttpOnly so
 * JavaScript cannot read it. This eliminates the token-in-header exposure seen in the
 * browser dev-tools network tab.
 *
 * <p><strong>Activation:</strong> Set {@code controller.ui.session.enabled=true} in
 * {@code pinot-controller.conf}. No factory class change required.
 */
@javax.ws.rs.ext.Provider
@javax.annotation.Priority(javax.ws.rs.Priorities.AUTHENTICATION - 10)
public class SessionAuthenticationFilter implements ContainerRequestFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionAuthenticationFilter.class);

  /**
   * Paths that are always accessible without a session.
   * Mirrors Trino's UNPROTECTED_PATHS set.
   */
  private static final Set<String> UNPROTECTED_PATHS = new HashSet<>(Arrays.asList(
      "",
      "help",
      "health",
      "auth/info",
      "auth/verify",
      "auth/login",   // login endpoint itself must be accessible
      "auth/logout",  // logout must be accessible to clear the session
      "auth/session"  // session check must be accessible
  ));

  @Inject
  private SessionManager _sessionManager;

  @Inject
  private ControllerConf _controllerConf;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    // Only activate when session mode is enabled via controller.ui.session.enabled=true.
    // This is INDEPENDENT of the configured AccessControlFactory.
    // Works with BasicAuth, ZkBasicAuth, LDAP/PasswordAuth, or any custom factory.
    if (_controllerConf == null
        || !_controllerConf.getProperty(ControllerConf.CONTROLLER_UI_SESSION_ENABLED, false)) {
      // Session mode not enabled – let the existing AuthenticationFilter handle it
      return;
    }

    String path = requestContext.getUriInfo().getPath();

    // Strip leading slash for comparison
    String normalizedPath = path.startsWith("/") ? path.substring(1) : path;

    // Always allow unprotected paths
    if (isUnprotected(normalizedPath)) {
      return;
    }

    // Allow static assets (files with extensions at the root level, e.g., index.html, favicon.ico)
    if (isStaticAsset(normalizedPath)) {
      return;
    }


    // Server-to-server API calls (e.g., pinot-admin.sh AddTable -authToken "Basic ...") carry an
    // Authorization header. This filter handles session validation for browser UI requests only.
    // Requests with an Authorization header are passed through here so the downstream
    // AuthenticationFilter (priority AUTHENTICATION, which runs after this filter at
    // AUTHENTICATION-10) can validate the credential. This is intentional: the session layer
    // is a UI concern; API callers authenticate via the AuthenticationFilter, not via session cookies.
    // NOTE: This means session mode does NOT block API callers that present a valid Authorization
    // header — they are authenticated by AuthenticationFilter instead.
    List<String> authHeaders = requestContext.getHeaders().get(HttpHeaders.AUTHORIZATION);
    if (authHeaders != null && !authHeaders.isEmpty()) {
      // Has Authorization header – let the existing AuthenticationFilter validate it
      LOGGER.debug("Request has Authorization header, bypassing session filter for path '{}'", path);
      return;
    }


    // Check for a valid session cookie (browser UI requests)
    Cookie sessionCookie = requestContext.getCookies().get(SessionManager.SESSION_COOKIE_NAME);
    if (sessionCookie != null && sessionCookie.getValue() != null) {
      Optional<String> username = _sessionManager.getUsername(sessionCookie.getValue());
      if (username.isPresent()) {
        // Valid session – allow the request to proceed
        LOGGER.debug("Session valid for user '{}' on path '{}'", username.get(), path);
        return;
      }
      // Session expired or invalid
      LOGGER.debug("Session expired or invalid for path '{}'", path);
    }

    // No valid session and no Authorization header – return 401 so the UI redirects to the login page
    requestContext.abortWith(
        Response.status(Response.Status.UNAUTHORIZED)
            .entity("{\"error\":\"Session expired or not authenticated. Please log in.\"}")
            .type(javax.ws.rs.core.MediaType.APPLICATION_JSON)
            .build()
    );
  }

  private static boolean isUnprotected(String path) {
    // Exact match
    if (UNPROTECTED_PATHS.contains(path)) {
      return true;
    }
    // Prefix match for auth/* paths
    if (path.startsWith("auth/login") || path.startsWith("auth/logout")
        || path.startsWith("auth/info") || path.startsWith("auth/verify")
        || path.startsWith("auth/session")) {
      return true;
    }
    return false;
  }

  private static boolean isStaticAsset(String path) {
    // Files at root level with an extension (e.g., index.html, favicon.ico)
    return !path.contains("/") && path.contains(".");
  }
}
