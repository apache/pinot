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

import java.util.Optional;


/**
 * Server-side session manager for Pinot UI authentication.
 *
 * <p>Manages opaque session tokens that are issued on successful login and invalidated on explicit
 * logout (Ambari-style proper logout invalidation). The default implementation is
 * {@link InMemorySessionManager}.
 *
 * <p><strong>Broker credential forwarding:</strong> The session stores the Basic-auth token used
 * to validate credentials at login. This allows the controller to inject a proper
 * {@code Authorization} header when forwarding queries to the broker (server-to-server), without
 * ever exposing credentials in the browser's network tab.
 */
public interface SessionManager {

  /** Cookie name used to carry the session token. */
  String SESSION_COOKIE_NAME = "Pinot-UI-Session";

  /** Default session TTL: 8 hours from login. */
  long DEFAULT_SESSION_TTL_SECONDS = 8 * 60 * 60L;

  /**
   * Creates a new session for the given username and returns the opaque session token.
   *
   * @param username       the authenticated username
   * @param basicAuthToken the Basic-auth token stored server-side for broker forwarding
   * @return a cryptographically random, URL-safe session token
   */
  String createSession(String username, String basicAuthToken);

  /**
   * Looks up the username for a session token.
   *
   * @param token the session token from the cookie
   * @return the username if the session is valid and not expired, empty otherwise
   */
  Optional<String> getUsername(String token);

  /**
   * Returns the Basic-auth token stored for the given session token.
   *
   * @param token the session token from the cookie
   * @return the Basic-auth token, or empty if session not found or expired
   */
  Optional<String> getBasicAuthToken(String token);

  /**
   * Invalidates a session token immediately (called on logout).
   *
   * @param token the session token to invalidate
   */
  void invalidateSession(String token);

  /** Returns the configured session TTL in seconds. */
  long getSessionTtlSeconds();

  /** Returns the number of active sessions (for monitoring). */
  int getActiveSessionCount();

  /** Releases any background resources (e.g. cleanup executor). */
  void shutdown();
}
