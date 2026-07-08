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


import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Server-side session manager for Pinot UI authentication.
 *
 * <p>Manages opaque session tokens that are issued on successful login and
 * invalidated on explicit logout (Ambari-style proper logout invalidation).
 * Sessions are stored in-memory with a configurable TTL and are cleaned up
 * periodically to prevent unbounded growth.
 *
 * <p><strong>Sliding window TTL:</strong> Each time a session is accessed via
 * {@link #getUsername(String)}, the expiry is extended by the configured TTL.
 * This means active users are never logged out due to inactivity — only truly
 * idle sessions (no API calls for the full TTL duration) expire.
 *
 * <p><strong>Broker credential forwarding:</strong> The session stores the
 * Basic-auth token used to validate credentials at login. This allows the
 * controller to inject a proper {@code Authorization} header when forwarding
 * queries to the broker (server-to-server), without ever exposing credentials
 * in the browser's network tab.
 *
 * <p>Design follows the Ambari / Trino FormWebUiAuthenticationFilter pattern:
 * <ul>
 *   <li>Login  → validate credentials → create session token → set HttpOnly cookie</li>
 *   <li>Request → read cookie → look up session → slide expiry → allow or redirect to login</li>
 *   <li>Logout → read cookie → remove session from store → delete cookie → redirect to login</li>
 * </ul>
 */
public class SessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

  /** Cookie name used to carry the session token. */
  public static final String SESSION_COOKIE_NAME = "Pinot-UI-Session";

  /** Default session TTL: 8 hours (sliding window – resets on each API call). */
  public static final long DEFAULT_SESSION_TTL_SECONDS = 8 * 60 * 60L;

  /** Cleanup interval: every 30 minutes. */
  private static final long CLEANUP_INTERVAL_SECONDS = 30 * 60L;

  /** Number of random bytes for the session token (256-bit entropy). */
  private static final int TOKEN_BYTES = 32;

  private final long _sessionTtlSeconds;
  private final SecureRandom _secureRandom = new SecureRandom();

  /**
   * Active session store: token → {@link SessionEntry}.
   * ConcurrentHashMap provides thread-safe reads without locking on the hot path.
   */
  private final ConcurrentHashMap<String, SessionEntry> _sessions = new ConcurrentHashMap<>();

  private final ScheduledExecutorService _cleanupExecutor =
      Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "pinot-session-cleanup");
        t.setDaemon(true);
        return t;
      });

  public SessionManager() {
    this(DEFAULT_SESSION_TTL_SECONDS);
  }

  public SessionManager(long sessionTtlSeconds) {
    _sessionTtlSeconds = sessionTtlSeconds;
    _cleanupExecutor.scheduleAtFixedRate(
        this::evictExpiredSessions,
        CLEANUP_INTERVAL_SECONDS,
        CLEANUP_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
    LOGGER.info("SessionManager initialized with sliding TTL={}s", _sessionTtlSeconds);
  }

  /**
   * Creates a new session for the given username and returns the opaque session token.
   *
   * @param username the authenticated username
   * @param basicAuthToken the Basic-auth token (e.g. "Basic dXNlcjpwYXNz") used to validate
   *                       credentials at login. Stored server-side so the controller can inject
   *                       it as an Authorization header when forwarding queries to the broker.
   *                       Never sent to the browser.
   * @return a cryptographically random, URL-safe session token
   */
  public String createSession(String username, String basicAuthToken) {
    String token = generateToken();
    Instant expiry = Instant.now().plusSeconds(_sessionTtlSeconds);
    _sessions.put(token, new SessionEntry(username, expiry, basicAuthToken));
    LOGGER.debug("Created session for user '{}', expires at {}", username, expiry);
    return token;
  }

  /**
   * Looks up the username associated with a session token.
   *
   * <p><strong>Sliding window:</strong> If the session is valid, its expiry is extended
   * by the configured TTL. This ensures active users are never logged out due to inactivity.
   *
   * @param token the session token from the cookie
   * @return the username if the session is valid and not expired, empty otherwise
   */
  public Optional<String> getUsername(String token) {
    if (token == null || token.isEmpty()) {
      return Optional.empty();
    }
    // Sliding window: single atomic compute() avoids a race condition where a non-atomic
    // get+check+put sequence could see the session removed between the expiry check and the put.
    // compute() provides an atomic read-modify-write on the ConcurrentHashMap entry.
    final String[] resolvedUsername = {null};
    _sessions.compute(token, (k, v) -> {
      if (v == null || Instant.now().isAfter(v.expiry())) {
        // Expired or not found – remove the entry (return null removes key from map)
        return null;
      }
      resolvedUsername[0] = v.username();
      return new SessionEntry(v.username(), Instant.now().plusSeconds(_sessionTtlSeconds), v.basicAuthToken());
    });
    return Optional.ofNullable(resolvedUsername[0]);
  }

  /**
   * Returns the Basic-auth token stored for the given session token.
   *
   * <p>Used by {@link org.apache.pinot.controller.api.resources.PinotQueryResource} to inject
   * a proper {@code Authorization} header when forwarding queries to the broker (server-to-server).
   * This ensures the broker can authenticate the request without the browser ever seeing credentials.
   *
   * @param token the session token from the cookie
   * @return the Basic-auth token (e.g. "Basic dXNlcjpwYXNz"), or empty if session not found
   */
  public Optional<String> getBasicAuthToken(String token) {
    if (token == null || token.isEmpty()) {
      return Optional.empty();
    }
    SessionEntry entry = _sessions.get(token);
    if (entry == null || Instant.now().isAfter(entry.expiry())) {
      return Optional.empty();
    }
    return Optional.ofNullable(entry.basicAuthToken());
  }

  /**
   * Invalidates a session token (called on logout).
   * This is the key difference from stateless JWT: the server can immediately
   * revoke access without waiting for token expiry.
   *
   * @param token the session token to invalidate
   */
  public void invalidateSession(String token) {
    if (token != null && !token.isEmpty()) {
      SessionEntry removed = _sessions.remove(token);
      if (removed != null) {
        LOGGER.debug("Session invalidated for user '{}'", removed.username());
      }
    }
  }

  /**
   * Returns the configured session TTL in seconds.
   */
  public long getSessionTtlSeconds() {
    return _sessionTtlSeconds;
  }

  /**
   * Returns the number of active (possibly including some expired) sessions.
   * Useful for monitoring/metrics.
   */
  public int getActiveSessionCount() {
    return _sessions.size();
  }

  /**
   * Shuts down the background cleanup executor.
   */
  public void shutdown() {
    _cleanupExecutor.shutdownNow();
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private String generateToken() {
    byte[] bytes = new byte[TOKEN_BYTES];
    _secureRandom.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  private void evictExpiredSessions() {
    Instant now = Instant.now();
    int removed = 0;
    Iterator<Map.Entry<String, SessionEntry>> it = _sessions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, SessionEntry> entry = it.next();
      if (now.isAfter(entry.getValue().expiry())) {
        it.remove();
        removed++;
      }
    }
    if (removed > 0) {
      LOGGER.debug("Evicted {} expired sessions; {} active sessions remain", removed, _sessions.size());
    }
  }

  // ---------------------------------------------------------------------------
  // Inner record
  // ---------------------------------------------------------------------------

  /**
   * Immutable session entry holding the username, expiry instant, and stored Basic-auth token.
   *
   * <p>The {@code basicAuthToken} is stored server-side only — it is never sent to the browser.
   * It is used to inject an {@code Authorization} header when the controller forwards queries
   * to the broker (server-to-server communication).
   */
  private static final class SessionEntry {
    private final String _username;
    private final Instant _expiry;
    private final String _basicAuthToken;

    SessionEntry(String username, Instant expiry, String basicAuthToken) {
      _username = username;
      _expiry = expiry;
      _basicAuthToken = basicAuthToken;
    }

    String username() {
      return _username;
    }

    Instant expiry() {
      return _expiry;
    }

    String basicAuthToken() {
      return _basicAuthToken;
    }
  }
}

