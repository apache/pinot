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
 * In-memory implementation of {@link SessionManager}.
 *
 * <p>Sessions are stored in a {@link ConcurrentHashMap} with a configurable fixed TTL
 * and are cleaned up periodically to prevent unbounded growth.
 *
 * <p><strong>Fixed TTL:</strong> Each session expires at {@code loginTime + TTL} regardless of
 * activity. This keeps the server-side expiry aligned with the browser cookie {@code Max-Age},
 * both of which are set to the same TTL at login. A sliding-window TTL would let the server
 * session outlive the browser cookie (which has no renewal mechanism here), creating a
 * desynchronised state where the server holds a valid session the browser can no longer reach.
 *
 * <p>Design follows the Ambari / Trino FormWebUiAuthenticationFilter pattern:
 * <ul>
 *   <li>Login  → validate credentials → create session token → set HttpOnly cookie</li>
 *   <li>Request → read cookie → look up session → allow or redirect to login</li>
 *   <li>Logout → read cookie → remove session from store → delete cookie → redirect to login</li>
 * </ul>
 */
public class InMemorySessionManager implements SessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemorySessionManager.class);

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

  public InMemorySessionManager() {
    this(DEFAULT_SESSION_TTL_SECONDS);
  }

  public InMemorySessionManager(long sessionTtlSeconds) {
    _sessionTtlSeconds = sessionTtlSeconds;
    _cleanupExecutor.scheduleAtFixedRate(
        this::evictExpiredSessions,
        CLEANUP_INTERVAL_SECONDS,
        CLEANUP_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
    LOGGER.info("InMemorySessionManager initialized with fixed TTL={}s", _sessionTtlSeconds);
  }

  @Override
  public String createSession(String username, String basicAuthToken) {
    String token = generateToken();
    Instant expiry = Instant.now().plusSeconds(_sessionTtlSeconds);
    _sessions.put(token, new SessionEntry(username, expiry, basicAuthToken));
    LOGGER.debug("Created session for user '{}', expires at {}", username, expiry);
    return token;
  }

  @Override
  public Optional<String> getUsername(String token) {
    if (token == null || token.isEmpty()) {
      return Optional.empty();
    }
    // Fixed TTL: just check expiry without extending it, keeping server and browser cookie
    // expiry in sync (both set to the same TTL value at login time).
    SessionEntry entry = _sessions.get(token);
    if (entry == null || Instant.now().isAfter(entry.expiry())) {
      return Optional.empty();
    }
    return Optional.of(entry.username());
  }

  @Override
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
   * Atomically rotates the session using {@link ConcurrentHashMap#remove(Object, Object)}.
   * Only one of concurrent renew requests will win the CAS; the rest see empty and get 401.
   * This prevents orphaned tokens from double-clicks on "Stay Logged In".
   */
  @Override
  public Optional<String> rotateSession(String oldToken) {
    if (oldToken == null || oldToken.isEmpty()) {
      return Optional.empty();
    }
    SessionEntry entry = _sessions.get(oldToken);
    if (entry == null || Instant.now().isAfter(entry.expiry())) {
      return Optional.empty();
    }
    // CAS: only one concurrent caller can remove this exact entry reference.
    if (!_sessions.remove(oldToken, entry)) {
      return Optional.empty();
    }
    String newToken = createSession(entry.username(), entry.basicAuthToken());
    return Optional.of(newToken);
  }

  @Override
  public void invalidateSession(String token) {
    if (token != null && !token.isEmpty()) {
      SessionEntry removed = _sessions.remove(token);
      if (removed != null) {
        LOGGER.debug("Session invalidated for user '{}'", removed.username());
      }
    }
  }

  @Override
  public long getSessionTtlSeconds() {
    return _sessionTtlSeconds;
  }

  @Override
  public int getActiveSessionCount() {
    return _sessions.size();
  }

  @Override
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
