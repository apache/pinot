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
 * <p>Sessions are stored in a {@link ConcurrentHashMap} with a configurable sliding-window TTL
 * and are cleaned up periodically to prevent unbounded growth.
 *
 * <p><strong>Sliding window TTL:</strong> Each time a session is accessed via
 * {@link #getUsername(String)}, the expiry is extended by the configured TTL. This means active
 * users are never logged out due to inactivity — only truly idle sessions (no API calls for the
 * full TTL duration) expire.
 *
 * <p>Design follows the Ambari / Trino FormWebUiAuthenticationFilter pattern:
 * <ul>
 *   <li>Login  → validate credentials → create session token → set HttpOnly cookie</li>
 *   <li>Request → read cookie → look up session → slide expiry → allow or redirect to login</li>
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
    LOGGER.info("InMemorySessionManager initialized with sliding TTL={}s", _sessionTtlSeconds);
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
