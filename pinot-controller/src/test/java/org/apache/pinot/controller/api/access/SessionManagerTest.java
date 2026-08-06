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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit tests for {@link InMemorySessionManager}.
 *
 * <p>Covers:
 * <ul>
 *   <li>createSession — token generation, storage</li>
 *   <li>getUsername — valid session, expired session, null/empty token</li>
 *   <li>Fixed TTL — session expires at loginTime + TTL regardless of access</li>
 *   <li>invalidateSession — immediate removal, idempotent</li>
 *   <li>getBasicAuthToken — returns stored token, empty after invalidation</li>
 *   <li>Concurrent access — no race condition under parallel getUsername calls</li>
 * </ul>
 */
public class SessionManagerTest {

  private SessionManager _sessionManager;

  @BeforeMethod
  public void setUp() {
    // 10-second TTL for most tests — long enough not to expire during assertions
    _sessionManager = new InMemorySessionManager(10L);
  }

  @AfterMethod
  public void tearDown() {
    _sessionManager.shutdown();
  }

  // ---------------------------------------------------------------------------
  // createSession
  // ---------------------------------------------------------------------------

  @Test
  public void testCreateSessionReturnsNonNullToken() {
    String token = _sessionManager.createSession("alice", "Basic dXNlcjpwYXNz");
    assertNotNull(token);
    assertFalse(token.isEmpty());
  }

  @Test
  public void testCreateSessionTokensAreUnique() {
    String token1 = _sessionManager.createSession("alice", "Basic abc");
    String token2 = _sessionManager.createSession("alice", "Basic abc");
    assertNotEquals(token1, token2, "Each createSession call should produce a unique token");
  }

  @Test
  public void testCreateSessionIncrementsActiveCount() {
    int before = _sessionManager.getActiveSessionCount();
    _sessionManager.createSession("alice", "Basic abc");
    assertEquals(_sessionManager.getActiveSessionCount(), before + 1);
  }

  // ---------------------------------------------------------------------------
  // getUsername — valid session
  // ---------------------------------------------------------------------------

  @Test
  public void testGetUsernameReturnsCorrectUsername() {
    String token = _sessionManager.createSession("alice", "Basic abc");
    Optional<String> result = _sessionManager.getUsername(token);
    assertTrue(result.isPresent());
    assertEquals(result.get(), "alice");
  }

  @Test
  public void testGetUsernameWithNullTokenReturnsEmpty() {
    Optional<String> result = _sessionManager.getUsername(null);
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetUsernameWithEmptyTokenReturnsEmpty() {
    Optional<String> result = _sessionManager.getUsername("");
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetUsernameWithUnknownTokenReturnsEmpty() {
    Optional<String> result = _sessionManager.getUsername("nonexistent-token-xyz");
    assertFalse(result.isPresent());
  }

  // ---------------------------------------------------------------------------
  // getUsername — expired session
  // ---------------------------------------------------------------------------

  @Test
  public void testGetUsernameAfterExpiryReturnsEmpty() throws InterruptedException {
    // Create a session with 1-second TTL
    SessionManager shortTtlManager = new InMemorySessionManager(1L);
    try {
      String token = shortTtlManager.createSession("bob", "Basic xyz");
      // Verify it works immediately
      assertTrue(shortTtlManager.getUsername(token).isPresent());
      // Wait for expiry
      Thread.sleep(1500);
      Optional<String> result = shortTtlManager.getUsername(token);
      assertFalse(result.isPresent(), "Session should be expired after TTL");
    } finally {
      shortTtlManager.shutdown();
    }
  }

  @Test
  public void testExpiredSessionIsNotAccessible() throws InterruptedException {
    // getUsername returns empty for expired sessions. Expired entries are NOT eagerly removed from
    // the store on access — they linger until the background sweep (every 30 minutes). This is
    // intentional: the map is only consulted for expiry checks, not for active garbage collection.
    SessionManager shortTtlManager = new InMemorySessionManager(1L);
    try {
      String token = shortTtlManager.createSession("bob", "Basic xyz");
      Thread.sleep(1500);
      Optional<String> result = shortTtlManager.getUsername(token);
      assertFalse(result.isPresent(), "Expired session should not be returned by getUsername");
    } finally {
      shortTtlManager.shutdown();
    }
  }

  // ---------------------------------------------------------------------------
  // Fixed TTL (no sliding window)
  // ---------------------------------------------------------------------------

  @Test
  public void testFixedTtlExpiresAtLoginPlusTimeout() throws InterruptedException {
    // TTL = 3 seconds. Session should be valid at t=1.5s but expired at t=3.5s.
    // Calling getUsername at t=1.5s must NOT extend the expiry — the fixed TTL
    // ensures server and browser cookie expiry are aligned from login time.
    SessionManager fixedManager = new InMemorySessionManager(3L);
    try {
      String token = fixedManager.createSession("carol", "Basic zzz");
      Thread.sleep(1500); // t=1.5s — still valid
      Optional<String> midResult = fixedManager.getUsername(token);
      assertTrue(midResult.isPresent(), "Session should still be valid at t=1.5s");
      // Without sliding TTL, session expires at t=3s regardless of the access above.
      Thread.sleep(2000); // t=3.5s — 500ms after expiry, generous margin for CI
      Optional<String> lateResult = fixedManager.getUsername(token);
      assertFalse(lateResult.isPresent(), "Session should have expired at loginTime + TTL");
    } finally {
      fixedManager.shutdown();
    }
  }

  // ---------------------------------------------------------------------------
  // invalidateSession
  // ---------------------------------------------------------------------------

  @Test
  public void testInvalidateSessionRemovesIt() {
    String token = _sessionManager.createSession("dave", "Basic aaa");
    assertTrue(_sessionManager.getUsername(token).isPresent());

    _sessionManager.invalidateSession(token);
    assertFalse(_sessionManager.getUsername(token).isPresent(), "Session should not be accessible after invalidation");
  }

  @Test
  public void testInvalidateSessionDecrementsCount() {
    String token = _sessionManager.createSession("dave", "Basic aaa");
    int before = _sessionManager.getActiveSessionCount();
    _sessionManager.invalidateSession(token);
    assertTrue(_sessionManager.getActiveSessionCount() < before);
  }

  @Test
  public void testInvalidateNonExistentTokenIsIdempotent() {
    // Should not throw
    _sessionManager.invalidateSession("ghost-token-that-does-not-exist");
    _sessionManager.invalidateSession(null);
    _sessionManager.invalidateSession("");
  }

  @Test
  public void testInvalidateSessionTwiceIsIdempotent() {
    String token = _sessionManager.createSession("dave", "Basic aaa");
    _sessionManager.invalidateSession(token);
    _sessionManager.invalidateSession(token); // second call should not throw
    assertFalse(_sessionManager.getUsername(token).isPresent());
  }

  // ---------------------------------------------------------------------------
  // getBasicAuthToken
  // ---------------------------------------------------------------------------

  @Test
  public void testGetBasicAuthTokenReturnsStoredToken() {
    String basicToken = "Basic dXNlcjpwYXNz";
    String sessionToken = _sessionManager.createSession("eve", basicToken);
    Optional<String> result = _sessionManager.getBasicAuthToken(sessionToken);
    assertTrue(result.isPresent());
    assertEquals(result.get(), basicToken);
  }

  @Test
  public void testGetBasicAuthTokenWithNullReturnsEmpty() {
    assertFalse(_sessionManager.getBasicAuthToken(null).isPresent());
  }

  @Test
  public void testGetBasicAuthTokenAfterInvalidationReturnsEmpty() {
    String sessionToken = _sessionManager.createSession("eve", "Basic abc");
    _sessionManager.invalidateSession(sessionToken);
    assertFalse(_sessionManager.getBasicAuthToken(sessionToken).isPresent());
  }

  // ---------------------------------------------------------------------------
  // Multiple sessions for the same user
  // ---------------------------------------------------------------------------

  @Test
  public void testMultipleSessionsForSameUser() {
    String token1 = _sessionManager.createSession("frank", "Basic aaa");
    String token2 = _sessionManager.createSession("frank", "Basic bbb");
    assertNotEquals(token1, token2);
    assertTrue(_sessionManager.getUsername(token1).isPresent());
    assertTrue(_sessionManager.getUsername(token2).isPresent());

    _sessionManager.invalidateSession(token1);
    assertFalse(_sessionManager.getUsername(token1).isPresent());
    assertTrue(_sessionManager.getUsername(token2).isPresent(), "Invalidating one session should not affect the other");
  }

  // ---------------------------------------------------------------------------
  // getSessionTtlSeconds
  // ---------------------------------------------------------------------------

  @Test
  public void testGetSessionTtlSeconds() {
    assertEquals(_sessionManager.getSessionTtlSeconds(), 10L);
    SessionManager custom = new InMemorySessionManager(42L);
    try {
      assertEquals(custom.getSessionTtlSeconds(), 42L);
    } finally {
      custom.shutdown();
    }
  }

  @Test
  public void testDefaultConstructorUsesFallbackTtl() {
    SessionManager defaultManager = new InMemorySessionManager();
    try {
      assertEquals(defaultManager.getSessionTtlSeconds(), SessionManager.DEFAULT_SESSION_TTL_SECONDS);
    } finally {
      defaultManager.shutdown();
    }
  }

  // ---------------------------------------------------------------------------
  // Concurrent access — no race condition
  // ---------------------------------------------------------------------------

  @Test
  public void testConcurrentGetUsernameIsRaceFree() throws InterruptedException {
    String token = _sessionManager.createSession("grace", "Basic ccc");
    int threadCount = 50;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger successCount = new AtomicInteger(0);
    List<Throwable> errors = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          Optional<String> result = _sessionManager.getUsername(token);
          if (result.isPresent()) {
            successCount.incrementAndGet();
          }
        } catch (Throwable t) {
          synchronized (errors) {
            errors.add(t);
          }
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown(); // release all threads simultaneously
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All threads should complete within 10s");
    executor.shutdown();

    assertTrue(errors.isEmpty(), "No exceptions thrown under concurrent access: " + errors);
    assertEquals(successCount.get(), threadCount,
        "All concurrent getUsername calls should succeed for a valid non-expired token");
  }

  @Test
  public void testConcurrentCreateAndInvalidate() throws InterruptedException {
    int count = 20;
    ExecutorService executor = Executors.newFixedThreadPool(count * 2);
    List<String> tokens = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(count * 2);

    // Create sessions
    for (int i = 0; i < count; i++) {
      final int idx = i;
      executor.submit(() -> {
        try {
          String tok = _sessionManager.createSession("user" + idx, "Basic tok" + idx);
          synchronized (tokens) {
            tokens.add(tok);
          }
        } finally {
          latch.countDown();
        }
      });
    }

    // Simultaneously invalidate (may operate on not-yet-created tokens — should not throw)
    for (int i = 0; i < count; i++) {
      executor.submit(() -> {
        try {
          synchronized (tokens) {
            if (!tokens.isEmpty()) {
              _sessionManager.invalidateSession(tokens.get(0));
            }
          }
        } finally {
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    executor.shutdown();
    // No assertion on count — just verify no exception was thrown
  }
}
