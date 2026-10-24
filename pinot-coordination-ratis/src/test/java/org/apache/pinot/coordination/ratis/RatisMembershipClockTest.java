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
package org.apache.pinot.coordination.ratis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.coordination.ServingPermitGuard.ClockReading;
import org.apache.pinot.coordination.ratis.RatisMetadataStore.MembershipClockSettings;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ActivateControlEpoch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Tick;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises proposal ownership and elapsed-time rules without depending on scheduler timing.
public class RatisMembershipClockTest {
  @Test
  public void testFullBarrierIntervalOneOutstandingTickAndNoCatchUp()
      throws Exception {
    Backend backend = new Backend();
    try (RatisMembershipClock clock = new RatisMembershipClock(backend, backend.settings(1_000_000, 0))) {
      clock.runOnce();
      assertTrue(backend.last() instanceof ActivateControlEpoch);
      backend.advance(1_000);
      clock.runOnce();
      assertEquals(backend._commands.size(), 1);
      backend.apply();
      clock.runOnce();
      backend.advance(99);
      clock.runOnce();
      assertEquals(backend._commands.size(), 1);
      backend.advance(1);
      clock.runOnce();
      assertEquals(((Tick) backend.last()).expectedRound(), 0L);
      backend.advance(20_000);
      clock.runOnce();
      assertEquals(backend._commands.size(), 2, "A paused request cannot create a second outstanding tick");
      backend.apply();
      clock.runOnce();
      assertEquals(backend._commands.size(), 2, "A delayed completion cannot cause a catch-up burst");
      backend.advance(100);
      clock.runOnce();
      assertEquals(((Tick) backend.last()).expectedRound(), 1L);
    }
  }

  @Test
  public void testUnknownOutcomeRetriesSameRoundAndNewTermNeedsBarrier()
      throws Exception {
    Backend backend = new Backend();
    try (RatisMembershipClock clock = new RatisMembershipClock(backend, backend.settings(1_000_000, 0))) {
      clock.runOnce();
      backend.apply();
      clock.runOnce();
      backend.advance(100);
      clock.runOnce();
      Tick first = (Tick) backend.last();
      backend._pending.completeExceptionally(new IOException("Lost reply"));
      clock.runOnce();
      backend.advance(99);
      clock.runOnce();
      assertEquals(backend._commands.size(), 2);
      backend.advance(1);
      clock.runOnce();
      assertEquals(backend.last(), first, "An unknown outcome can only retry the same expected round");
      backend._term = 2;
      backend.advance(10_000);
      clock.runOnce();
      assertEquals(backend._commands.size(), 3, "Retain ownership across a term change until terminal");
      backend._pending.completeExceptionally(new IOException("Fenced old term"));
      clock.runOnce();
      assertEquals(((ActivateControlEpoch) backend.last()).term(), 2L);
      backend.apply();
      clock.runOnce();
      backend.advance(99);
      clock.runOnce();
      assertTrue(backend.last() instanceof ActivateControlEpoch);
      backend.advance(1);
      clock.runOnce();
      assertEquals(((Tick) backend.last()).control().raftTerm(), 2L);
      backend.apply();
      backend._ready = false;
      clock.runOnce();
      backend.advance(1_000);
      clock.runOnce();
      assertEquals(backend._commands.size(), 5, "Followers never drive the clock");
    }
  }

  @Test
  public void testPolicyGraceAndLeaderChangeRestartFullQualifiedInterval()
      throws Exception {
    Backend backend = new Backend();
    try (RatisMembershipClock clock = new RatisMembershipClock(backend, backend.settings(1_000_000, 0))) {
      clock.runOnce();
      backend.apply();
      backend._observeDelayMillis = 500;
      clock.runOnce();
      backend.advance(99);
      clock.runOnce();
      assertEquals(backend._commands.size(), 1, "Time spent capturing a new barrier cannot age its interval");
      backend._control = new ControlState(backend._control.controlEpoch(), 0,
          new Policy(2, 20, 5, 50, 20, 10), ++backend._index, 300, 50);
      clock.runOnce();
      backend.advance(299);
      clock.runOnce();
      assertEquals(backend._commands.size(), 1, "A shorter policy interval cannot bypass the persisted grace delay");
      backend._term = 2;
      clock.runOnce();
      backend.apply();
      clock.runOnce();
      backend.advance(299);
      clock.runOnce();
      assertTrue(backend.last() instanceof ActivateControlEpoch, "The replacement leader waits the entire grace delay");
      backend.advance(1);
      clock.runOnce();
      assertEquals(((Tick) backend.last()).policyVersion(), 2L);
      assertEquals(((Tick) backend.last()).control().raftTerm(), 2L);
      backend.apply();
      clock.runOnce();
      backend.advance(19);
      clock.runOnce();
      assertEquals(backend._commands.size(), 3);
      backend.advance(1);
      clock.runOnce();
      assertEquals(backend._commands.size(), 4, "The completed grace permits the new policy interval");
    }
  }

  @Test
  public void testUncertaintyAndMaximumClockRateNeverRoundTheWaitDown() throws Exception {
    Backend backend = new Backend();
    backend._uncertaintyNanos = TimeUnit.MILLISECONDS.toNanos(10);
    try (RatisMembershipClock clock = new RatisMembershipClock(backend,
        backend.settings(1_000_001, backend._uncertaintyNanos))) {
      clock.runOnce();
      backend.apply();
      clock.runOnce();
      clock.runOnce();
      assertNull(clock.failure(), "Overlapping clock intervals are not a central-clock regression");
      backend._now = 120_000_099;
      clock.runOnce();
      assertEquals(backend._commands.size(), 1);
      backend._now++;
      clock.runOnce();
      assertTrue(backend.last() instanceof Tick, "Both uncertainty endpoints and the fractional rate bound apply");
    }
  }

  @DataProvider
  public Object[][] invalidClocks() {
    return new Object[][]{{"regression"}, {"untrusted"}, {"uncertainty"}, {"provider"}, {"endpointOverflow"}};
  }

  @Test(dataProvider = "invalidClocks")
  public void testQualificationFailureIsTerminalWithoutCancellingPendingProposal(String failure) throws Exception {
    Backend backend = new Backend();
    try (RatisMembershipClock clock = new RatisMembershipClock(backend, backend.settings(1_000_000, 10))) {
      clock.runOnce();
      switch (failure) {
        case "regression" -> backend._now = -1;
        case "untrusted" -> backend._trusted = false;
        case "uncertainty" -> backend._uncertaintyNanos = 11;
        case "provider" -> backend._clockThrows = true;
        case "endpointOverflow" -> {
          backend._now = Long.MAX_VALUE;
          backend._uncertaintyNanos = 1;
        }
        default -> throw new AssertionError(failure);
      }
      expectThrows(IOException.class, clock::runOnce);
      String reason = clock.failure();
      assertNotNull(reason);
      assertFalse(backend._pending.isCancelled(), "Qualification loss cannot abandon native request ownership");
      backend._now = 0;
      backend._trusted = true;
      backend._uncertaintyNanos = 0;
      backend._clockThrows = false;
      backend.apply();
      backend.advance(1_000);
      clock.runOnce();
      assertEquals(clock.failure(), reason);
      assertEquals(backend._commands.size(), 1, "Apparent clock recovery must not restart a fenced driver");
    }
  }

  @Test
  public void testQualifiedDelayAndElapsedArithmeticOverflowAreTerminal() throws Exception {
    for (boolean elapsedOverflow : List.of(true, false)) {
      Backend backend = new Backend();
      backend._now = elapsedOverflow ? Long.MIN_VALUE : 0;
      long rate = elapsedOverflow ? 1_000_000 : Long.MAX_VALUE;
      try (RatisMembershipClock clock = new RatisMembershipClock(backend, backend.settings(rate, 0))) {
        clock.runOnce();
        backend.apply();
        clock.runOnce();
        backend._now = Long.MAX_VALUE;
        expectThrows(IOException.class, clock::runOnce);
        assertNotNull(clock.failure());
        clock.runOnce();
        assertEquals(backend._commands.size(), 1, "Overflow cannot produce a tick or restart its interval");
      }
    }
  }

  private static final class Backend implements RatisMembershipClock.Backend {
    private final UUID _history = UUID.randomUUID();
    private final List<Operation> _commands = new ArrayList<>();
    private ControlState _control = new ControlState(null, 0, new Policy(1, 100, 5, 50, 20, 10), 0, 100, 50);
    private CompletableFuture<Void> _pending;
    private long _term = 1;
    private long _now;
    private long _index;
    private long _observeDelayMillis;
    private long _uncertaintyNanos;
    private boolean _ready = true;
    private boolean _trusted = true;
    private boolean _clockThrows;

    MembershipClockSettings settings(long maximumRate, long maximumUncertainty) {
      return new MembershipClockSettings(() -> {
        if (_clockThrows) {
          throw new IllegalStateException("Clock provider failed");
        }
        return new ClockReading(_now, _uncertaintyNanos, _trusted);
      }, maximumRate, maximumUncertainty);
    }

    @Override
    public RatisMembershipClock.Observation observe() {
      advance(_observeDelayMillis);
      _observeDelayMillis = 0;
      return new RatisMembershipClock.Observation(_ready, _term, "voter", _control);
    }

    @Override
    public CompletableFuture<?> submit(Operation operation) {
      _commands.add(operation);
      _pending = new CompletableFuture<>();
      return _pending;
    }

    void apply() {
      _index++;
      if (last() instanceof ActivateControlEpoch activation) {
        _control = new ControlState(new ControlToken(_history, activation.term(), _index, activation.leaderId(),
            activation.activationId()), _control.round(), _control.policy(), _index,
            _control.minimumTickDelayMillis(), _control.maxConfiguredPermitLifetimeMillis());
      } else {
        Tick tick = (Tick) last();
        assertEquals(tick.expectedRound(), _control.round());
        _control = new ControlState(_control.controlEpoch(), _control.round() + 1, _control.policy(), _index,
            _control.policy().minTickIntervalMillis(), _control.policy().permitLifetimeMillis());
      }
      _pending.complete(null);
    }

    Operation last() {
      return _commands.get(_commands.size() - 1);
    }

    void advance(long millis) {
      _now += TimeUnit.MILLISECONDS.toNanos(millis);
    }

    @Override
    public void close() {
    }
  }
}
