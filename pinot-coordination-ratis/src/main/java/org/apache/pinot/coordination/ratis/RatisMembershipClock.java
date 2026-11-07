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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.RatisMetadataStore.MembershipClockSettings;
import org.apache.pinot.coordination.ratis.client.ServingPermitGuard.ClockReading;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ActivateControlEpoch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Drives the replicated logical clock only while the local voter is leader-ready. The committed barrier and every
/// tick start a fresh full interval; pauses never cause catch-up bursts. A single native request remains owned until
/// terminal completion, including lost replies. Actual entry terms and expected rounds fence delayed proposals.
/// Lost clock qualification permanently stops this driver; it never substitutes a clock or cancels a pending request.
final class RatisMembershipClock implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RatisMembershipClock.class);
  private final Backend _backend;
  private final MembershipClockSettings _settings;
  private final ScheduledExecutorService _executor;
  private CompletableFuture<?> _pending;
  private long _term = -1;
  private UUID _activationId;
  private long _barrierIndex = -1;
  private long _intervalStartUpper;
  private boolean _clockRead;
  private long _lastClockNanos;
  @Nullable
  private String _failure;
  private boolean _closed;

  RatisMembershipClock(Backend backend, MembershipClockSettings settings) {
    _backend = Objects.requireNonNull(backend, "backend");
    _settings = Objects.requireNonNull(settings, "settings");
    _executor = Executors.newSingleThreadScheduledExecutor(task -> {
      Thread thread = new Thread(task, "pinot-ratis-membership-clock");
      thread.setDaemon(true);
      return thread;
    });
  }

  void start() {
    // Fixed delay schedules observations, not logical ticks; policy supplies the actual minimum tick interval.
    _executor.scheduleWithFixedDelay(this::poll, 0, 100, TimeUnit.MILLISECONDS);
  }

  private void poll() {
    try {
      runOnce();
    } catch (IOException | RuntimeException e) {
      LOGGER.warn("Cannot advance the membership clock; no uncommitted lease authority is granted", e);
    }
  }

  @VisibleForTesting
  synchronized void runOnce()
      throws IOException {
    if (_closed || _failure != null) {
      return;
    }
    // Check qualification even while a previously issued proposal is outstanding, without cancelling its future.
    readClock();
    boolean completed = false;
    if (_pending != null) {
      if (!_pending.isDone()) {
        return;
      }
      try {
        _pending.join();
      } catch (CompletionException e) {
        LOGGER.debug("Membership clock request ended without a successful reply; checking committed state", e);
      }
      _pending = null;
      // An unknown outcome can only retry the same expected round until local apply proves progress. Wait a full
      // interval even after failure rather than allowing a stream of immediately failed retries.
      completed = true;
    }
    Observation observation = _backend.observe();
    ClockInterval now = readClock();
    if (completed) {
      _intervalStartUpper = now.upper();
    }
    if (!observation.leaderReady()) {
      _term = -1;
      _barrierIndex = -1;
      return;
    }
    if (_term != observation.term()) {
      _term = observation.term();
      _activationId = UUID.randomUUID();
      _barrierIndex = -1;
      _intervalStartUpper = now.upper();
    }
    ControlState state = observation.control();
    ControlToken control = state.controlEpoch();
    if (control == null || control.raftTerm() != _term) {
      _pending = _backend.submit(new ActivateControlEpoch(_term, observation.voterId(), _activationId));
      return;
    }
    // A different identity for the same term is a configuration/control-plane error, never authority to tick.
    if (!control.leaderId().equals(observation.voterId())) {
      return;
    }
    if (_barrierIndex != state.clockBarrierIndex()) {
      _barrierIndex = state.clockBarrierIndex();
      _intervalStartUpper = now.upper();
      return;
    }
    if (state.policy() == null) {
      return;
    }
    if (state.minimumTickDelayMillis() <= 0) {
      throw fail("Membership control has no positive minimum tick delay", null);
    }
    long elapsed;
    long required;
    try {
      elapsed = Math.subtractExact(now.lower(), _intervalStartUpper);
      // Milliseconds-to-nanoseconds and parts-per-million cancel exactly, without rounding down the real delay.
      required = Math.multiplyExact(state.minimumTickDelayMillis(), _settings.maxClockRatePpm());
    } catch (ArithmeticException e) {
      throw fail("Membership clock interval or qualified delay overflow", e);
    }
    if (elapsed >= required) {
      _intervalStartUpper = now.upper();
      _pending = _backend.submit(new Tick(control, state.round(), state.policy().version()));
    }
  }

  @Nullable
  synchronized String failure() {
    return _failure;
  }

  private ClockInterval readClock()
      throws IOException {
    ClockReading now;
    try {
      now = _settings.clock().read();
    } catch (RuntimeException e) {
      throw fail("Membership clock provider is unavailable", e);
    }
    if (now == null || !now.trusted() || now.uncertaintyNanos() < 0
        || now.uncertaintyNanos() > _settings.maxClockUncertaintyNanos()
        || _clockRead && now.nanos() < _lastClockNanos) {
      throw fail("Membership clock regressed or lost qualification", null);
    }
    ClockInterval interval;
    try {
      interval = new ClockInterval(Math.subtractExact(now.nanos(), now.uncertaintyNanos()),
          Math.addExact(now.nanos(), now.uncertaintyNanos()));
    } catch (ArithmeticException e) {
      throw fail("Membership clock uncertainty interval overflow", e);
    }
    _clockRead = true;
    _lastClockNanos = now.nanos();
    return interval;
  }

  private IOException fail(String reason, @Nullable Throwable cause) {
    _failure = reason;
    return new IOException(reason, cause);
  }

  private record ClockInterval(long lower, long upper) {
  }

  @Override
  public void close()
      throws IOException {
    synchronized (this) {
      if (_closed) {
        return;
      }
      _closed = true;
    }
    _executor.shutdown();
    // Do not cancel a Ratis future: cancellation can bypass its internal admission-permit cleanup.
    _backend.close();
  }

  record Observation(boolean leaderReady, long term, String voterId, ControlState control) {
  }

  interface Backend extends AutoCloseable {
    Observation observe()
        throws IOException;

    CompletableFuture<?> submit(Operation operation)
        throws IOException;

    @Override
    void close()
        throws IOException;
  }
}
