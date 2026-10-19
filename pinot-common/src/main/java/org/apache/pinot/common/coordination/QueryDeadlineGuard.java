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
package org.apache.pinot.common.coordination;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.NativeQueryDeadline;

import static com.google.common.base.Preconditions.checkArgument;


/// Enforces an immutable query deadline using a deployment-qualified shared time interval. There is no default clock
/// or qualification policy. The provider must return a fresh, conservative interval containing the shared time when
/// sampled during that invocation, account for host suspension and detect lost synchronization. Cached synchronization
/// data must be extrapolated into a current conservative interval; a cached old time interval is insufficient.
/// The domain must also fix leap/smear behavior.
/// Callers must recheck after resuming and before each external result delivery. Use [ServingPermitGuard] to combine
/// this bound with a fixed process-local deadline; an absolute check alone cannot bridge pauses between clock reads.
///
/// Loss of clock qualification permanently invalidates this guard. A bad or expired query only rejects that query.
public final class QueryDeadlineGuard {
  private final BoundedClock _clock;
  private final String _clockDomain;
  private final long _maxQueryLifetimeMillis;
  private final long _maxUncertaintyMillis;
  private final int _localMinimumRatePpm;
  @Nullable
  private String _invalidReason;

  /// Local, nonblocking observation of the configured shared time domain; must not perform an RPC on the query path.
  @FunctionalInterface
  public interface BoundedClock {
    TimeInterval read();
  }

  /// Conservative earliest/latest shared time in milliseconds. The configured uncertainty bound limits the interval
  /// width, not its half-width. A provider must round outward when converting a finer-grained time/error estimate.
  public static final class TimeInterval {
    private final long _earliestMillis;
    private final long _latestMillis;
    private final boolean _qualified;

    public TimeInterval(long earliestMillis, long latestMillis, boolean qualified) {
      _earliestMillis = earliestMillis;
      _latestMillis = latestMillis;
      _qualified = qualified;
    }

    public long earliestMillis() {
      return _earliestMillis;
    }

    public long latestMillis() {
      return _latestMillis;
    }

    public boolean qualified() {
      return _qualified;
    }
  }

  /// `localMinimumRatePpm` is the qualified minimum process-local clock rate relative to this shared time domain,
  /// in parts per million. It applies continuously through pauses, suspension and the domain's leap/smear or clock
  /// adjustment behavior; unbounded forward steps cannot meet this contract. Merely configuring it does not establish
  /// that the platform meets the bound; callers must supply a qualified provider and deployment-specific evidence.
  public QueryDeadlineGuard(BoundedClock clock, String clockDomain, long maxQueryLifetimeMillis,
      long maxUncertaintyMillis, int localMinimumRatePpm) {
    _clock = Objects.requireNonNull(clock, "clock");
    _clockDomain = new NativeQueryDeadline(clockDomain, 0, 1).clockDomain();
    checkArgument(maxQueryLifetimeMillis > 0 && maxUncertaintyMillis >= 0
        && localMinimumRatePpm > 0 && localMinimumRatePpm <= 1_000_000, "Invalid qualified query deadline policy");
    _maxQueryLifetimeMillis = maxQueryLifetimeMillis;
    _maxUncertaintyMillis = maxUncertaintyMillis;
    _localMinimumRatePpm = localMinimumRatePpm;
  }

  /// Originates once at the authenticated query entry point. Receivers and retries reuse the returned value.
  public synchronized NativeQueryDeadline originate(long requestedMillis) {
    checkArgument(requestedMillis > 0, "A positive query lifetime is required");
    TimeInterval now = readClock();
    long duration = Math.min(requestedMillis, _maxQueryLifetimeMillis);
    if (duration > Long.MAX_VALUE - now.earliestMillis()) {
      throw new IllegalStateException("Query deadline overflows the shared clock domain");
    }
    NativeQueryDeadline deadline = new NativeQueryDeadline(_clockDomain, now.earliestMillis(),
        now.earliestMillis() + duration);
    remaining(deadline, now);
    return deadline;
  }

  /// Returns a conservative remaining budget at this observation, never a fresh requested duration. A later local
  /// ticket must anchor its conversion to a monotonic sample taken before this method, not after a possible pause.
  public synchronized long remainingMillis(NativeQueryDeadline deadline) {
    Objects.requireNonNull(deadline, "deadline");
    if (!_clockDomain.equals(deadline.clockDomain())
        || deadline.notAfterMillis() - deadline.originLowerMillis() > _maxQueryLifetimeMillis) {
      throw new IllegalStateException("Native query deadline has an incompatible clock domain or lifetime");
    }
    return remaining(deadline, readClock());
  }

  public void check(NativeQueryDeadline deadline) {
    remainingMillis(deadline);
  }

  public int localMinimumRatePpm() {
    return _localMinimumRatePpm;
  }

  private long remaining(NativeQueryDeadline deadline, TimeInterval now) {
    if (deadline.originLowerMillis() > now.latestMillis() || now.latestMillis() >= deadline.notAfterMillis()) {
      throw new IllegalStateException("Native query deadline is expired or its origin is in the future");
    }
    return deadline.notAfterMillis() - now.latestMillis();
  }

  private TimeInterval readClock() {
    if (_invalidReason != null) {
      throw new IllegalStateException(_invalidReason);
    }
    TimeInterval now;
    try {
      now = _clock.read();
    } catch (RuntimeException e) {
      _invalidReason = "Qualified shared query clock is unavailable";
      throw new IllegalStateException(_invalidReason, e);
    }
    if (now == null || !now.qualified() || now.earliestMillis() < 0 || now.latestMillis() < now.earliestMillis()
        || now.latestMillis() - now.earliestMillis() > _maxUncertaintyMillis) {
      _invalidReason = "Shared query clock lost its qualification";
      throw new IllegalStateException(_invalidReason);
    }
    return now;
  }
}
