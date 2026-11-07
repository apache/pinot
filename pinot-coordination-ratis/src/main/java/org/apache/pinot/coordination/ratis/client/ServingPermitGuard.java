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
package org.apache.pinot.coordination.ratis.client;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ServingPermit;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;


/// Local admission and result-delivery fencing for read-only serving permits. All state transitions are synchronized;
/// the supplied clock and coherent membership-view supplier must be local, nonblocking and perform no RPCs. Normal
/// role integration must check a ticket again after every resume and immediately before delivering a result.
///
/// The guard binds one participant registration for its lifetime. Retries never move a renewal's first-send time.
/// New grants replace previous capabilities immediately, including while waiting for their minimum image revision.
/// Tickets retain their original finite deadline across renewals and cannot be transferred to another guard/process.
/// This is not a cross-host query-deadline protocol, and no permit authorizes metadata or segment mutations.
public final class ServingPermitGuard {
  private static final long NANOS_PER_MILLI = 1_000_000;

  private final ParticipantToken _participant;
  private final Set<Capability> _allowedCapabilities;
  private final MonotonicClock _clock;
  private final QualifiedLifetimePolicy _policy;
  private final Supplier<MembershipView> _view;
  private final Object _ticketOwner = new Object();
  private final Map<Capability, Long> _capabilityEpochs = new EnumMap<>(Capability.class);
  private boolean _clockRead;
  private long _lastClockNanos;
  private long _lastSentSequence;
  private long _firstSendLowerBound;
  private long _controlGeneration;
  private long _lifetimeGeneration;
  private boolean _draining;
  @Nullable
  private String _invalidReason;
  @Nullable
  private ControlToken _control;
  @Nullable
  private ServingPermit _lastAccepted;
  @Nullable
  private ServingPermit _permit;
  @Nullable
  private MembershipView _lastView;
  private long _permitDeadline;

  /// A qualified provider must advance through process pauses and host suspension, detect lost clock qualification,
  /// and report a conservative absolute error bound. There is deliberately no built-in production clock.
  @FunctionalInterface
  public interface MonotonicClock {
    ClockReading read();
  }

  public record ClockReading(long nanos, long uncertaintyNanos, boolean trusted) {
  }

  /// Deployment-qualified limits, with no defaults. `safeLifetimeFractionPpm` conservatively converts authority
  /// lifetimes into this clock's units, including the qualified relative clock-rate bound. `safetyMarginMillis`
  /// subtracts an additional deployment-approved margin. Callers must establish those bounds for the platform;
  /// merely constructing this record does not qualify a clock or make arbitrary values safe.
  public record QualifiedLifetimePolicy(long maxPermitLifetimeMillis, long maxQueryLifetimeMillis,
                                        int safeLifetimeFractionPpm, long safetyMarginMillis,
                                        long maxClockUncertaintyNanos) {
    public QualifiedLifetimePolicy {
      if (maxPermitLifetimeMillis <= 0 || maxQueryLifetimeMillis <= 0 || safeLifetimeFractionPpm <= 0
          || safeLifetimeFractionPpm > NANOS_PER_MILLI || safetyMarginMillis < 0
          || maxClockUncertaintyNanos < 0 || maxPermitLifetimeMillis > Long.MAX_VALUE / NANOS_PER_MILLI
          || maxQueryLifetimeMillis > Long.MAX_VALUE / NANOS_PER_MILLI
          || safetyMarginMillis >= Math.min(maxPermitLifetimeMillis, maxQueryLifetimeMillis)
          || Math.min(maxPermitLifetimeMillis, maxQueryLifetimeMillis) * safeLifetimeFractionPpm
          <= safetyMarginMillis * NANOS_PER_MILLI) {
        throw new IllegalArgumentException("Invalid qualified serving lifetime policy");
      }
    }
  }

  /// One coherent local image projection. `live` must include expiry-round comparison, even before cleanup marks a
  /// participant expired. A failed/uninitialized cache must make the supplier fail, never return a stale success.
  public record MembershipView(Revision revision, @Nullable ControlToken control,
                               @Nullable ParticipantToken participant, boolean live, boolean ready, boolean draining,
                               Set<Capability> capabilities, long policyVersion) {
    public MembershipView {
      capabilities = Set.copyOf(capabilities);
    }

    /// Decode once when publishing an image update, not on the query path. The containing cache must separately
    /// report failed/stale hydration through the view supplier; retaining this object does not prove cache health.
    public static MembershipView fromImage(MetadataImage image, ParticipantToken expected)
        throws IOException {
      CatalogRecord controlRecord = image.records(RecordKind.MEMBERSHIP_CONTROL).get(MembershipProtocol.CONTROL_NAME);
      CatalogRecord participantRecord = image.records(RecordKind.PARTICIPANT).get(expected.participantId());
      ControlState control = controlRecord == null ? null : MembershipProtocol.decodeControl(controlRecord.value());
      ParticipantState participant = participantRecord == null ? null
          : MembershipProtocol.decodeParticipant(participantRecord.value());
      boolean initialized = control != null && control.controlEpoch() != null && control.policy() != null;
      boolean live = initialized && participant != null && !participant.retired()
          && participant.expiryRound() > control.round();
      return new MembershipView(new Revision(image.cursor().historyId(), image.cursor().index()),
          initialized ? control.controlEpoch() : null, participant == null ? null : participant.participant(), live,
          participant != null && participant.ready(), participant != null && participant.draining(),
          participant == null ? Set.of() : participant.capabilities(), initialized ? control.policy().version() : 0);
    }
  }

  /// Opaque local ticket. It is neither serializable nor reconstructible by a caller, and renewal cannot extend it.
  public static final class QueryTicket {
    private final Object _owner;
    private final Capability _capability;
    private final long _deadline;
    private final long _controlGeneration;
    private final long _capabilityEpoch;
    private final long _lifetimeGeneration;
    private final long _policyVersion;

    private QueryTicket(Object owner, Capability capability, long deadline, long controlGeneration,
        long capabilityEpoch, long lifetimeGeneration, long policyVersion) {
      _owner = owner;
      _capability = capability;
      _deadline = deadline;
      _controlGeneration = controlGeneration;
      _capabilityEpoch = capabilityEpoch;
      _lifetimeGeneration = lifetimeGeneration;
      _policyVersion = policyVersion;
    }
  }

  public ServingPermitGuard(ParticipantToken participant, Set<Capability> allowedCapabilities, MonotonicClock clock,
      QualifiedLifetimePolicy policy, Supplier<MembershipView> view) {
    _participant = Objects.requireNonNull(participant, "participant");
    _allowedCapabilities = Set.copyOf(allowedCapabilities);
    _clock = Objects.requireNonNull(clock, "clock");
    _policy = Objects.requireNonNull(policy, "policy");
    _view = Objects.requireNonNull(view, "view");
    if (participant.historyId() == null || participant.incarnation() == null || participant.participantId() == null
        || participant.participantId().isBlank() || participant.role() == null
        || participant.registrationEpoch() <= 0) {
      throw new IllegalArgumentException("A committed participant registration is required");
    }
    for (Capability capability : Capability.values()) {
      _capabilityEpochs.put(capability, 0L);
    }
  }

  /// Must run immediately before the first transmission of each logical renewal. A retry calls this with the same
  /// sequence and retains the original timestamp even after leadership changes. Unsent responses cannot activate.
  public synchronized void firstSend(long sequence) {
    ClockReading now = readClock();
    if (sequence == _lastSentSequence && sequence > 0) {
      return;
    }
    require(_lastSentSequence < Long.MAX_VALUE && sequence == _lastSentSequence + 1,
        "Renewal sends must use the next logical sequence or retry the current one");
    _lastSentSequence = sequence;
    _firstSendLowerBound = subtract(now.nanos(), now.uncertaintyNanos());
  }

  /// Stages an authenticated grant. Admission independently requires its minimum coherent image revision. Exact
  /// duplicate and older grants cannot refresh a deadline; a conflicting response for one sequence fails closed.
  public synchronized void accept(ServingPermit grant) {
    readClock();
    Objects.requireNonNull(grant, "grant");
    require(_participant.equals(grant.participant()), "Serving grant belongs to another participant registration");
    if (_lastAccepted != null && grant.renewalSequence() <= _lastAccepted.renewalSequence()) {
      if (grant.renewalSequence() == _lastAccepted.renewalSequence() && !grant.equals(_lastAccepted)) {
        invalidate("Conflicting grant for an already accepted renewal");
        deny(_invalidReason);
      }
      return;
    }
    require(grant.renewalSequence() > 0 && grant.renewalSequence() == _lastSentSequence,
        "Serving grant does not match the latest locally sent renewal");
    require(grant.controlEpoch() != null && grant.minAppliedRevision() != null
        && _participant.historyId().equals(grant.controlEpoch().historyId())
        && _participant.historyId().equals(grant.minAppliedRevision().historyId())
        && grant.controlEpoch().raftTerm() > 0 && grant.controlEpoch().barrierIndex() > 0
        && grant.controlEpoch().activationId() != null && grant.controlEpoch().leaderId() != null
        && !grant.controlEpoch().leaderId().isBlank()
        && grant.minAppliedRevision().index() >= grant.controlEpoch().barrierIndex() && grant.policyVersion() > 0
        && grant.permitLifetimeMillis() > 0 && grant.maxQueryLifetimeMillis() > 0
        && grant.capabilities() != null && _allowedCapabilities.containsAll(grant.capabilities()),
        "Invalid serving grant or capabilities outside the configured read-only role");
    require(observeControl(grant.controlEpoch()), "Serving grant belongs to an older control epoch");
    if (_permit != null) {
      for (Capability capability : _permit.capabilities()) {
        if (!grant.capabilities().contains(capability)) {
          _capabilityEpochs.put(capability, add(_capabilityEpochs.get(capability), 1));
        }
      }
    }
    if (_lastAccepted != null && (grant.permitLifetimeMillis() < _lastAccepted.permitLifetimeMillis()
        || grant.maxQueryLifetimeMillis() < _lastAccepted.maxQueryLifetimeMillis())) {
      _lifetimeGeneration = add(_lifetimeGeneration, 1);
    }
    _lastAccepted = grant;
    // A newer grant revokes prior authority even if its lifetime is too short for this deployment's safety margin.
    _permit = null;
    long lifetime = safeLifetime(Math.min(grant.permitLifetimeMillis(), _policy.maxPermitLifetimeMillis()));
    long deadline = add(_firstSendLowerBound, lifetime);
    _permit = grant;
    _permitDeadline = deadline;
  }

  public synchronized QueryTicket admit(Capability capability) {
    ClockReading now = readClock();
    ServingPermit permit = checkServing(capability, now, true);
    // A process pause can occur while the local view is read. Sample again after that validation before deciding.
    now = readClock();
    long lifetime = safeLifetime(Math.min(permit.maxQueryLifetimeMillis(), _policy.maxQueryLifetimeMillis()));
    long deadline = Math.min(_permitDeadline, add(subtract(now.nanos(), now.uncertaintyNanos()), lifetime));
    require(add(now.nanos(), now.uncertaintyNanos()) < deadline, "Insufficient qualified time for query admission");
    return new QueryTicket(_ticketOwner, capability, deadline, _controlGeneration, _capabilityEpochs.get(capability),
        _lifetimeGeneration, permit.policyVersion());
  }

  /// Check after resuming work and immediately before result delivery. Draining preserves already admitted work
  /// only within its original deadline and while the current image, control epoch and capability remain valid.
  public synchronized void checkDelivery(QueryTicket ticket) {
    ClockReading now = readClock();
    require(ticket != null && ticket._owner == _ticketOwner, "Query ticket belongs to another guard or process");
    checkServing(ticket._capability, now, false);
    require(ticket._controlGeneration == _controlGeneration
        && ticket._capabilityEpoch == _capabilityEpochs.get(ticket._capability)
        && ticket._lifetimeGeneration == _lifetimeGeneration && ticket._policyVersion == _permit.policyVersion(),
        "Query ticket was fenced or revoked");
    now = readClock();
    require(add(now.nanos(), now.uncertaintyNanos()) < Math.min(ticket._deadline, _permitDeadline),
        "Query delivery deadline expired");
  }

  /// Stops new admissions; existing tickets remain subject to every delivery check and their finite deadlines.
  public synchronized void drain() {
    _draining = true;
  }

  /// Terminal revocation. Recovery after lost clock qualification or participant fencing requires a new guard.
  public synchronized void invalidate() {
    invalidate("Serving guard explicitly invalidated");
  }

  private ServingPermit checkServing(Capability capability, ClockReading now, boolean admission) {
    require(_permit != null, "No serving permit is active");
    ServingPermit permit = _permit;
    require(capability != null && _allowedCapabilities.contains(capability)
        && permit.capabilities().contains(capability),
        "Serving capability is not currently granted");
    MembershipView view;
    try {
      view = _view.get();
    } catch (RuntimeException e) {
      throw new ServingDeniedException("Coherent membership image is unavailable", e);
    }
    require(view != null && view.revision() != null, "Coherent membership image is unavailable");
    if (!_participant.historyId().equals(view.revision().historyId())) {
      invalidate("Metadata history changed");
      deny(_invalidReason);
    }
    require(view.revision().index() >= 0 && (_lastView == null
        || view.revision().index() >= _lastView.revision().index()), "Coherent membership image regressed");
    if (_lastView != null && view.revision().equals(_lastView.revision()) && !view.equals(_lastView)) {
      invalidate("Conflicting membership projection for one image revision");
      deny(_invalidReason);
    }
    _lastView = view;
    if (view.control() != null) {
      require(observeControl(view.control()), "Membership image has an older control epoch");
    }
    require(_permit == permit && permit.controlEpoch().equals(view.control()), "Serving control epoch changed");
    require(view.revision().index() >= permit.minAppliedRevision().index(), "Membership image has not caught up");
    if (!_participant.equals(view.participant())) {
      invalidate("Participant incarnation or registration epoch changed");
      deny(_invalidReason);
    }
    require(view.live() && (view.ready() || !admission && view.draining()),
        "Participant lease expired or participant is not ready");
    require(view.capabilities().contains(capability) && view.policyVersion() == permit.policyVersion(),
        "Participant capability or serving policy changed");
    require(!admission || !_draining && !view.draining(), "Participant is draining");
    require(add(now.nanos(), now.uncertaintyNanos()) < _permitDeadline, "Serving permit expired");
    return permit;
  }

  private boolean observeControl(ControlToken control) {
    if (_control == null) {
      _control = control;
      _controlGeneration++;
      return true;
    }
    if (_control.equals(control)) {
      return true;
    }
    int term = Long.compare(control.raftTerm(), _control.raftTerm());
    int barrier = Long.compare(control.barrierIndex(), _control.barrierIndex());
    if (term < 0 || term == 0 && barrier < 0) {
      return false;
    }
    if (barrier <= 0 || !_participant.historyId().equals(control.historyId())) {
      invalidate("Conflicting control epoch");
      deny(_invalidReason);
    }
    _control = control;
    _controlGeneration = add(_controlGeneration, 1);
    _permit = null;
    return true;
  }

  private ClockReading readClock() {
    require(_invalidReason == null, _invalidReason == null ? "Serving guard invalidated" : _invalidReason);
    ClockReading now;
    try {
      now = _clock.read();
    } catch (RuntimeException e) {
      invalidate("Qualified monotonic clock is unavailable");
      throw new ServingDeniedException(_invalidReason, e);
    }
    if (now == null || !now.trusted() || now.uncertaintyNanos() < 0
        || now.uncertaintyNanos() > _policy.maxClockUncertaintyNanos()
        || _clockRead && now.nanos() < _lastClockNanos) {
      invalidate("Monotonic clock regressed or lost its qualification");
      deny(_invalidReason);
    }
    _clockRead = true;
    _lastClockNanos = now.nanos();
    // A wrapped uncertainty interval cannot establish an expiry decision on this process-local time axis.
    subtract(now.nanos(), now.uncertaintyNanos());
    add(now.nanos(), now.uncertaintyNanos());
    return now;
  }

  private long safeLifetime(long millis) {
    long nanos = subtract(Math.multiplyExact(millis, _policy.safeLifetimeFractionPpm()),
        Math.multiplyExact(_policy.safetyMarginMillis(), NANOS_PER_MILLI));
    require(nanos > 0, "No positive lifetime remains under the qualified safety policy");
    return nanos;
  }

  private long add(long first, long second) {
    try {
      return Math.addExact(first, second);
    } catch (ArithmeticException e) {
      invalidate("Monotonic time or local fence counter overflow");
      throw new ServingDeniedException(_invalidReason, e);
    }
  }

  private long subtract(long first, long second) {
    try {
      return Math.subtractExact(first, second);
    } catch (ArithmeticException e) {
      invalidate("Monotonic time overflow");
      throw new ServingDeniedException(_invalidReason, e);
    }
  }

  private void invalidate(String reason) {
    _invalidReason = reason;
    _permit = null;
  }

  private static void require(boolean condition, String reason) {
    if (!condition) {
      deny(reason);
    }
  }

  private static void deny(String reason) {
    throw new ServingDeniedException(reason);
  }

  /// Expected local refusal to admit or deliver query work; callers must stop rather than reuse an older permit.
  public static final class ServingDeniedException extends IllegalStateException {
    private ServingDeniedException(String reason) {
      super(reason);
    }

    private ServingDeniedException(String reason, Throwable cause) {
      super(reason, cause);
    }
  }
}
