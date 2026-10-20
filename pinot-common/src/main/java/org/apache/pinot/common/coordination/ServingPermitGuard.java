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

import com.google.protobuf.Message;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ControlToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.Revision;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ServingPermit;


/// Local admission and result-delivery fencing for read-only serving permits. All state transitions are synchronized;
/// the supplied clock and coherent membership-view supplier must be local, nonblocking and perform no RPCs. Normal
/// role integration must check a ticket again after every resume and immediately before delivering a result.
///
/// The guard binds one participant registration for its lifetime. Retries never move a renewal's first-send time.
/// New grants replace previous capabilities immediately, including while waiting for their minimum image revision.
/// Tickets retain their original finite deadline across renewals and cannot be transferred to another guard/process.
/// Cross-host requests additionally bind their immutable [NativeQueryDeadline] using [QueryDeadlineGuard]. No permit
/// authorizes metadata or segment mutations.
public final class ServingPermitGuard {
  private static final long NANOS_PER_MILLI = 1_000_000;

  private final ParticipantToken _participant;
  private final Set<ReadOnlyCapability> _allowedCapabilities;
  private final MonotonicClock _clock;
  private final QualifiedLifetimePolicy _policy;
  private final Supplier<MembershipView> _view;
  private final Object _ticketOwner = new Object();
  private final Map<ReadOnlyCapability, Long> _capabilityEpochs = new EnumMap<>(ReadOnlyCapability.class);
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

  /// A single process-local qualified clock observation.
  public static final class ClockReading {
    private final long _nanos;
    private final long _uncertaintyNanos;
    private final boolean _trusted;

    public ClockReading(long nanos, long uncertaintyNanos, boolean trusted) {
      _nanos = nanos;
      _uncertaintyNanos = uncertaintyNanos;
      _trusted = trusted;
    }

    public long nanos() {
      return _nanos;
    }

    public long uncertaintyNanos() {
      return _uncertaintyNanos;
    }

    public boolean trusted() {
      return _trusted;
    }
  }

  /// Deployment-qualified limits, with no defaults. `safeLifetimeFractionPpm` conservatively converts authority
  /// lifetimes into this clock's units, including the qualified relative clock-rate bound. `safetyMarginMillis`
  /// subtracts an additional deployment-approved margin. Callers must establish those bounds for the platform;
  /// merely constructing this value does not qualify a clock or make arbitrary values safe.
  public static final class QualifiedLifetimePolicy {
    private final long _maxPermitLifetimeMillis;
    private final long _maxQueryLifetimeMillis;
    private final int _safeLifetimeFractionPpm;
    private final long _safetyMarginMillis;
    private final long _maxClockUncertaintyNanos;

    public QualifiedLifetimePolicy(long maxPermitLifetimeMillis, long maxQueryLifetimeMillis,
        int safeLifetimeFractionPpm, long safetyMarginMillis, long maxClockUncertaintyNanos) {
      if (maxPermitLifetimeMillis <= 0 || maxQueryLifetimeMillis <= 0 || safeLifetimeFractionPpm <= 0
          || safeLifetimeFractionPpm > NANOS_PER_MILLI || safetyMarginMillis < 0
          || maxClockUncertaintyNanos < 0 || maxPermitLifetimeMillis > Long.MAX_VALUE / NANOS_PER_MILLI
          || maxQueryLifetimeMillis > Long.MAX_VALUE / NANOS_PER_MILLI
          || safetyMarginMillis >= Math.min(maxPermitLifetimeMillis, maxQueryLifetimeMillis)
          || Math.min(maxPermitLifetimeMillis, maxQueryLifetimeMillis) * safeLifetimeFractionPpm
          <= safetyMarginMillis * NANOS_PER_MILLI) {
        throw new IllegalArgumentException("Invalid qualified serving lifetime policy");
      }
      _maxPermitLifetimeMillis = maxPermitLifetimeMillis;
      _maxQueryLifetimeMillis = maxQueryLifetimeMillis;
      _safeLifetimeFractionPpm = safeLifetimeFractionPpm;
      _safetyMarginMillis = safetyMarginMillis;
      _maxClockUncertaintyNanos = maxClockUncertaintyNanos;
    }

    public long maxPermitLifetimeMillis() {
      return _maxPermitLifetimeMillis;
    }

    public long maxQueryLifetimeMillis() {
      return _maxQueryLifetimeMillis;
    }

    public int safeLifetimeFractionPpm() {
      return _safeLifetimeFractionPpm;
    }

    public long safetyMarginMillis() {
      return _safetyMarginMillis;
    }

    public long maxClockUncertaintyNanos() {
      return _maxClockUncertaintyNanos;
    }
  }

  /// Closed read-only capabilities. Wire unknown and unspecified enum values never grant authority.
  public enum ReadOnlyCapability {
    BROKER_QUERY, SERVER_QUERY;

    public static ReadOnlyCapability fromWire(MetadataRpc.ServingCapability capability) {
      if (capability == MetadataRpc.ServingCapability.BROKER_QUERY) {
        return BROKER_QUERY;
      }
      if (capability == MetadataRpc.ServingCapability.SERVER_QUERY) {
        return SERVER_QUERY;
      }
      throw new IllegalArgumentException("Unsupported serving capability");
    }

    public MetadataRpc.ServingCapability toWire() {
      return MetadataRpc.ServingCapability.valueOf(name());
    }
  }

  /// One coherent local image projection. `live` must include expiry-round comparison, even before cleanup marks a
  /// participant expired. A failed/uninitialized cache must make the supplier fail, never return a stale success.
  /// Decode once when publishing an image update, not on the query path.
  public static final class MembershipView {
    private final Revision _revision;
    @Nullable
    private final ControlToken _control;
    @Nullable
    private final ParticipantToken _participant;
    private final boolean _live;
    private final boolean _ready;
    private final boolean _draining;
    private final Set<ReadOnlyCapability> _capabilities;
    private final long _policyVersion;

    public MembershipView(Revision revision, @Nullable ControlToken control, @Nullable ParticipantToken participant,
        boolean live, boolean ready, boolean draining, Set<ReadOnlyCapability> capabilities, long policyVersion) {
      _revision = normalize(revision);
      _control = control == null ? null : normalize(control);
      _participant = participant == null ? null : normalize(participant);
      _live = live;
      _ready = ready;
      _draining = draining;
      _capabilities = Set.copyOf(capabilities);
      if (policyVersion < 0) {
        throw new IllegalArgumentException("Invalid membership policy version");
      }
      _policyVersion = policyVersion;
    }

    public Revision revision() {
      return _revision;
    }

    @Nullable
    public ControlToken control() {
      return _control;
    }

    @Nullable
    public ParticipantToken participant() {
      return _participant;
    }

    public boolean live() {
      return _live;
    }

    public boolean ready() {
      return _ready;
    }

    public boolean draining() {
      return _draining;
    }

    public Set<ReadOnlyCapability> capabilities() {
      return _capabilities;
    }

    public long policyVersion() {
      return _policyVersion;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof MembershipView)) {
        return false;
      }
      MembershipView that = (MembershipView) other;
      return _revision.equals(that._revision) && Objects.equals(_control, that._control)
          && Objects.equals(_participant, that._participant) && _live == that._live && _ready == that._ready
          && _draining == that._draining && _capabilities.equals(that._capabilities)
          && _policyVersion == that._policyVersion;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_revision, _control, _participant, _live, _ready, _draining, _capabilities, _policyVersion);
    }
  }

  /// Opaque local ticket. It is neither serializable nor reconstructible by a caller, and renewal cannot extend it.
  public static final class QueryTicket {
    private final Object _owner;
    private final ReadOnlyCapability _capability;
    private final long _deadline;
    private final long _controlGeneration;
    private final long _capabilityEpoch;
    private final long _lifetimeGeneration;
    private final long _policyVersion;
    @Nullable
    private final NativeQueryDeadline _absoluteDeadline;
    @Nullable
    private final QueryDeadlineGuard _deadlineGuard;
    private boolean _deadlineRejected;

    private QueryTicket(Object owner, ReadOnlyCapability capability, long deadline, long controlGeneration,
        long capabilityEpoch, long lifetimeGeneration, long policyVersion,
        @Nullable NativeQueryDeadline absoluteDeadline, @Nullable QueryDeadlineGuard deadlineGuard) {
      _owner = owner;
      _capability = capability;
      _deadline = deadline;
      _controlGeneration = controlGeneration;
      _capabilityEpoch = capabilityEpoch;
      _lifetimeGeneration = lifetimeGeneration;
      _policyVersion = policyVersion;
      _absoluteDeadline = absoluteDeadline;
      _deadlineGuard = deadlineGuard;
    }
  }

  /// Opaque qualified lower time bound captured before transmitting a coherent membership snapshot request.
  public static final class LeaseObservation {
    private final Object _owner;
    private final long _firstSendLowerBound;

    private LeaseObservation(Object owner, long firstSendLowerBound) {
      _owner = owner;
      _firstSendLowerBound = firstSendLowerBound;
    }
  }

  /// An immutable peer-lease deadline derived from one snapshot observation. Refresh or renewal cannot extend an
  /// admitted attempt's captured bound, and a rejected bound cannot revive if clock uncertainty later narrows.
  public static final class PeerLeaseBound {
    private final Object _owner;
    private final long _deadline;
    private boolean _rejected;

    private PeerLeaseBound(Object owner, long deadline) {
      _owner = owner;
      _deadline = deadline;
    }
  }

  public ServingPermitGuard(ParticipantToken participant, Set<ReadOnlyCapability> allowedCapabilities,
      MonotonicClock clock, QualifiedLifetimePolicy policy, Supplier<MembershipView> view) {
    _participant = normalize(participant);
    _allowedCapabilities = Set.copyOf(allowedCapabilities);
    _clock = Objects.requireNonNull(clock, "clock");
    _policy = Objects.requireNonNull(policy, "policy");
    _view = Objects.requireNonNull(view, "view");
    for (ReadOnlyCapability capability : ReadOnlyCapability.values()) {
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

  /// Call immediately before sending the snapshot RPC, never on receipt. Network transit consumes the lease budget.
  public synchronized LeaseObservation beginLeaseObservation() {
    ClockReading now = readClock();
    return new LeaseObservation(_ticketOwner, subtract(now.nanos(), now.uncertaintyNanos()));
  }

  /// Bounds a peer's remaining lease using the snapshot's current policy, including possible future policy changes.
  /// The next tick may be immediate, so only `expiryRound - round - 1` full intervals are guaranteed. A later policy
  /// may accelerate subsequent ticks, but its persisted transition barrier first waits out the current policy's
  /// permit lifetime. Cap by that lifetime, not by the transient minimum-tick delay or historical maximum lifetime.
  /// This relies on the same qualified membership clock driver as serving permits.
  public synchronized PeerLeaseBound boundPeerLease(LeaseObservation observation, long round, long expiryRound,
      long minTickIntervalMillis, long permitLifetimeMillis) {
    require(observation != null && observation._owner == _ticketOwner,
        "Lease observation belongs to another guard or process");
    readClock();
    require(round >= 0 && expiryRound > round && minTickIntervalMillis > 0 && permitLifetimeMillis > 0,
        "Invalid peer membership lease");
    long fullRounds = expiryRound - round - 1;
    long cap = Math.min(permitLifetimeMillis, _policy.maxPermitLifetimeMillis());
    // Compare before multiplying: historical leases can retain arbitrarily large remaining round counts.
    long millis = fullRounds > cap / minTickIntervalMillis ? cap : fullRounds * minTickIntervalMillis;
    long nanos = Math.max(0, convertedLifetime(millis, _policy.safeLifetimeFractionPpm()));
    return new PeerLeaseBound(_ticketOwner, add(observation._firstSendLowerBound, nanos));
  }

  /// Check both before admission and every result callback, even when membership snapshots stop arriving.
  public synchronized void checkPeerLease(PeerLeaseBound bound) {
    require(bound != null && bound._owner == _ticketOwner, "Peer lease belongs to another guard or process");
    require(!bound._rejected, "Peer membership lease was already rejected");
    ClockReading now = readClock();
    if (add(now.nanos(), now.uncertaintyNanos()) >= bound._deadline) {
      bound._rejected = true;
      deny("Peer membership lease expired");
    }
  }

  /// Stages an authenticated grant. Admission independently requires its minimum coherent image revision. Exact
  /// duplicate and older grants cannot refresh a deadline; a conflicting response for one sequence fails closed.
  public synchronized void accept(ServingPermit grant) {
    readClock();
    try {
      grant = normalize(grant);
    } catch (IllegalArgumentException | NullPointerException e) {
      invalidate("Malformed serving grant");
      throw new ServingDeniedException(_invalidReason, e);
    }
    require(_participant.equals(grant.getParticipant()), "Serving grant belongs to another participant registration");
    if (_lastAccepted != null && grant.getRenewalSequence() <= _lastAccepted.getRenewalSequence()) {
      if (grant.getRenewalSequence() == _lastAccepted.getRenewalSequence() && !grant.equals(_lastAccepted)) {
        invalidate("Conflicting grant for an already accepted renewal");
        deny(_invalidReason);
      }
      return;
    }
    require(grant.getRenewalSequence() > 0 && grant.getRenewalSequence() == _lastSentSequence,
        "Serving grant does not match the latest locally sent renewal");
    require(grant.hasControlEpoch() && grant.hasMinAppliedRevision()
        && _participant.getHistoryId().equals(grant.getControlEpoch().getHistoryId())
        && _participant.getHistoryId().equals(grant.getMinAppliedRevision().getHistoryId())
        && grant.getMinAppliedRevision().getIndex() >= grant.getControlEpoch().getBarrierIndex()
        && grant.getPolicyVersion() > 0
        && grant.getPermitLifetimeMillis() > 0 && grant.getMaxQueryLifetimeMillis() > 0
        && _allowedCapabilities.containsAll(capabilities(grant)),
        "Invalid serving grant or capabilities outside the configured read-only role");
    if (!observeControl(grant.getControlEpoch())) {
      require(grant.getControlEpoch().getRaftTerm() < _control.getRaftTerm()
          && grant.getControlEpoch().getBarrierIndex() < _control.getBarrierIndex(),
          "Serving grant conflicts with the observed control epoch");
      // A durable renewal can replay its original grant after a newer control epoch was observed locally.
      // Consume that authenticated outcome without restoring authority; remember it to reject conflicting retries.
      _lastAccepted = grant;
      _permit = null;
      return;
    }
    if (_permit != null) {
      for (ReadOnlyCapability capability : capabilities(_permit)) {
        if (!capabilities(grant).contains(capability)) {
          _capabilityEpochs.put(capability, add(_capabilityEpochs.get(capability), 1));
        }
      }
    }
    if (_lastAccepted != null && (grant.getPermitLifetimeMillis() < _lastAccepted.getPermitLifetimeMillis()
        || grant.getMaxQueryLifetimeMillis() < _lastAccepted.getMaxQueryLifetimeMillis())) {
      _lifetimeGeneration = add(_lifetimeGeneration, 1);
    }
    _lastAccepted = grant;
    // A newer grant revokes prior authority even if its lifetime is too short for this deployment's safety margin.
    _permit = null;
    long lifetime = safeLifetime(Math.min(grant.getPermitLifetimeMillis(), _policy.maxPermitLifetimeMillis()));
    long deadline = add(_firstSendLowerBound, lifetime);
    _permit = grant;
    _permitDeadline = deadline;
  }

  public synchronized QueryTicket admit(ReadOnlyCapability capability) {
    ClockReading now = readClock();
    ServingPermit permit = checkServing(capability, now, true);
    // A process pause can occur while the local view is read. Sample again after that validation before deciding.
    now = readClock();
    long lifetime = safeLifetime(Math.min(permit.getMaxQueryLifetimeMillis(), _policy.maxQueryLifetimeMillis()));
    long deadline = Math.min(_permitDeadline, add(subtract(now.nanos(), now.uncertaintyNanos()), lifetime));
    require(add(now.nanos(), now.uncertaintyNanos()) < deadline, "Insufficient qualified time for query admission");
    return new QueryTicket(_ticketOwner, capability, deadline, _controlGeneration, _capabilityEpochs.get(capability),
        _lifetimeGeneration, permit.getPolicyVersion(), null, null);
  }

  /// Combines a shared immutable deadline with a fixed local ticket. Sampling the local lower bound before the
  /// shared clock prevents a pause between the observations from starting a fresh budget. The qualified minimum
  /// relative rate must hold continuously in the named domain, including its leap/smear and clock-step behavior.
  public synchronized QueryTicket admit(ReadOnlyCapability capability, NativeQueryDeadline absoluteDeadline,
      QueryDeadlineGuard deadlineGuard) {
    Objects.requireNonNull(absoluteDeadline, "absoluteDeadline");
    Objects.requireNonNull(deadlineGuard, "deadlineGuard");
    ClockReading now = readClock();
    ServingPermit permit = checkServing(capability, now, true);
    ClockReading anchor = readClock();
    long remaining;
    try {
      remaining = deadlineGuard.remainingMillis(absoluteDeadline);
    } catch (RuntimeException e) {
      throw new ServingDeniedException("Native query deadline cannot authorize admission", e);
    }
    long millis = Math.min(remaining,
        Math.min(permit.getMaxQueryLifetimeMillis(), _policy.maxQueryLifetimeMillis()));
    int rate = Math.min(_policy.safeLifetimeFractionPpm(), deadlineGuard.localMinimumRatePpm());
    long deadline = Math.min(_permitDeadline,
        add(subtract(anchor.nanos(), anchor.uncertaintyNanos()), safeLifetime(millis, rate)));
    now = readClock();
    require(add(now.nanos(), now.uncertaintyNanos()) < deadline, "Insufficient qualified time for query admission");
    return new QueryTicket(_ticketOwner, capability, deadline, _controlGeneration, _capabilityEpochs.get(capability),
        _lifetimeGeneration, permit.getPolicyVersion(), absoluteDeadline, deadlineGuard);
  }

  /// Check after resuming work and immediately before result delivery. Draining preserves already admitted work
  /// only within its original deadline and while the current image, control epoch and capability remain valid.
  public synchronized void checkDelivery(QueryTicket ticket) {
    require(ticket != null && ticket._owner == _ticketOwner, "Query ticket belongs to another guard or process");
    require(!ticket._deadlineRejected, "Query ticket deadline was already rejected");
    if (ticket._deadlineGuard != null) {
      try {
        ticket._deadlineGuard.check(ticket._absoluteDeadline);
      } catch (RuntimeException e) {
        ticket._deadlineRejected = true;
        throw new ServingDeniedException("Native query deadline cannot authorize delivery", e);
      }
    }
    ClockReading now = readClock();
    checkTicketTime(ticket, now);
    checkServing(ticket._capability, now, false);
    require(ticket._controlGeneration == _controlGeneration
        && ticket._capabilityEpoch == _capabilityEpochs.get(ticket._capability)
        && ticket._lifetimeGeneration == _lifetimeGeneration && ticket._policyVersion == _permit.getPolicyVersion(),
        "Query ticket was fenced or revoked");
    now = readClock();
    checkTicketTime(ticket, now);
  }

  private void checkTicketTime(QueryTicket ticket, ClockReading now) {
    if (add(now.nanos(), now.uncertaintyNanos()) >= Math.min(ticket._deadline, _permitDeadline)) {
      ticket._deadlineRejected = true;
      deny("Query delivery deadline expired");
    }
  }

  /// Stops new admissions; existing tickets remain subject to every delivery check and their finite deadlines.
  public synchronized void drain() {
    _draining = true;
  }

  /// Terminal revocation. Recovery after lost clock qualification or participant fencing requires a new guard.
  public synchronized void invalidate() {
    invalidate("Serving guard explicitly invalidated");
  }

  private ServingPermit checkServing(ReadOnlyCapability capability, ClockReading now, boolean admission) {
    require(_permit != null, "No serving permit is active");
    ServingPermit permit = _permit;
    require(capability != null && _allowedCapabilities.contains(capability)
        && permit.getCapabilitiesList().contains(capability.toWire()),
        "Serving capability is not currently granted");
    MembershipView view;
    try {
      view = _view.get();
    } catch (RuntimeException e) {
      throw new ServingDeniedException("Coherent membership image is unavailable", e);
    }
    require(view != null && view.revision() != null, "Coherent membership image is unavailable");
    if (!_participant.getHistoryId().equals(view.revision().getHistoryId())) {
      invalidate("Metadata history changed");
      deny(_invalidReason);
    }
    require(view.revision().getIndex() >= 0 && (_lastView == null
        || view.revision().getIndex() >= _lastView.revision().getIndex()), "Coherent membership image regressed");
    if (_lastView != null && view.revision().equals(_lastView.revision()) && !view.equals(_lastView)) {
      invalidate("Conflicting membership projection for one image revision");
      deny(_invalidReason);
    }
    _lastView = view;
    if (view.control() != null) {
      require(observeControl(view.control()), "Membership image has an older control epoch");
    }
    require(_permit == permit && permit.getControlEpoch().equals(view.control()), "Serving control epoch changed");
    require(view.revision().getIndex() >= permit.getMinAppliedRevision().getIndex(),
        "Membership image has not caught up");
    if (!_participant.equals(view.participant())) {
      invalidate("Participant incarnation or registration epoch changed");
      deny(_invalidReason);
    }
    require(view.live() && (view.ready() || !admission && view.draining()),
        "Participant lease expired or participant is not ready");
    require(view.capabilities().contains(capability) && view.policyVersion() == permit.getPolicyVersion(),
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
    int term = Long.compare(control.getRaftTerm(), _control.getRaftTerm());
    int barrier = Long.compare(control.getBarrierIndex(), _control.getBarrierIndex());
    if (term < 0 || term == 0 && barrier < 0) {
      return false;
    }
    if (barrier <= 0 || !_participant.getHistoryId().equals(control.getHistoryId())) {
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
    return safeLifetime(millis, _policy.safeLifetimeFractionPpm());
  }

  private long safeLifetime(long millis, int fractionPpm) {
    long nanos = convertedLifetime(millis, fractionPpm);
    require(nanos > 0, "No positive lifetime remains under the qualified safety policy");
    return nanos;
  }

  private long convertedLifetime(long millis, int fractionPpm) {
    return subtract(Math.multiplyExact(millis, fractionPpm),
        Math.multiplyExact(_policy.safetyMarginMillis(), NANOS_PER_MILLI));
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

  private static ServingPermit normalize(ServingPermit grant) {
    known(grant);
    argument(grant.hasParticipant() && grant.hasControlEpoch() && grant.hasMinAppliedRevision(),
        "Incomplete serving permit");
    argument(grant.getRenewalSequence() > 0 && grant.getPolicyVersion() > 0
        && grant.getPermitLifetimeMillis() > 0 && grant.getMaxQueryLifetimeMillis() > 0,
        "Invalid serving permit limits or sequence");
    ServingPermit.Builder normalized = grant.toBuilder().setParticipant(normalize(grant.getParticipant()))
        .setControlEpoch(normalize(grant.getControlEpoch()))
        .setMinAppliedRevision(normalize(grant.getMinAppliedRevision())).clearCapabilities();
    for (ReadOnlyCapability capability : capabilities(grant)) {
      normalized.addCapabilities(capability.toWire());
    }
    return normalized.build();
  }

  private static Set<ReadOnlyCapability> capabilities(ServingPermit grant) {
    EnumSet<ReadOnlyCapability> capabilities = EnumSet.noneOf(ReadOnlyCapability.class);
    argument(grant.getCapabilitiesCount() <= ReadOnlyCapability.values().length, "Too many serving capabilities");
    for (MetadataRpc.ServingCapability capability : grant.getCapabilitiesList()) {
      argument(capabilities.add(ReadOnlyCapability.fromWire(capability)), "Duplicate serving capability");
    }
    return capabilities;
  }

  private static ParticipantToken normalize(ParticipantToken participant) {
    known(participant);
    argument(participant.getRole() != MetadataRpc.ParticipantRole.PARTICIPANT_ROLE_UNSPECIFIED
        && participant.getRole() != MetadataRpc.ParticipantRole.UNRECOGNIZED
        && !participant.getParticipantId().isBlank() && participant.getParticipantId().length() <= 1024
        && participant.getRegistrationEpoch() > 0, "A committed participant registration is required");
    return participant.toBuilder().setHistoryId(uuid(participant.getHistoryId()))
        .setIncarnation(uuid(participant.getIncarnation())).build();
  }

  private static ControlToken normalize(ControlToken control) {
    known(control);
    argument(control.getRaftTerm() > 0 && control.getBarrierIndex() > 0
        && !control.getLeaderId().isBlank() && control.getLeaderId().length() <= 1024,
        "Invalid membership control epoch");
    return control.toBuilder().setHistoryId(uuid(control.getHistoryId()))
        .setActivationId(uuid(control.getActivationId())).build();
  }

  private static Revision normalize(Revision revision) {
    known(revision);
    argument(revision.getIndex() >= 0, "Invalid metadata revision");
    return revision.toBuilder().setHistoryId(uuid(revision.getHistoryId())).build();
  }

  private static String uuid(String value) {
    UUID parsed = UUID.fromString(value);
    argument(parsed.toString().equalsIgnoreCase(value), "Malformed UUID");
    return parsed.toString();
  }

  private static void known(Message message) {
    Objects.requireNonNull(message, "message");
    argument(message.getUnknownFields().asMap().isEmpty(), "Unknown serving protocol fields");
  }

  private static void argument(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
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
