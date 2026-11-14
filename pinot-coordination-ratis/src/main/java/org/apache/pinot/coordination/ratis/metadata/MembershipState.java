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
package org.apache.pinot.coordination.ratis.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ActivateControlEpoch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.BeginDrain;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ConfigurePolicy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.GetControl;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.GetParticipant;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Read;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Register;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.RenewBatch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Renewal;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.RenewalResult;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Response;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ServingPermit;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.SetReady;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Tick;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Unregister;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;


/// Apply-only membership model. Staging touches only the bounded affected participants; ticking is constant time.
/// No local clock, randomness, I/O or caller-supplied Raft term can establish replicated authority here.
final class MembershipState {
  private static final int MAX_MEMBERS = 100_000;
  private static final long MAX_MILLIS = Long.MAX_VALUE / 1_000_000;
  private final UUID _historyId;
  private ControlState _control = new ControlState(null, 0, null, 0, 0, 0);
  private final Map<String, Member> _members = new HashMap<>();

  MembershipState(UUID historyId) {
    _historyId = historyId;
  }

  ControlState control() {
    return _control;
  }

  Plan stage(Operation request, long actualTerm, long index) {
    try {
      check(request != null && actualTerm > 0, Status.INVALID_REQUEST,
          "Membership apply requires an operation and the actual committed Raft term");
      if (request instanceof ActivateControlEpoch activate) {
        return activate(activate, actualTerm, index);
      }
      check(_control.controlEpoch() != null && _control.controlEpoch().raftTerm() == actualTerm,
          Status.NOT_READY, "The current Raft term has no committed control epoch");
      if (request instanceof ConfigurePolicy configure) {
        return configure(configure, index);
      }
      check(_control.policy() != null, Status.NOT_READY, "Membership policy is not configured");
      return switch (request) {
        case Tick tick -> tick(tick, index);
        case Register register -> register(register, index);
        case RenewBatch renew -> renew(renew, index);
        case SetReady ready -> ready(ready, index);
        case BeginDrain drain -> drain(drain, index);
        case Unregister unregister -> unregister(unregister, index);
        default -> throw new Rejected(Status.INVALID_REQUEST, "Unsupported membership operation");
      };
    } catch (Rejected e) {
      return new Plan(_control, Map.of(), response(e._status, e.getMessage(), index, null, List.of()));
    }
  }

  void commit(Plan plan) {
    _control = plan.control();
    _members.putAll(plan.members());
  }

  Response read(Read request, long index) {
    try {
      check(request != null, Status.INVALID_REQUEST, "Membership read is required");
      return switch (request) {
        case GetControl ignored -> response(Status.OK, "", index, null, List.of());
        case GetParticipant participant -> {
          identifier(participant.participantId());
          Member member = _members.get(participant.participantId());
          check(member != null, Status.NOT_FOUND, "Participant is absent");
          yield response(Status.OK, "", index, member.state(), List.of());
        }
      };
    } catch (Rejected e) {
      return response(e._status, e.getMessage(), index, null, List.of());
    }
  }

  Response error(Status status, String detail, long index) {
    return response(status, detail, index, null, List.of());
  }

  private Plan activate(ActivateControlEpoch request, long actualTerm, long index) {
    identifier(request.leaderId());
    check(request.activationId() != null && request.term() == actualTerm, Status.FENCED,
        "Control activation must match the actual committed Raft term");
    ControlToken previous = _control.controlEpoch();
    if (previous != null && previous.raftTerm() == actualTerm) {
      check(previous.leaderId().equals(request.leaderId()) && previous.activationId().equals(request.activationId()),
          Status.CONFLICT, "A different control epoch is already committed for this term");
      return unchanged(index, null);
    }
    check(previous == null || actualTerm > previous.raftTerm(), Status.FENCED, "Control term cannot go backwards");
    ControlState next = new ControlState(new ControlToken(_historyId, actualTerm, index, request.leaderId(),
        request.activationId()), _control.round(), _control.policy(), index, _control.minimumTickDelayMillis(),
        _control.maxConfiguredPermitLifetimeMillis());
    return new Plan(next, Map.of(), new Response(Status.OK, revision(index), "", next, null, List.of()));
  }

  private Plan configure(ConfigurePolicy request, long index) {
    currentControl(request.control());
    validatePolicy(request.policy());
    long version = _control.policy() == null ? 0 : _control.policy().version();
    if (request.policy().equals(_control.policy()) && request.expectedVersion() == version - 1) {
      return unchanged(index, null);
    }
    check(request.expectedVersion() == version && version < Long.MAX_VALUE
        && request.policy().version() == version + 1, Status.CONFLICT, "Membership policy version changed");
    check(request.policy().leaseRounds() <= Long.MAX_VALUE - _control.round(), Status.LIMIT_EXCEEDED,
        "Membership lease round exhausted");
    // Renewals under any intervening policy may still have unexpired grants, including replies that were lost.
    // The next tick waits out their conservative lifetime bound from this committed configuration barrier.
    long previousLifetime = _control.maxConfiguredPermitLifetimeMillis();
    ControlState next = new ControlState(_control.controlEpoch(), _control.round(), request.policy(), index,
        Math.max(request.policy().minTickIntervalMillis(), previousLifetime),
        Math.max(request.policy().permitLifetimeMillis(), previousLifetime));
    return new Plan(next, Map.of(), new Response(Status.OK, revision(index), "", next, null, List.of()));
  }

  private Plan tick(Tick request, long index) {
    currentControl(request.control());
    check(request.policyVersion() == _control.policy().version() && request.expectedRound() == _control.round(),
        Status.CONFLICT, "Tick round or policy changed");
    check(_control.round() < Long.MAX_VALUE, Status.LIMIT_EXCEEDED, "Membership rounds exhausted");
    // The qualified driver has waited through the last configuration barrier. Only current-policy grants can
    // still be live, so the conservative historical bound no longer needs to delay later ticks.
    ControlState next = new ControlState(_control.controlEpoch(), _control.round() + 1, _control.policy(), index,
        _control.policy().minTickIntervalMillis(), _control.policy().permitLifetimeMillis());
    return new Plan(next, Map.of(), new Response(Status.OK, revision(index), "", next, null, List.of()));
  }

  private Plan register(Register request, long index) {
    validateRegistration(request);
    Member previous = _members.get(request.participantId());
    if (previous != null && previous.state().participant().incarnation().equals(request.incarnation())) {
      check(previous.registration().equals(request), Status.CONFLICT, "Registration retry differs");
      currentParticipant(previous.state().participant());
      return unchanged(index, previous.state());
    }
    long epoch = previous == null ? 0 : previous.state().participant().registrationEpoch();
    check(request.expectedEpoch() == epoch, Status.CONFLICT, "Participant registration epoch changed");
    check(previous == null || previous.state().participant().role() == request.role(), Status.FENCED,
        "A durable participant ID cannot change its role");
    check(previous == null || previous.state().retired() || previous.state().expiryRound() <= _control.round(),
        Status.CONFLICT, "A live participant cannot be replaced");
    check(previous != null || _members.size() < MAX_MEMBERS, Status.LIMIT_EXCEEDED, "Participant bound reached");
    check(epoch < Long.MAX_VALUE, Status.LIMIT_EXCEEDED, "Participant registration epochs exhausted");
    ParticipantToken token = new ParticipantToken(_historyId, request.participantId(), request.role(),
        request.incarnation(), epoch + 1);
    ParticipantState state = new ParticipantState(token, request.endpoints(), request.capabilities(), leaseEnd(),
        false, false, false, revision(0), 0, index);
    Member member = new Member(state, request, null, null);
    return new Plan(_control, Map.of(request.participantId(), member),
        response(Status.OK, "", index, state, List.of()));
  }

  private Plan renew(RenewBatch request, long index) {
    check(request.renewals() != null && !request.renewals().isEmpty()
        && request.renewals().size() <= MembershipProtocol.MAX_RENEWALS, Status.INVALID_REQUEST,
        "Renewal batch is outside the supported bounds");
    Set<String> ids = new HashSet<>();
    for (Renewal renewal : request.renewals()) {
      check(renewal != null && renewal.participant() != null
          && ids.add(renewal.participant().participantId()), Status.INVALID_REQUEST,
          "A renewal batch may include each participant only once");
    }
    Map<String, Member> staged = new HashMap<>();
    List<RenewalResult> results = new ArrayList<>();
    for (Renewal renewal : request.renewals()) {
      try {
        Member member = currentParticipant(renewal.participant());
        ParticipantState state = member.state();
        check(renewal.sequence() > 0, Status.INVALID_REQUEST, "Renewal sequence must be positive");
        if (renewal.sequence() == state.lastRenewalSequence()) {
          check(renewal.equals(member.renewal()), Status.CONFLICT,
              "Renewal sequence was reused for a different request");
          results.add(member.outcome());
          continue;
        }
        check(state.lastRenewalSequence() < Long.MAX_VALUE
            && renewal.sequence() == state.lastRenewalSequence() + 1, Status.OUT_OF_ORDER,
            "Renewal sequences must be contiguous and never reset across control terms");
        RenewalResult outcome;
        ParticipantState next;
        try {
          check(renewal.requestedCapabilities() != null
              && state.capabilities().containsAll(renewal.requestedCapabilities()), Status.FENCED,
              "Renewal capabilities exceed the registered role capability bound");
          validApplied(renewal.appliedRevision(), state.appliedRevision(), index);
          ServingPermit permit = state.ready() && !state.draining()
              ? new ServingPermit(state.participant(), renewal.sequence(), _control.controlEpoch(), revision(index),
                  _control.policy().permitLifetimeMillis(), _control.policy().maxQueryLifetimeMillis(),
                  _control.policy().version(), renewal.requestedCapabilities()) : null;
          outcome = new RenewalResult(renewal.participant(), renewal.sequence(), Status.OK, permit, "");
          next = copy(state, Math.max(state.expiryRound(), leaseEnd()), state.ready(), state.draining(), false,
              renewal.appliedRevision(),
              renewal.sequence(), index);
        } catch (Rejected e) {
          // A valid current token's next sequence has one durable outcome, including semantic rejection. It cannot
          // become a grant on retry after metadata or policy changes. Invalid tokens never consume its sequence.
          outcome = new RenewalResult(renewal.participant(), renewal.sequence(), e._status, null, e.getMessage());
          next = copy(state, state.expiryRound(), state.ready(), state.draining(), state.retired(),
              state.appliedRevision(), renewal.sequence(), index);
        }
        staged.put(state.participant().participantId(), new Member(next, member.registration(), renewal, outcome));
        results.add(outcome);
      } catch (Rejected e) {
        results.add(new RenewalResult(renewal.participant(), renewal.sequence(), e._status, null, e.getMessage()));
      }
    }
    return new Plan(_control, staged, response(Status.OK, "", index, null, results));
  }

  private Plan ready(SetReady request, long index) {
    Member member = currentParticipant(request.participant());
    ParticipantState state = member.state();
    validApplied(request.appliedRevision(), state.appliedRevision(), index);
    if (state.ready() == request.ready() && state.appliedRevision().equals(request.appliedRevision())) {
      return unchanged(index, state);
    }
    check(!request.ready() || (!state.draining() && request.appliedRevision().index() >= state.lastModifiedIndex()),
        Status.NOT_READY, "Readiness requires current participant metadata and no drain fence");
    ParticipantState next = copy(state, state.expiryRound(), request.ready(), state.draining(), false,
        request.appliedRevision(), state.lastRenewalSequence(), index);
    return memberChange(member, next, index);
  }

  private Plan drain(BeginDrain request, long index) {
    Member member = currentParticipant(request.participant());
    ParticipantState state = member.state();
    if (state.draining()) {
      return unchanged(index, state);
    }
    return memberChange(member, copy(state, state.expiryRound(), false, true, false, state.appliedRevision(),
        state.lastRenewalSequence(), index), index);
  }

  private Plan unregister(Unregister request, long index) {
    Member member = matchingParticipant(request.participant());
    ParticipantState state = member.state();
    if (state.retired()) {
      return unchanged(index, state);
    }
    return memberChange(member, copy(state, state.expiryRound(), false, true, true, state.appliedRevision(),
        state.lastRenewalSequence(), index), index);
  }

  private Plan memberChange(Member previous, ParticipantState next, long index) {
    return new Plan(_control, Map.of(next.participant().participantId(), new Member(next, previous.registration(),
        previous.renewal(), previous.outcome())), response(Status.OK, "", index, next, List.of()));
  }

  private Plan unchanged(long index, ParticipantState participant) {
    return new Plan(_control, Map.of(), response(Status.OK, "", index, participant, List.of()));
  }

  private void currentControl(ControlToken token) {
    check(_control.controlEpoch().equals(token), Status.FENCED, "Control epoch differs");
  }

  private Member matchingParticipant(ParticipantToken token) {
    validateToken(token, _historyId);
    Member member = _members.get(token.participantId());
    check(member != null && member.state().participant().equals(token), Status.FENCED,
        "Participant history, role, incarnation or registration epoch differs");
    return member;
  }

  private Member currentParticipant(ParticipantToken token) {
    Member member = matchingParticipant(token);
    check(!member.state().retired() && member.state().expiryRound() > _control.round(), Status.FENCED,
        "Participant lease expired or registration retired");
    return member;
  }

  private long leaseEnd() {
    check(_control.policy().leaseRounds() <= Long.MAX_VALUE - _control.round(), Status.LIMIT_EXCEEDED,
        "Membership lease rounds exhausted");
    return _control.round() + _control.policy().leaseRounds();
  }

  private void validApplied(Revision applied, Revision previous, long index) {
    check(applied != null && _historyId.equals(applied.historyId()), Status.HISTORY_MISMATCH,
        "Applied metadata history differs");
    check(applied.index() >= previous.index() && applied.index() < index, Status.INVALID_REQUEST,
        "Applied revision must be monotonic and precede this command");
    check(index - applied.index() <= _control.policy().maxRevisionLag(), Status.NOT_READY,
        "Participant metadata image is too far behind");
  }

  Snapshot snapshot() {
    return new Snapshot(_control, Map.copyOf(_members));
  }

  static MembershipState restore(UUID historyId, Snapshot snapshot, long index) {
    check(snapshot != null && snapshot.members() != null && snapshot.members().size() <= MAX_MEMBERS,
        Status.INVALID_REQUEST, "Invalid membership snapshot");
    validateControl(snapshot.control(), historyId, index);
    MembershipState state = new MembershipState(historyId);
    state._control = snapshot.control();
    for (Map.Entry<String, Member> entry : snapshot.members().entrySet()) {
      Member member = entry.getValue();
      check(member != null, Status.INVALID_REQUEST, "Null membership entry");
      validateParticipant(member.state(), historyId, index);
      validateRegistration(member.registration());
      ParticipantState participant = member.state();
      ParticipantToken token = participant.participant();
      check(entry.getKey().equals(token.participantId()) && member.registration().participantId().equals(entry.getKey())
          && member.registration().role() == token.role()
          && member.registration().incarnation().equals(token.incarnation())
          && member.registration().expectedEpoch() == token.registrationEpoch() - 1
          && member.registration().capabilities().equals(participant.capabilities())
          && member.registration().endpoints().equals(participant.endpoints())
          && snapshot.control().policy() != null, Status.INVALID_REQUEST, "Membership registration differs from state");
      if (participant.lastRenewalSequence() == 0) {
        check(member.renewal() == null && member.outcome() == null, Status.INVALID_REQUEST,
            "Unrenewed participant contains an outcome");
      } else {
        Renewal renewal = member.renewal();
        RenewalResult outcome = member.outcome();
        check(renewal != null && outcome != null && token.equals(renewal.participant())
            && token.equals(outcome.participant()) && renewal.sequence() == participant.lastRenewalSequence()
            && outcome.sequence() == renewal.sequence() && outcome.status() != null && outcome.detail() != null,
            Status.INVALID_REQUEST, "Invalid retained renewal outcome");
        check(outcome.status() == Status.OK || outcome.permit() == null, Status.INVALID_REQUEST,
            "Rejected renewal contains a serving permit");
        if (outcome.status() == Status.OK) {
          check(renewal.appliedRevision() != null && historyId.equals(renewal.appliedRevision().historyId())
              && renewal.appliedRevision().index() >= 0
              && renewal.appliedRevision().index() <= participant.appliedRevision().index()
              && renewal.requestedCapabilities() != null
              && participant.capabilities().containsAll(renewal.requestedCapabilities()), Status.INVALID_REQUEST,
              "Successful renewal contains invalid applied revision or capabilities");
        }
        ServingPermit permit = outcome.permit();
        if (permit != null) {
          check(outcome.status() == Status.OK && token.equals(permit.participant())
              && permit.renewalSequence() == renewal.sequence() && permit.controlEpoch() != null
              && permit.minAppliedRevision() != null && historyId.equals(permit.minAppliedRevision().historyId())
              && permit.minAppliedRevision().index() > renewal.appliedRevision().index()
              && permit.minAppliedRevision().index() <= participant.lastModifiedIndex()
              && permit.capabilities() != null && permit.capabilities().equals(renewal.requestedCapabilities())
              && participant.capabilities().containsAll(permit.capabilities())
              && permit.policyVersion() > 0 && permit.policyVersion() <= snapshot.control().policy().version()
              && validMillis(permit.permitLifetimeMillis()) && validMillis(permit.maxQueryLifetimeMillis()),
              Status.INVALID_REQUEST, "Invalid retained serving permit");
          validateControlToken(permit.controlEpoch(), historyId, index);
          ControlToken currentControl = snapshot.control().controlEpoch();
          check(permit.controlEpoch().raftTerm() <= currentControl.raftTerm()
              && permit.controlEpoch().barrierIndex() <= permit.minAppliedRevision().index()
              && (permit.controlEpoch().raftTerm() == currentControl.raftTerm()
                  ? permit.controlEpoch().equals(currentControl)
                  : permit.minAppliedRevision().index() < currentControl.barrierIndex()), Status.INVALID_REQUEST,
              "Retained permit control epoch is in the future or conflicts with the current term");
          Policy policy = snapshot.control().policy();
          check(permit.policyVersion() != policy.version()
              || (permit.permitLifetimeMillis() == policy.permitLifetimeMillis()
                  && permit.maxQueryLifetimeMillis() == policy.maxQueryLifetimeMillis()), Status.INVALID_REQUEST,
              "Retained permit conflicts with its policy version");
        }
      }
      state._members.put(entry.getKey(), member);
    }
    return state;
  }

  static void validateControl(ControlState control, UUID historyId, long index) {
    check(control != null && control.round() >= 0 && control.clockBarrierIndex() >= 0
        && control.clockBarrierIndex() <= index, Status.INVALID_REQUEST, "Invalid membership control state");
    if (control.controlEpoch() == null) {
      check(control.round() == 0 && control.clockBarrierIndex() == 0 && control.policy() == null,
          Status.INVALID_REQUEST, "Inactive control contains committed authority");
    } else {
      validateControlToken(control.controlEpoch(), historyId, index);
      check(control.clockBarrierIndex() >= control.controlEpoch().barrierIndex(), Status.INVALID_REQUEST,
          "Control clock precedes its activation barrier");
    }
    if (control.policy() != null) {
      validatePolicy(control.policy());
      check(validMillis(control.minimumTickDelayMillis()) && validMillis(control.maxConfiguredPermitLifetimeMillis())
          && control.minimumTickDelayMillis() >= control.policy().minTickIntervalMillis()
          && control.maxConfiguredPermitLifetimeMillis() >= control.policy().permitLifetimeMillis()
          && (control.maxConfiguredPermitLifetimeMillis() == control.policy().permitLifetimeMillis()
              || control.minimumTickDelayMillis() >= control.maxConfiguredPermitLifetimeMillis()),
          Status.INVALID_REQUEST, "Membership timing barrier does not protect configured serving lifetimes");
    } else {
      check(control.round() == 0 && control.minimumTickDelayMillis() == 0
          && control.maxConfiguredPermitLifetimeMillis() == 0, Status.INVALID_REQUEST,
          "Logical rounds and timing barriers require a configured policy");
    }
  }

  private static void validateControlToken(ControlToken token, UUID historyId, long index) {
    check(token != null && historyId.equals(token.historyId()) && token.raftTerm() > 0 && token.barrierIndex() > 0
        && token.barrierIndex() <= index && token.activationId() != null, Status.INVALID_REQUEST,
        "Invalid membership control token");
    identifier(token.leaderId());
  }

  static void validateParticipant(ParticipantState participant, UUID historyId, long index) {
    check(participant != null, Status.INVALID_REQUEST, "Missing participant state");
    validateToken(participant.participant(), historyId);
    validateEndpoints(participant.endpoints());
    validateCapabilities(participant.participant().role(), participant.capabilities());
    check(participant.expiryRound() > 0 && participant.lastRenewalSequence() >= 0
        && participant.lastModifiedIndex() > 0 && participant.lastModifiedIndex() <= index
        && participant.appliedRevision() != null && historyId.equals(participant.appliedRevision().historyId())
        && participant.appliedRevision().index() >= 0
        && participant.appliedRevision().index() < participant.lastModifiedIndex()
        && (!participant.ready() || (!participant.draining() && !participant.retired()))
        && (!participant.retired() || participant.draining()), Status.INVALID_REQUEST, "Invalid participant state");
  }

  static void validatePolicy(Policy policy) {
    check(policy != null && policy.version() > 0 && validMillis(policy.minTickIntervalMillis())
        && policy.leaseRounds() >= 2 && validMillis(policy.permitLifetimeMillis())
        && validMillis(policy.maxQueryLifetimeMillis()) && policy.maxRevisionLag() > 0,
        Status.INVALID_REQUEST, "Membership policy requires explicit positive bounded values");
    // A renewal can commit immediately before a tick, leaving only L-1 full intervals before expiry. Division
    // avoids overflow in (L-1)*T, and equality is safe because delivery rejects at the permit deadline.
    check((policy.permitLifetimeMillis() - 1) / policy.minTickIntervalMillis() < policy.leaseRounds() - 1,
        Status.INVALID_REQUEST, "Serving permit outlives the earliest possible membership expiry");
  }

  private static boolean validMillis(long value) {
    return value > 0 && value <= MAX_MILLIS;
  }

  private static void validateRegistration(Register request) {
    check(request != null && request.incarnation() != null && request.expectedEpoch() >= 0,
        Status.INVALID_REQUEST, "Invalid participant registration");
    identifier(request.participantId());
    validateEndpoints(request.endpoints());
    validateCapabilities(request.role(), request.capabilities());
  }

  private static void validateToken(ParticipantToken token, UUID historyId) {
    check(token != null && historyId.equals(token.historyId()), Status.HISTORY_MISMATCH,
        "Participant history differs");
    identifier(token.participantId());
    check(token.role() != null && token.incarnation() != null && token.registrationEpoch() > 0,
        Status.INVALID_REQUEST, "Incomplete participant token");
  }

  private static void validateEndpoints(Map<String, String> endpoints) {
    check(endpoints != null && endpoints.size() <= MembershipProtocol.MAX_ENDPOINTS, Status.INVALID_REQUEST,
        "Participant endpoints exceed the supported bound");
    endpoints.forEach((name, value) -> {
      identifier(name);
      check(value != null && !value.isBlank() && value.length() <= 2048
          && value.chars().noneMatch(Character::isISOControl), Status.INVALID_REQUEST, "Invalid participant endpoint");
    });
  }

  private static void validateCapabilities(Role role, Set<Capability> capabilities) {
    check(role != null && capabilities != null, Status.INVALID_REQUEST, "Participant role/capabilities are required");
    for (Capability capability : capabilities) {
      check((role == Role.BROKER && capability == Capability.BROKER_QUERY)
          || (role == Role.SERVER && capability == Capability.SERVER_QUERY), Status.INVALID_REQUEST,
          "Capability is incompatible with participant role");
    }
  }

  private static void identifier(String value) {
    check(value != null && !value.isBlank() && value.length() <= 1024
        && value.chars().noneMatch(Character::isISOControl), Status.INVALID_REQUEST, "Invalid membership identifier");
  }

  private static ParticipantState copy(ParticipantState previous, long expiry, boolean ready, boolean draining,
      boolean retired, Revision applied, long sequence, long index) {
    return new ParticipantState(previous.participant(), previous.endpoints(), previous.capabilities(), expiry, ready,
        draining, retired, applied, sequence, index);
  }

  private Response response(Status status, String detail, long index, ParticipantState participant,
      List<RenewalResult> renewals) {
    return new Response(status, revision(index), detail, _control, participant, renewals);
  }

  private Revision revision(long index) {
    return new Revision(_historyId, index);
  }

  private static void check(boolean condition, Status status, String message) {
    if (!condition) {
      throw new Rejected(status, message);
    }
  }

  record Plan(ControlState control, Map<String, Member> members, Response response) {
  }

  record Member(ParticipantState state, Register registration, Renewal renewal, RenewalResult outcome) {
  }

  record Snapshot(ControlState control, Map<String, Member> members) {
  }

  static final class Rejected extends RuntimeException {
    private final Status _status;

    private Rejected(Status status, String message) {
      super(message);
      _status = status;
    }
  }
}
