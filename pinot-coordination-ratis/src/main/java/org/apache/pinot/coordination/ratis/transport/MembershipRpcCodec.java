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
package org.apache.pinot.coordination.ratis.transport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.RenewalResult;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Response;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ServingPermit;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;


/// Closed membership wire vocabulary. Participant authority is filled from authenticated policy; internal clock
/// and control-epoch commands cannot be represented by a public request. Catalog payloads remain explicitly typed.
final class MembershipRpcCodec {
  private MembershipRpcCodec() {
  }

  static MetadataProtocol.MembershipCommand command(MetadataRpc.MembershipCommandRequest request,
      ParticipantIdentity identity) {
    MetadataRpcCodec.rejectUnknown(request);
    UUID history = UUID.fromString(request.getHistoryId());
    MembershipProtocol.Operation operation = switch (request.getCommandCase()) {
      case REGISTER -> {
        MetadataRpc.ParticipantRegistration registration = request.getRegister();
        yield new MembershipProtocol.Register(identity.participantId(), identity.role(),
            UUID.fromString(registration.getIncarnation()), registration.getExpectedEpoch(),
            registration.getEndpointsMap(), identity.capabilities());
      }
      case RENEW -> {
        MetadataRpc.ParticipantRenewal renewal = request.getRenew();
        require(renewal.hasSession() && renewal.hasAppliedRevision(), "Renewal session and revision required");
        yield new MembershipProtocol.RenewBatch(List.of(new MembershipProtocol.Renewal(
            participant(history, renewal.getSession(), identity), renewal.getSequence(),
            revision(renewal.getAppliedRevision()), identity.capabilities())));
      }
      case READY -> {
        MetadataRpc.ParticipantReadiness ready = request.getReady();
        require(ready.hasSession() && ready.hasAppliedRevision(), "Readiness session and revision required");
        yield new MembershipProtocol.SetReady(participant(history, ready.getSession(), identity),
            revision(ready.getAppliedRevision()), ready.getReady());
      }
      case DRAIN -> new MembershipProtocol.BeginDrain(participant(history, request.getDrain(), identity));
      case UNREGISTER -> new MembershipProtocol.Unregister(participant(history, request.getUnregister(), identity));
      default -> throw new IllegalArgumentException("A supported participant command is required");
    };
    return new MetadataProtocol.MembershipCommand(history, identity.participantId(), operation);
  }

  static MetadataProtocol.MembershipQuery query(MetadataRpc.MembershipReadRequest request,
      ParticipantIdentity identity) {
    MetadataRpcCodec.rejectUnknown(request);
    return new MetadataProtocol.MembershipQuery(UUID.fromString(request.getHistoryId()),
        new MembershipProtocol.GetParticipant(identity.participantId()));
  }

  private static ParticipantToken participant(UUID history, MetadataRpc.ParticipantSession session,
      ParticipantIdentity identity) {
    require(session.getRegistrationEpoch() > 0, "A current registration epoch is required");
    return new ParticipantToken(history, identity.participantId(), identity.role(),
        UUID.fromString(session.getIncarnation()), session.getRegistrationEpoch());
  }

  static MetadataRpc.MembershipResponse response(Response response) {
    MetadataRpc.MembershipResponse.Builder result = MetadataRpc.MembershipResponse.newBuilder()
        .setStatus(MetadataRpc.ResultStatus.valueOf(response.status().name()))
        .setRevision(revision(response.revision()))
        .setDetail(response.detail());
    if (response.control() != null) {
      result.setControl(control(response.control()));
    }
    if (response.participant() != null) {
      result.setParticipant(participant(response.participant()));
    }
    for (RenewalResult renewal : response.renewals()) {
      MetadataRpc.RenewalResult.Builder encoded = MetadataRpc.RenewalResult.newBuilder()
          .setParticipant(participant(renewal.participant())).setSequence(renewal.sequence())
          .setStatus(MetadataRpc.ResultStatus.valueOf(renewal.status().name())).setDetail(renewal.detail());
      if (renewal.permit() != null) {
        encoded.setPermit(permit(renewal.permit()));
      }
      result.addRenewals(encoded);
    }
    return result.build();
  }

  static Response response(MetadataRpc.MembershipResponse response) {
    MetadataRpcCodec.rejectUnknown(response);
    require(response.hasRevision() && response.getRenewalsCount() <= 1, "Invalid participant response");
    List<RenewalResult> renewals = new ArrayList<>();
    for (MetadataRpc.RenewalResult renewal : response.getRenewalsList()) {
      require(renewal.hasParticipant(), "Renewal participant required");
      renewals.add(new RenewalResult(participant(renewal.getParticipant()), renewal.getSequence(),
          MetadataProtocol.Status.valueOf(renewal.getStatus().name()),
          renewal.hasPermit() ? permit(renewal.getPermit()) : null, renewal.getDetail()));
    }
    return new Response(MetadataProtocol.Status.valueOf(response.getStatus().name()), revision(response.getRevision()),
        response.getDetail(), response.hasControl() ? control(response.getControl()) : null,
        response.hasParticipant() ? participant(response.getParticipant()) : null, renewals);
  }

  static MetadataRpc.MembershipControl control(ControlState control) {
    MetadataRpc.MembershipControl.Builder result = MetadataRpc.MembershipControl.newBuilder().setRound(control.round())
        .setClockBarrierIndex(control.clockBarrierIndex()).setMinimumTickDelayMillis(control.minimumTickDelayMillis())
        .setMaxConfiguredPermitLifetimeMillis(control.maxConfiguredPermitLifetimeMillis());
    if (control.policy() != null) {
      result.setPolicy(policy(control.policy()));
    }
    if (control.controlEpoch() != null) {
      result.setControlEpoch(control(control.controlEpoch()));
    }
    return result.build();
  }

  static ControlState control(MetadataRpc.MembershipControl control) {
    MetadataRpcCodec.rejectUnknown(control);
    return new ControlState(control.hasControlEpoch() ? control(control.getControlEpoch()) : null, control.getRound(),
        control.hasPolicy() ? policy(control.getPolicy()) : null, control.getClockBarrierIndex(),
        control.getMinimumTickDelayMillis(), control.getMaxConfiguredPermitLifetimeMillis());
  }

  static MetadataRpc.ParticipantState participant(ParticipantState participant) {
    MetadataRpc.ParticipantState.Builder result = MetadataRpc.ParticipantState.newBuilder()
        .setParticipant(participant(participant.participant())).putAllEndpoints(participant.endpoints())
        .addAllCapabilities(capabilities(participant.capabilities())).setExpiryRound(participant.expiryRound())
        .setReady(participant.ready()).setDraining(participant.draining()).setRetired(participant.retired())
        .setLastRenewalSequence(participant.lastRenewalSequence())
        .setLastModifiedIndex(participant.lastModifiedIndex());
    if (participant.appliedRevision() != null) {
      result.setAppliedRevision(revision(participant.appliedRevision()));
    }
    return result.build();
  }

  static ParticipantState participant(MetadataRpc.ParticipantState participant) {
    MetadataRpcCodec.rejectUnknown(participant);
    require(participant.hasParticipant(), "Participant token required");
    return new ParticipantState(participant(participant.getParticipant()), participant.getEndpointsMap(),
        capabilities(participant.getCapabilitiesList()), participant.getExpiryRound(), participant.getReady(),
        participant.getDraining(), participant.getRetired(),
        participant.hasAppliedRevision() ? revision(participant.getAppliedRevision()) : null,
        participant.getLastRenewalSequence(), participant.getLastModifiedIndex());
  }

  static MetadataRpc.ParticipantToken participant(ParticipantToken token) {
    return MetadataRpc.ParticipantToken.newBuilder().setHistoryId(token.historyId().toString())
        .setParticipantId(token.participantId()).setRole(MetadataRpc.ParticipantRole.valueOf(token.role().name()))
        .setIncarnation(token.incarnation().toString()).setRegistrationEpoch(token.registrationEpoch()).build();
  }

  static ParticipantToken participant(MetadataRpc.ParticipantToken token) {
    return new ParticipantToken(UUID.fromString(token.getHistoryId()), token.getParticipantId(),
        MembershipProtocol.Role.valueOf(token.getRole().name()), UUID.fromString(token.getIncarnation()),
        token.getRegistrationEpoch());
  }

  private static MetadataRpc.ControlToken control(ControlToken token) {
    return MetadataRpc.ControlToken.newBuilder().setHistoryId(token.historyId().toString())
        .setRaftTerm(token.raftTerm())
        .setBarrierIndex(token.barrierIndex()).setLeaderId(token.leaderId())
        .setActivationId(token.activationId().toString()).build();
  }

  private static ControlToken control(MetadataRpc.ControlToken token) {
    return new ControlToken(UUID.fromString(token.getHistoryId()), token.getRaftTerm(), token.getBarrierIndex(),
        token.getLeaderId(), UUID.fromString(token.getActivationId()));
  }

  private static MetadataRpc.MembershipPolicy policy(Policy policy) {
    return MetadataRpc.MembershipPolicy.newBuilder().setVersion(policy.version())
        .setMinTickIntervalMillis(policy.minTickIntervalMillis()).setLeaseRounds(policy.leaseRounds())
        .setPermitLifetimeMillis(policy.permitLifetimeMillis())
        .setMaxQueryLifetimeMillis(policy.maxQueryLifetimeMillis())
        .setMaxRevisionLag(policy.maxRevisionLag()).build();
  }

  private static Policy policy(MetadataRpc.MembershipPolicy policy) {
    return new Policy(policy.getVersion(), policy.getMinTickIntervalMillis(), policy.getLeaseRounds(),
        policy.getPermitLifetimeMillis(), policy.getMaxQueryLifetimeMillis(), policy.getMaxRevisionLag());
  }

  private static MetadataRpc.ServingPermit permit(ServingPermit permit) {
    return MetadataRpc.ServingPermit.newBuilder().setParticipant(participant(permit.participant()))
        .setRenewalSequence(permit.renewalSequence()).setControlEpoch(control(permit.controlEpoch()))
        .setMinAppliedRevision(revision(permit.minAppliedRevision()))
        .setPermitLifetimeMillis(permit.permitLifetimeMillis())
        .setMaxQueryLifetimeMillis(permit.maxQueryLifetimeMillis()).setPolicyVersion(permit.policyVersion())
        .addAllCapabilities(capabilities(permit.capabilities())).build();
  }

  private static ServingPermit permit(MetadataRpc.ServingPermit permit) {
    require(permit.hasParticipant() && permit.hasControlEpoch() && permit.hasMinAppliedRevision(),
        "Serving permit identity and barriers required");
    return new ServingPermit(participant(permit.getParticipant()), permit.getRenewalSequence(),
        control(permit.getControlEpoch()), revision(permit.getMinAppliedRevision()), permit.getPermitLifetimeMillis(),
        permit.getMaxQueryLifetimeMillis(), permit.getPolicyVersion(), capabilities(permit.getCapabilitiesList()));
  }

  private static MetadataRpc.Revision revision(Revision revision) {
    return MetadataRpc.Revision.newBuilder().setHistoryId(revision.historyId().toString()).setIndex(revision.index())
        .build();
  }

  private static Revision revision(MetadataRpc.Revision revision) {
    require(revision.getIndex() >= 0, "Invalid applied revision");
    return new Revision(UUID.fromString(revision.getHistoryId()), revision.getIndex());
  }

  private static List<MetadataRpc.ServingCapability> capabilities(Set<Capability> capabilities) {
    return capabilities.stream().map(capability -> MetadataRpc.ServingCapability.valueOf(capability.name())).sorted()
        .toList();
  }

  private static Set<Capability> capabilities(List<MetadataRpc.ServingCapability> capabilities) {
    Set<Capability> result = new HashSet<>();
    for (MetadataRpc.ServingCapability capability : capabilities) {
      require(result.add(Capability.valueOf(capability.name())), "Duplicate serving capability");
    }
    return Set.copyOf(result);
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalArgumentException(message);
    }
  }
}
