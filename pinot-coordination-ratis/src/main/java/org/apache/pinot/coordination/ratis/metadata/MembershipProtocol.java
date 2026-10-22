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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;


/// Deterministic membership and read-only serving grants. Identities in this protocol are fences, not credentials;
/// the transport authenticates participant identities and derives their allowed roles and capabilities.
public final class MembershipProtocol {
  public static final String CONTROL_NAME = "control";
  public static final int MAX_RENEWALS = MetadataProtocol.MAX_MUTATIONS;
  public static final int MAX_ENDPOINTS = 16;
  public static final int MAX_IDENTIFIER_LENGTH = 1024;
  public static final int MAX_ENDPOINT_VALUE_LENGTH = 2048;

  private MembershipProtocol() {
  }

  public enum Role {
    CONTROLLER, BROKER, SERVER, MINION
  }

  /// Capabilities select a protocol lane; task publication additionally requires exact task/attempt and output grants.
  public enum Capability {
    BROKER_QUERY, SERVER_QUERY, MINION_TASK
  }

  public record ParticipantToken(UUID historyId, String participantId, Role role, UUID incarnation,
                                 long registrationEpoch) {
  }

  public record ControlToken(UUID historyId, long raftTerm, long barrierIndex, String leaderId, UUID activationId) {
  }

  /// All values are explicit deployment qualifications, not production defaults. Millisecond bounds constrain local
  /// clocks/query admission; logical rounds alone determine replicated membership expiry.
  public record Policy(long version, long minTickIntervalMillis, long leaseRounds, long permitLifetimeMillis,
                       long maxQueryLifetimeMillis, long maxRevisionLag) {
  }

  /// clockBarrierIndex changes on activation, policy updates and ticks. A leader waits the full persisted
  /// minimumTickDelayMillis after observing that barrier; replicated apply cannot verify wall-clock passage.
  /// maxConfiguredPermitLifetimeMillis conservatively bounds permits configured since the latest completed grace.
  public record ControlState(ControlToken controlEpoch, long round, Policy policy, long clockBarrierIndex,
                             long minimumTickDelayMillis, long maxConfiguredPermitLifetimeMillis) {
  }

  public record ParticipantState(ParticipantToken participant, Map<String, String> endpoints,
                                 Set<Capability> capabilities, long expiryRound, boolean ready, boolean draining,
                                 boolean retired, Revision appliedRevision, long lastRenewalSequence,
                                 long lastModifiedIndex) {
    public ParticipantState {
      endpoints = endpoints == null ? null : Map.copyOf(endpoints);
      capabilities = immutableCapabilities(capabilities);
    }
  }

  /// minAppliedRevision is recomputed at the renewal's commit. The local guard must catch up before using the grant
  /// and measures its lifetime from the first send of this logical sequence, including retries and delayed replies.
  public record ServingPermit(ParticipantToken participant, long renewalSequence, ControlToken controlEpoch,
                              Revision minAppliedRevision, long permitLifetimeMillis, long maxQueryLifetimeMillis,
                              long policyVersion, Set<Capability> capabilities) {
    public ServingPermit {
      capabilities = immutableCapabilities(capabilities);
    }
  }

  public record Renewal(ParticipantToken participant, long sequence, Revision appliedRevision,
                        Set<Capability> requestedCapabilities) {
    public Renewal {
      requestedCapabilities = immutableCapabilities(requestedCapabilities);
    }
  }

  public record RenewalResult(ParticipantToken participant, long sequence, Status status, ServingPermit permit,
                              String detail) {
  }

  public record Response(Status status, Revision revision, String detail, ControlState control,
                         ParticipantState participant, List<RenewalResult> renewals) {
    public Response {
      renewals = renewals == null ? null : List.copyOf(renewals);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = ActivateControlEpoch.class, name = "ACTIVATE_CONTROL_EPOCH"),
      @JsonSubTypes.Type(value = ConfigurePolicy.class, name = "CONFIGURE_POLICY"),
      @JsonSubTypes.Type(value = Tick.class, name = "TICK"),
      @JsonSubTypes.Type(value = Register.class, name = "REGISTER"),
      @JsonSubTypes.Type(value = RenewBatch.class, name = "RENEW_BATCH"),
      @JsonSubTypes.Type(value = SetReady.class, name = "SET_READY"),
      @JsonSubTypes.Type(value = BeginDrain.class, name = "BEGIN_DRAIN"),
      @JsonSubTypes.Type(value = Unregister.class, name = "UNREGISTER")})
  public sealed interface Operation permits ActivateControlEpoch, ConfigurePolicy, Tick, Register, RenewBatch,
      SetReady, BeginDrain, Unregister {
  }

  public record ActivateControlEpoch(long term, String leaderId, UUID activationId) implements Operation {
  }

  public record ConfigurePolicy(ControlToken control, long expectedVersion, Policy policy) implements Operation {
  }

  public record Tick(ControlToken control, long expectedRound, long policyVersion) implements Operation {
  }

  public record Register(String participantId, Role role, UUID incarnation, long expectedEpoch,
                         Map<String, String> endpoints, Set<Capability> capabilities) implements Operation {
    public Register {
      endpoints = endpoints == null ? null : Map.copyOf(endpoints);
      capabilities = immutableCapabilities(capabilities);
    }
  }

  public record RenewBatch(List<Renewal> renewals) implements Operation {
    public RenewBatch {
      renewals = renewals == null ? null : List.copyOf(renewals);
    }
  }

  public record SetReady(ParticipantToken participant, Revision appliedRevision, boolean ready) implements Operation {
  }

  public record BeginDrain(ParticipantToken participant) implements Operation {
  }

  public record Unregister(ParticipantToken participant) implements Operation {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = GetControl.class, name = "GET_CONTROL"),
      @JsonSubTypes.Type(value = GetParticipant.class, name = "GET_PARTICIPANT")})
  public sealed interface Read permits GetControl, GetParticipant {
  }

  public record GetControl() implements Read {
  }

  public record GetParticipant(String participantId) implements Read {
  }

  public static Response decodeResponse(byte[] bytes)
      throws IOException {
    return MetadataProtocol.decodeValue(bytes, Response.class);
  }

  public static ControlState decodeControl(JsonNode value)
      throws IOException {
    return MetadataProtocol.fromTree(value, ControlState.class);
  }

  public static ParticipantState decodeParticipant(JsonNode value)
      throws IOException {
    return MetadataProtocol.fromTree(value, ParticipantState.class);
  }

  private static Set<Capability> immutableCapabilities(Set<Capability> capabilities) {
    return capabilities == null ? null : capabilities.isEmpty() ? Set.of()
        : Collections.unmodifiableSet(EnumSet.copyOf(capabilities));
  }
}
